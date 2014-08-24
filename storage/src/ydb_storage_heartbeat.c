/*
 * =====================================================================================
 *
 *       Filename:  ydb_storage_heartbeat.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/07/01 13时53分16秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <ev.h>

#include "include/spx_socket.h"
#include "include/spx_alloc.h"
#include "include/spx_string.h"
#include "include/spx_message.h"
#include "include/spx_path.h"
#include "include/spx_nio.h"
#include "include/spx_io.h"
#include "include/spx_list.h"
#include "include/spx_defs.h"
#include "include/spx_job.h"

#include "ydb_protocol.h"
#include "ydb_storage_configurtion.h"
#include "ydb_storage_startpoint.h"
#include "ydb_storage_state.h"

spx_private struct spx_job_context *hjcontext = NULL;
spx_private ev_timer *heartbeat_timer = NULL;
spx_private struct ev_loop *hloop;

spx_private bool_t ydb_storage_regedit(struct ydb_storage_configurtion *c,\
        struct spx_job_context *jc);
spx_private void *ydb_storage_report();

spx_private err_t ydb_storage_heartbeat_send(int protocol,\
        struct spx_host *tracker,string_t groupname,string_t machineid,\
        string_t ip,int port,\
        u64_t first_start,u64_t disksize,u64_t freesize,int status,\
        struct spx_job_context *jc){/*{{{*/
    err_t err = 0;
    int fd = spx_socket_new(&err);
    if(0 >= fd){
        SpxLog2(jc->log,SpxLogError,err,"create heartbeat socket fd is fail.");
        return err;
    }
    if(0 != (err = spx_set_nb(fd))){
        SpxLog2(jc->log,SpxLogError,err,"set socket nonblacking is fail.");
        goto r1;
    }
    if(0 != (err = spx_socket_set(fd,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,30))){
        SpxLog2(jc->log,SpxLogError,err,"set socket operator is fail.");
        goto r1;
    }
    if(0 != (err = spx_socket_connect(fd,tracker->ip,tracker->port))){
        SpxLogFmt2(jc->log,SpxLogError,err,\
                "conntect to tracker:%s:%d is fail.",\
                tracker->ip,tracker->port);
        goto r1;
    }

    struct spx_msg_header *writer_header = NULL;
    writer_header = spx_alloc_alone(sizeof(*writer_header),&err);
    if(NULL == writer_header){
        SpxLog2(jc->log,SpxLogError,err,\
                "alloc writer header is fail.");
        goto r1;
    }
    jc->writer_header = writer_header;
    writer_header->version = YDB_VERSION;
    writer_header->protocol = protocol;
    writer_header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN + SpxIpv4Size + sizeof(i32_t)
        + sizeof(u64_t) + sizeof(u64_t) + sizeof(i64_t) + sizeof(int);
    struct spx_msg *ctx = spx_msg_new(writer_header->bodylen,&err);
    if(NULL == ctx){
        SpxLog2(jc->log,SpxLogError,err,\
                "alloc writer body is fail.");
        goto r1;
    }
    jc->writer_body_ctx = ctx;
    spx_msg_pack_fixed_string(ctx,groupname,YDB_GROUPNAME_LEN);
    spx_msg_pack_fixed_string(ctx,machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_fixed_string(ctx,ip,SpxIpv4Size);
    spx_msg_pack_i32(ctx,port);
    spx_msg_pack_u64(ctx,first_start);
    spx_msg_pack_u64(ctx,disksize);
    spx_msg_pack_u64(ctx,freesize);
    spx_msg_pack_i32(ctx,status);

    jc->fd = fd;
    spx_nio_regedit_writer(hloop,jc->fd,hjcontext);
    return err;
r1:
    SpxClose(fd);
    return err;
}/*}}}*/

spx_private void ydb_storage_heartbeat_to_tracker_reader(int fd,struct spx_job_context *jc){/*{{{*/
    spx_nio_reader_body_handler(fd,jc,jc->reader_header->bodylen);
    if(0 != jc->err){
        SpxLog2(jc->log,SpxLogError,jc->err,\
                "recv the regedit response is fail.");
        goto r1;
    }

    struct spx_msg *ctx = jc->reader_body_ctx;
    if(NULL == ctx){
        SpxLog2(jc->log,SpxLogError,jc->err,\
                "no recved the body ctx.");
        goto r1;
    }

    spx_msg_seek(ctx,YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN + SpxIpv4Size \
            + sizeof(i32_t),SpxMsgSeekSet);
    u64_t first_start = spx_msg_unpack_u64(ctx);
    if(first_start < g_ydb_storage_state->ydb_storage_first_start){
        g_ydb_storage_state->ydb_storage_first_start = first_start;
        ydb_storage_startpoint_reset(jc->log,jc->config);
    }

r1:
    spx_job_context_clear(jc);
}/*}}}*/

spx_private void ydb_storage_heartbeat_handler(struct ev_loop *loop,\
        ev_timer *w,int revents){
    struct spx_job_context *jc = (struct spx_job_context *) w->data;
    struct ydb_storage_configurtion *c = (struct ydb_storage_configurtion *) \
                                         jc->config;

    u64_t disksize = 0;
    u64_t freesize = 0;
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_COUNT; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)){
            mp->freesize = spx_mountpoint_freesize(mp->path,&(jc->err));
            mp->disksize = spx_mountpoint_size(mp->path,&(jc->err));
            mp->freesize = 0 >= mp->freesize - c->freedisk \
                           ? 0 : mp->freesize - c->freedisk;
            disksize += mp->disksize;
            freesize += mp->freesize;
        }
    }

    g_ydb_storage_state->freesize = freesize;
    g_ydb_storage_state->disksize = disksize;

    struct spx_vector_iter *iter = spx_vector_iter_init(c->trackers,&(jc->err));
    if(NULL == iter){
        SpxLog2(jc->log,SpxLogError,jc->err,\
                "init the trackers iter is fail.");
        goto r1;
    }

    struct spx_host *host = NULL;
    while(NULL != (host = spx_vector_iter_next(iter))){
        ydb_storage_heartbeat_send( YDB_HEARTBEAT_STORAGE,
                host,c->groupname,c->machineid,c->ip,c->port,
                g_ydb_storage_state->ydb_storage_first_start,\
                disksize,freesize,g_ydb_storage_state->ydb_storage_status,
                hjcontext);
    }
r1:
    ev_timer_set (heartbeat_timer, (double) 30, 0.);
    ev_io_start(hloop,&(hjcontext->watcher));
}


spx_private bool_t ydb_storage_regedit(struct ydb_storage_configurtion *c,\
        struct spx_job_context *jc){

    err_t err = 0;
    u64_t disksize = 0;
    u64_t freesize = 0;
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_COUNT; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)){
            mp->freesize = spx_mountpoint_freesize(mp->path,&(jc->err));
            mp->disksize = spx_mountpoint_size(mp->path,&(jc->err));
            mp->freesize = 0 >= mp->freesize - c->freedisk \
                           ? 0 : mp->freesize - c->freedisk;
            disksize += mp->disksize;
            freesize += mp->freesize;
        }
    }

    g_ydb_storage_state->freesize = freesize;
    g_ydb_storage_state->disksize = disksize;

    struct spx_vector_iter *iter = spx_vector_iter_init(c->trackers ,&(jc->err));
    if(NULL == iter){
        SpxLog2(jc->log,SpxLogError,jc->err,\
                "init the trackers iter is fail.");
        return false;
    }

    bool_t can_run = false;
    struct spx_host *host = NULL;
    while(NULL != (host = spx_vector_iter_next(iter))){
        err = ydb_storage_heartbeat_send( YDB_REGEDIT_STORAGE,\
                host,c->groupname,c->machineid,c->ip,c->port,
                g_ydb_storage_state->ydb_storage_first_start,\
                disksize,freesize,g_ydb_storage_state->ydb_storage_status,
                jc);
        if(0 != err){
            jc->err = err;
            SpxLogFmt2(jc->log,SpxLogError,err,\
                    "regedit to tracker %s:%d is fail.",\
                    host->ip,host->port);
        } else {
            can_run = true;
        }
    }
    spx_vector_iter_free(&iter);

    if(!can_run){
        SpxLog1(jc->log,SpxLogError,\
                "regedit to all tracker is fail,"
                "so storage cannot run.");
    }
    return can_run ;
}



spx_private void *ydb_storage_report(){
    ev_timer_init(heartbeat_timer,ydb_storage_heartbeat_handler,(double) 30,(double) 1);
    heartbeat_timer->data = hjcontext;
    ev_io_start(hloop,&(hjcontext->watcher));
    ev_run(hloop,0);
    return NULL;
}




pthread_t ydb_storage_heartbeat_service_init(
        SpxLogDelegate *log,
        u32_t timeout,
        SpxNioDelegate *nio_reader,
        SpxNioDelegate *nio_writer,
        SpxNioHeaderValidatorDelegate *reader_header_validator,
        SpxNioHeaderValidatorFailDelegate *reader_header_validator_fail,
        SpxNioBodyProcessDelegate *reader_body_process,
        SpxNioBodyProcessDelegate *writer_body_process,
        struct ydb_storage_configurtion *config,\
        err_t *err){/*{{{*/
    struct spx_job_context_transport arg;
    SpxZero(arg);
    arg.timeout = timeout;
    arg.nio_reader = nio_reader;
    arg.nio_writer = nio_writer;
    arg.log = log;
    arg.reader_header_validator = reader_header_validator;
    arg.reader_body_process = reader_body_process;
    arg.writer_body_process = writer_body_process;
    arg.reader_header_validator_fail = reader_header_validator_fail;
    arg.config = config;
    hjcontext = (struct spx_job_context *) spx_job_context_new(0,&arg,err);
    if(NULL == hjcontext){
        SpxLog2(log,SpxLogError,*err,"alloc heartbeat nio context is fail.");
        return 0;
    }
    heartbeat_timer = spx_alloc_alone(sizeof(*heartbeat_timer),err);
    if(NULL == heartbeat_timer){
        SpxLog2(log,SpxLogError,*err,"alloc heartbeat timer is fail.");
        goto r1;
    }
    hloop = ev_loop_new(0);
    if(NULL == hloop){
        *err = errno;
        SpxLog2(log,SpxLogError,*err,\
                "new event loop for heartbeat is fail.");
        goto r1;
    }
    if(!ydb_storage_regedit(config,hjcontext)){
        goto r1;
    }

    pthread_t tid = 0;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t ostack_size = 0;
    pthread_attr_getstacksize(&attr, &ostack_size);
    do{
        if (ostack_size != config->stacksize
                && (0 != (*err = pthread_attr_setstacksize(&attr,config->stacksize)))){
            pthread_attr_destroy(&attr);
            SpxLog2(log,SpxLogError,*err,\
                    "set thread stack size is fail.");
            goto r1;
        }
        if (0 !=(*err =  pthread_create(&(tid), &attr, ydb_storage_report,
                        NULL))){
            pthread_attr_destroy(&attr);
            SpxLog2(log,SpxLogError,*err,\
                    "create heartbeat thread is fail.");
            goto r1;
        }
    }while(false);
    pthread_attr_destroy(&attr);
    return tid;
r1:
    if(NULL != heartbeat_timer){
        SpxFree(heartbeat_timer);
    }
    if(NULL != hjcontext) {
        spx_job_context_free((void **) &hjcontext);
    }
    return 0;
}/*}}}*/

void ydb_storage_heartbeat_service(int fd,struct spx_job_context *jc){

    switch(jc->reader_header->protocol) {
        case YDB_REGEDIT_STORAGE :{
                                      ydb_storage_heartbeat_to_tracker_reader(fd,jc);
                                      break;
                                  }
        case YDB_HEARTBEAT_STORAGE:{
                                       ydb_storage_heartbeat_to_tracker_reader(fd,jc);
                                       break;
                                   }
        case YDB_SHUTDOWN_STORAGE:{
                                      ydb_storage_heartbeat_to_tracker_reader(fd,jc);
                                      break;
                                  }
        default:{
                    spx_job_context_clear(jc);
                    break;
                }
    }
    return;
}
void ydb_storage_shutdown(struct ev_loop *loop,struct spx_host *tracker,string_t groupname,string_t machineid,\
        string_t ip,int port,\
        u64_t first_start,u64_t disksize,u64_t freesize){
   g_ydb_storage_state-> ydb_storage_status = YDB_STORAGE_CLOSING;
    ev_timer_stop(hloop,heartbeat_timer);
    ydb_storage_heartbeat_send( YDB_SHUTDOWN_STORAGE,
            tracker,groupname,machineid,ip,port,
            first_start,disksize,freesize,YDB_STORAGE_CLOSING,
            hjcontext);
    g_ydb_storage_state->ydb_storage_status = YDB_STORAGE_CLOSED;
}

void ydb_storage_heartbeat_nio_writer_body_handler(\
        int fd,struct spx_job_context *jc,size_t size){/*{{{*/
    spx_nio_writer_body_handler(fd,jc,size);
    spx_nio_regedit_reader(hloop,jc->fd,jc);
}/*}}}*/
