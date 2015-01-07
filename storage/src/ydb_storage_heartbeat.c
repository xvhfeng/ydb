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

#include "spx_socket.h"
#include "spx_alloc.h"
#include "spx_string.h"
#include "spx_message.h"
#include "spx_path.h"
#include "spx_nio.h"
#include "spx_io.h"
#include "spx_list.h"
#include "spx_defs.h"
#include "spx_job.h"

#include "ydb_protocol.h"
#include "ydb_storage_configurtion.h"
#include "ydb_storage_runtime.h"

spx_private ev_timer *heartbeat_timer = NULL;
spx_private struct ev_loop *hloop = NULL;


spx_private void ydb_storage_heartbeat_nio_body_writer(\
        struct ev_loop *loop,int fd,struct spx_job_context *jc);
spx_private void ydb_storage_heartbeat_nio_body_reader(
        struct ev_loop *loop,int fd,struct spx_job_context *jc);

spx_private bool_t ydb_storage_regedit(struct ev_loop *loop,struct ydb_storage_configurtion *c);
spx_private void *ydb_storage_report(void *arg);
spx_private err_t ydb_storage_heartbeat_send(struct ev_loop *loop,int protocol,\
        struct ydb_tracker *tracker,string_t groupname,string_t machineid,\
        string_t syncgroup,string_t ip,int port,\
        u64_t first_start,u64_t this_startup_time,
        u64_t disksize,u64_t freesize,int status,
        u32_t timeout);

spx_private err_t ydb_storage_heartbeat_send(struct ev_loop *loop,int protocol,\
        struct ydb_tracker *tracker,string_t groupname,string_t machineid,\
        string_t syncgroup,string_t ip,int port,
        u64_t first_start,u64_t this_startup_time,
        u64_t disksize,u64_t freesize,int status,
        u32_t timeout){/*{{{*/
    err_t err = 0;
    struct spx_job_context *jc = tracker->hjc;
    int fd = spx_socket_new(&err);
    if(0 >= fd){
        SpxLogFmt2(jc->log,SpxLogError,err,
                "create heartbeat socket fd is fail."
                "tracker ip:%s port:%d.",
                tracker->host.ip,tracker->host.port);
        return err;
    }
    jc->fd = fd;
    if(0 != (err = spx_set_nb(fd))){
        SpxLogFmt2(jc->log,SpxLogError,err,
                "set socket fd nonblacking is fail."
                "tracker ip:%s port:%d.",
                tracker->host.ip,tracker->host.port);
        goto r1;
    }
    if(0 != (err = spx_socket_set(fd,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,timeout))){
        SpxLogFmt2(jc->log,SpxLogError,err,
                "set socket fd is fail."
                "tracker ip:%s port:%d.",
                tracker->host.ip,tracker->host.port);
        goto r1;
    }
    if(0 != (err = spx_socket_connect_nb(fd,tracker->host.ip,tracker->host.port,timeout))){
        SpxLogFmt2(jc->log,SpxLogError,err,\
                "conntect to tracker:%s:%d is fail.",\
                tracker->host.ip,tracker->host.port);
        goto r1;
    }

    struct spx_msg_header *writer_header = NULL;
    writer_header = spx_alloc_alone(sizeof(*writer_header),&err);
    if(NULL == writer_header){
        SpxLogFmt2(jc->log,SpxLogError,err,\
                "alloc writer header  send to tracker ip:%s port:%d is fail.",
                tracker->host.ip,tracker->host.port);
        goto r1;
    }
    jc->writer_header = writer_header;
    writer_header->version = YDB_VERSION;
    writer_header->protocol = protocol;
    writer_header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN
                             + YDB_SYNCGROUP_LEN + SpxIpv4Size + sizeof(i32_t)
                             + sizeof(u64_t) + sizeof(u64_t) + sizeof(u64_t)
                             + sizeof(i64_t) + sizeof(int);
    struct spx_msg *ctx = spx_msg_new(writer_header->bodylen,&err);
    if(NULL == ctx){
        SpxLogFmt2(jc->log,SpxLogError,err,\
                "alloc writer body send to tracker ip:%s port:%d is fail.",
                tracker->host.ip,tracker->host.port);
        goto r1;
    }
    jc->writer_body_ctx = ctx;
    spx_msg_pack_fixed_string(ctx,groupname,YDB_GROUPNAME_LEN);
    spx_msg_pack_fixed_string(ctx,machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_fixed_string(ctx,syncgroup,YDB_SYNCGROUP_LEN);
    spx_msg_pack_fixed_string(ctx,ip,SpxIpv4Size);
    spx_msg_pack_i32(ctx,port);
    spx_msg_pack_u64(ctx,first_start);
    spx_msg_pack_u64(ctx,this_startup_time);
    spx_msg_pack_u64(ctx,disksize);
    spx_msg_pack_u64(ctx,freesize);
    spx_msg_pack_i32(ctx,status);

    jc->lifecycle = SpxNioLifeCycleHeader;
    spx_nio_writer_faster(loop,fd,jc);
    if(0 != jc->err){
        err = jc->err;
        SpxLogFmt2(jc->log,SpxLogError,jc->err,\
                "call write body handler to client:%s is fail."\
                "and forced clear jc to pool.",\
                jc->client_ip);
        goto r1;
    }
    return err;
r1:
    spx_job_context_clear(jc);
    SpxClose(fd);
    return err;
}/*}}}*/

spx_private void ydb_storage_heartbeat_handler(struct ev_loop *loop,\
        ev_timer *w,int revents){/*{{{*/
    struct ydb_storage_configurtion *c = (struct ydb_storage_configurtion *) \
                                         w->data;
    err_t err = 0;
    u64_t disksize = 0;
    u64_t freesize = 0;
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_COUNT; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)){
            mp->freesize = spx_mountpoint_availsize(mp->path,&(err));
            mp->disksize = spx_mountpoint_totalsize(mp->path,&(err));
            mp->freesize = 0 >= mp->freesize - c->freedisk \
                           ? 0 : mp->freesize - c->freedisk;
            disksize += mp->disksize;
            freesize += mp->freesize;
        }
    }

    g_ydb_storage_runtime->total_freesize = freesize;
    g_ydb_storage_runtime->total_disksize = disksize;

    struct spx_vector_iter *iter = spx_vector_iter_new(c->trackers,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the trackers iter is fail.");
        return;
    }

    struct ydb_tracker *t = NULL;
    while(NULL != (t = spx_vector_iter_next(iter))){
        ydb_storage_heartbeat_send( hloop,YDB_S2T_HEARTBEAT,
                t,c->groupname,c->machineid,c->syncgroup,
                c->ip,c->port,
                g_ydb_storage_runtime->first_statrup_time,
                g_ydb_storage_runtime->this_startup_time,
                disksize,freesize,g_ydb_storage_runtime->status,
                c->timeout);
    }
    spx_vector_iter_free(&iter);

}/*}}}*/

spx_private bool_t ydb_storage_regedit(struct ev_loop *loop,struct ydb_storage_configurtion *c){/*{{{*/
    err_t err = 0;
    u64_t disksize = 0;
    u64_t freesize = 0;
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_COUNT; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)){
            mp->freesize = spx_mountpoint_availsize(mp->path,&err);
            mp->disksize = spx_mountpoint_totalsize(mp->path,&err);
            mp->freesize = 0 >= mp->freesize - c->freedisk \
                           ? 0 : mp->freesize - c->freedisk;
            disksize += mp->disksize;
            freesize += mp->freesize;
        }
    }

    g_ydb_storage_runtime->total_disksize = freesize;
    g_ydb_storage_runtime->total_freesize = disksize;

    struct spx_vector_iter *iter = spx_vector_iter_new(c->trackers ,&err);
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the trackers iter is fail.");
        return false;
    }

    bool_t can_run = false;
    struct ydb_tracker *t = NULL;
    while(NULL != (t = spx_vector_iter_next(iter))){
        err = ydb_storage_heartbeat_send( loop,YDB_S2T_REGEDIT,
                t,c->groupname,c->machineid,c->syncgroup,c->ip,c->port,
                g_ydb_storage_runtime->first_statrup_time,
                g_ydb_storage_runtime->this_startup_time,
                disksize,freesize,g_ydb_storage_runtime->status,
                c->timeout);

        if(0 != err){
            SpxLogFmt2(t->hjc->log,SpxLogError,err,\
                    "regedit to tracker %s:%d is fail.",\
                    t->host.ip,t->host.port);
        } else {
            can_run = true;
        }
    }
    spx_vector_iter_free(&iter);

    if(!can_run){
        SpxLog1(c->log,SpxLogError,\
                "regedit to all tracker is fail,"
                "so storage cannot run.");
    }
    return can_run ;
}/*}}}*/

spx_private void *ydb_storage_report(void *arg){/*{{{*/
    struct ydb_storage_configurtion *c = (struct ydb_storage_configurtion *) arg;
    if(NULL == c){
        return NULL;
    }
    ev_timer_init(heartbeat_timer,
            ydb_storage_heartbeat_handler,
            (double) c->heartbeat ,(double) c->heartbeat);
    ev_timer_start(hloop,heartbeat_timer);
    ev_run(hloop,0);
    return NULL;
}/*}}}*/

void ydb_storage_shutdown(struct ydb_tracker *tracker,\
        string_t groupname,string_t machineid,\
        string_t syncgroup, string_t ip,int port,\
        u64_t first_start,u64_t this_startup_time,
        u64_t disksize,u64_t freesize,
        u32_t timeout){/*{{{*/
    g_ydb_storage_runtime->status = YDB_STORAGE_CLOSING;
    ev_timer_stop(hloop,heartbeat_timer);
    ydb_storage_heartbeat_send(hloop,YDB_S2T_SHUTDOWN,
            tracker,groupname,machineid,syncgroup,ip,port,
            first_start,this_startup_time,
            disksize,freesize,YDB_STORAGE_CLOSING,
            timeout );
    g_ydb_storage_runtime->status = YDB_STORAGE_CLOSED;
}/*}}}*/

spx_private void ydb_storage_heartbeat_nio_body_writer(\
        struct ev_loop *loop,int fd,struct spx_job_context *jc){/*{{{*/
    spx_nio_writer_body_faster_handler(loop,fd,jc);
    if(0 != jc->err){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,\
                "call write body handler to client:%s is fail."\
                "and forced clear jc to pool.",\
                jc->client_ip);
        return;
    }
    spx_nio_regedit_reader(loop,jc->fd,jc);
}/*}}}*/

spx_private void ydb_storage_heartbeat_nio_body_reader(struct ev_loop *loop,
        int fd,struct spx_job_context *jc){/*{{{*/
    spx_nio_reader_body_handler(loop,fd,jc);
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

    spx_msg_seek(ctx,YDB_GROUPNAME_LEN
            + YDB_MACHINEID_LEN + YDB_SYNCGROUP_LEN + SpxIpv4Size \
            + sizeof(i32_t),SpxMsgSeekSet);
    u64_t first_start = spx_msg_unpack_u64(ctx);
    if(first_start < g_ydb_storage_runtime->first_statrup_time){
        g_ydb_storage_runtime->first_statrup_time = first_start;
    }

r1:
    spx_job_context_clear(jc);
}/*}}}*/


pthread_t ydb_storage_heartbeat_service_init(
        SpxLogDelegate *log,
        u32_t timeout,
        struct ydb_storage_configurtion *config,\
        err_t *err){/*{{{*/
    struct spx_job_context_transport arg;
    SpxZero(arg);
    arg.timeout = timeout;
    arg.nio_reader = spx_nio_reader;
    arg.nio_writer = spx_nio_writer;
    arg.log = log;
    arg.reader_header_validator = NULL;
    arg.writer_body_process = ydb_storage_heartbeat_nio_body_writer;
    arg.reader_body_process = ydb_storage_heartbeat_nio_body_reader;
    arg.config = config;
    heartbeat_timer = spx_alloc_alone(sizeof(*heartbeat_timer),err);
    if(NULL == heartbeat_timer){
        SpxLog2(log,SpxLogError,*err,"alloc heartbeat timer is fail.");
        goto r1;
    }
    heartbeat_timer->data = config;
    hloop = ev_loop_new(0);
    if(NULL == hloop){
        *err = errno;
        SpxLog2(log,SpxLogError,*err,\
                "new event loop for heartbeat is fail.");
        goto r1;
    }

    struct spx_vector_iter *iter = spx_vector_iter_new(config->trackers ,err);
    if(NULL == iter){
        SpxLog2(log,SpxLogError,*err,\
                "init the trackers iter is fail.");
        goto r1;
    }

    bool_t isrun = true;
    do{
        struct ydb_tracker *t = NULL;
        while(NULL != (t = spx_vector_iter_next(iter))){
            if(NULL == t->hjc){
                t->hjc = (struct spx_job_context *) spx_job_context_new(0,&arg,err);
                if(NULL == t->hjc){
                    isrun = false;
                    SpxLog2(log,SpxLogError,*err,"alloc heartbeat nio context is fail.");
                    break;
                }
            }
        }
    }while(false);
    spx_vector_iter_free(&iter);
    if(!isrun){
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
                        config))){
            pthread_attr_destroy(&attr);
            SpxLog2(log,SpxLogError,*err,\
                    "create heartbeat thread is fail.");
            goto r1;
        }
    }while(false);
    pthread_attr_destroy(&attr);
    return tid;
    return 0;
r1:
    if(NULL != heartbeat_timer){
        SpxFree(heartbeat_timer);
    }
    return 0;
}/*}}}*/


