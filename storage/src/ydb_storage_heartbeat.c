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
#include <ev.h>

#include "include/spx_socket.h"
#include "include/spx_alloc.h"
#include "include/spx_string.h"
#include "include/spx_message.h"
#include "include/spx_path.h"
#include "include/spx_nio_context.h"
#include "include/spx_nio.h"
#include "include/spx_io.h"
#include "include/spx_list.h"
#include "include/spx_defs.h"

#include "ydb_protocol.h"
#include "ydb_storage_configurtion.h"
#include "ydb_storage_startpoint.h"

spx_private struct spx_nio_context *heartbeat_nio_context = NULL;
spx_private ev_timer *heartbeat_timer = NULL;

spx_private err_t ydb_storage_heartbeat_send(int protocol,\
        struct spx_host *tracker,string_t groupname,string_t machineid,\
        string_t ip,int port,\
        u64_t first_start,u64_t disksize,u64_t freesize,int status,\
        struct spx_nio_context *nio_context){/*{{{*/
    err_t err = 0;
    int fd = spx_socket_new(&err);
    if(0 >= fd){
        SpxLog2(nio_context->log,SpxLogError,err,"create heartbeat socket fd is fail.");
        return err;
    }
    if(0 != (err = spx_set_nb(fd))){
        SpxLog2(nio_context->log,SpxLogError,err,"set socket nonblacking is fail.");
        goto r1;
    }
    if(0 != (err = spx_socket_set(fd,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,30))){
        SpxLog2(nio_context->log,SpxLogError,err,"set socket operator is fail.");
        goto r1;
    }
    if(0 != (err = spx_socket_connect(fd,tracker->ip,tracker->port))){
        SpxLogFmt2(nio_context->log,SpxLogError,err,\
                "conntect to tracker:%s:%d is fail.",\
                tracker->ip,tracker->port);
        goto r1;
    }

    struct spx_msg_header *writer_header = NULL;
    writer_header = spx_alloc_alone(sizeof(*writer_header),&err);
    if(NULL == writer_header){
        SpxLog2(nio_context->log,SpxLogError,err,\
                "alloc writer header is fail.");
        goto r1;
    }
    nio_context->writer_header = writer_header;
    writer_header->version = YDB_VERSION;
    writer_header->protocol = protocol;
    writer_header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN + SpxIpv4Size + sizeof(i32_t)
        + sizeof(u64_t) + sizeof(u64_t) + sizeof(i64_t) + sizeof(int);
    struct spx_msg *ctx = spx_msg_new(writer_header->bodylen,&err);
    if(NULL == ctx){
        SpxLog2(nio_context->log,SpxLogError,err,\
                "alloc writer body is fail.");
        goto r1;
    }
    nio_context->writer_body_ctx = ctx;
    spx_msg_pack_fixed_string(ctx,groupname,YDB_GROUPNAME_LEN);
    spx_msg_pack_fixed_string(ctx,machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_fixed_string(ctx,ip,SpxIpv4Size);
    spx_msg_pack_i32(ctx,port);
    spx_msg_pack_u64(ctx,first_start);
    spx_msg_pack_u64(ctx,disksize);
    spx_msg_pack_u64(ctx,freesize);
    spx_msg_pack_i32(ctx,status);

    nio_context->fd = fd;
    spx_nio_regedit_writer(heartbeat_nio_context);
    return err;
r1:
    SpxClose(fd);
    return err;
}/*}}}*/

spx_private void ydb_storage_heartbeat_to_tracker_reader(int fd,struct spx_nio_context *nio_context){/*{{{*/
    spx_nio_reader_body_handler(fd,nio_context);
    if(0 != nio_context->err){
        SpxLog2(nio_context->log,SpxLogError,nio_context->err,\
                "recv the regedit response is fail.");
        goto r1;
    }

    struct spx_msg *ctx = nio_context->reader_body_ctx;
    if(NULL == ctx){
        SpxLog2(nio_context->log,SpxLogError,nio_context->err,\
                "no recved the body ctx.");
        goto r1;
    }

    spx_msg_seek(ctx,YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN + SpxIpv4Size \
            + sizeof(i32_t),SpxMsgSeekSet);
    u64_t first_start = spx_msg_unpack_u64(ctx);
    if(first_start < ydb_storage_first_start){
        ydb_storage_first_start = first_start;
        ydb_storage_startpoint_reset(nio_context->log,nio_context->config);
    }

r1:
    spx_nio_context_clear(nio_context);
}/*}}}*/

spx_private void ydb_storage_heartbeat_handler(struct ev_loop *loop,\
        ev_timer *w,int revents){
    struct spx_nio_context *nio_context = (struct spx_nio_context *) w->data;
    struct ydb_storage_configurtion *c = (struct ydb_storage_configurtion *) \
                                         nio_context->config;

    u64_t disksize = 0;
    u64_t freesize = 0;
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_COUNT; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)){
            mp->freesize = spx_mountpoint_freesize(mp->path,&(nio_context->err));
            mp->disksize = spx_mountpoint_size(mp->path,&(nio_context->err));
            mp->freesize = 0 >= mp->freesize - c->freedisk \
                           ? 0 : mp->freesize - c->freedisk;
            disksize += mp->disksize;
            freesize += mp->freesize;
        }
    }

    struct spx_vector_iter *iter = spx_vector_iter_init(c->trackers,&(nio_context->err));
    if(NULL == iter){
        SpxLog2(nio_context->log,SpxLogError,nio_context->err,\
                "init the trackers iter is fail.");
        goto r1;
    }

    struct spx_host *host = NULL;
    while(NULL != (host = spx_vector_iter_next(iter))){
        ydb_storage_heartbeat_send( YDB_HEARTBEAT_STORAGE,
                host,c->groupname,c->machineid,c->ip,c->port,
                ydb_storage_first_start,disksize,freesize,ydb_storage_status,
                heartbeat_nio_context);
    }
r1:
    ev_timer_set (heartbeat_timer, (double) 30, 0.);
    ev_io_start(heartbeat_nio_context->loop,&(heartbeat_nio_context->watcher));
}

err_t ydb_storage_heartbeat_init(
        SpxLogDelegate *log,
        u32_t timeout,
        SpxNioDelegate *nio_reader,
        SpxNioDelegate *nio_writer,
        SpxNioHeaderValidatorDelegate *reader_header_validator,
        SpxNioHeaderValidatorFailDelegate *reader_header_validator_fail,
        SpxNioBodyProcessDelegate *reader_body_process,
        SpxNioBodyProcessDelegate *writer_body_process,
        struct spx_properties *config){/*{{{*/
    err_t err = 0;
    struct spx_nio_context_arg arg;
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
    heartbeat_nio_context = (struct spx_nio_context *) spx_nio_context_new(&arg,&err);
    if(NULL == heartbeat_nio_context){
        SpxLog2(log,SpxLogError,err,"alloc heartbeat nio context is fail.");
    }
    heartbeat_timer = spx_alloc_alone(sizeof(*heartbeat_timer),&err);
    if(NULL == heartbeat_timer){
        spx_nio_context_free((void **) &heartbeat_nio_context);
        SpxLog2(log,SpxLogError,err,"alloc heartbeat timer is fail.");
    }
    return err;
}/*}}}*/

void ydb_storage_heartbeat_service(int fd,struct spx_nio_context *nio_context){

    switch(nio_context->reader_header->protocol) {
        case YDB_REGEDIT_STORAGE :{
                                      ydb_storage_heartbeat_to_tracker_reader(fd,nio_context);
                                      break;
                                  }
        case YDB_HEARTBEAT_STORAGE:{
                                       ydb_storage_heartbeat_to_tracker_reader(fd,nio_context);
                                       break;
                                   }
        case YDB_SHUTDOWN_STORAGE:{
                                      ydb_storage_heartbeat_to_tracker_reader(fd,nio_context);
                                      break;
                                  }
        default:{
                    spx_nio_context_clear(nio_context);
                    break;
                }
    }
    return;
}

void ydb_storage_regedit(struct spx_host *tracker,string_t groupname,string_t machineid,\
        string_t ip,int port,\
        u64_t first_start,u64_t disksize,u64_t freesize){
    ydb_storage_status = YDB_STORAGE_INITING;
    ydb_storage_heartbeat_send( YDB_SHUTDOWN_STORAGE,
            tracker,groupname,machineid,ip,port,
            first_start,disksize,freesize,YDB_STORAGE_CLOSING,
            heartbeat_nio_context);
}

void ydb_storage_report(){
    ev_timer_init(heartbeat_timer,ydb_storage_heartbeat_handler,(double) 30,(double) 0);
    heartbeat_timer->data = heartbeat_nio_context;
    ev_io_start(heartbeat_nio_context->loop,&(heartbeat_nio_context->watcher));
    ev_run(heartbeat_nio_context->loop,0);
}

void ydb_storage_shutdown(struct spx_host *tracker,string_t groupname,string_t machineid,\
        string_t ip,int port,\
        u64_t first_start,u64_t disksize,u64_t freesize){
    ydb_storage_status = YDB_STORAGE_CLOSING;
    ev_timer_stop(heartbeat_nio_context->loop,heartbeat_timer);
    ydb_storage_heartbeat_send( YDB_SHUTDOWN_STORAGE,
            tracker,groupname,machineid,ip,port,
            first_start,disksize,freesize,YDB_STORAGE_CLOSING,
            heartbeat_nio_context);
    ydb_storage_status = YDB_STORAGE_CLOSED;
}

void ydb_storage_heartbeat_nio_writer_body_handler(\
        int fd,struct spx_nio_context *nio_context){/*{{{*/
    spx_nio_writer_body_handler(fd,nio_context);
    spx_nio_regedit_reader(nio_context);
}/*}}}*/
