//                   _ooOoo_
//                  o8888888o
//                  88" . "88
//                  (| -_- |)
//                  O\  =  /O
//               ____/`---'\____
//             .'  \\|     |//  `.
//            /  \\|||  :  |||//  \
//           /  _||||| -:- |||||-  \
//           |   | \\\  -  /// |   |
//           | \_|  ''\---/''  |   |
//           \  .-\__  `-`  ___/-. /
//         ___`. .'  /--.--\  `. . __
//      ."" '<  `.___\_<|>_/___.'  >'"".
//     | | :  `- \`.;`\ _ /`;.`/ - ` : | |
//     \  \ `-.   \_ __\ /__ _/   .-` /  /
//======`-.____`-.___\_____/___.-`____.-'======
//                   `=---='
//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//         佛祖保佑       永无BUG

/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_syncquery.c
 *        Created:  2014/08/26 13时46分56秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <ev.h>


#include "include/spx_types.h"
#include "include/spx_defs.h"
#include "include/spx_job.h"
#include "include/spx_alloc.h"
#include "include/spx_socket.h"
#include "include/spx_nio.h"
#include "include/spx_io.h"
#include "include/spx_message.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_sync.h"

spx_private ev_timer *sync_timer = NULL;
spx_private struct ev_loop *sloop;

spx_private void ydb_storage_syncquery_nio_body_writer(\
        int fd,struct spx_job_context *jc);

spx_private void ydb_storage_syncquery_nio_body_reader(\
        int fd,struct spx_job_context *jc);

spx_private void ydb_storage_syncquery_handler(struct ev_loop *loop,\
        ev_timer *w,int revents);

spx_private void *ydb_storage_syncquery_heartbeat();

spx_private bool_t ydb_storage_get_remote(struct ydb_storage_configurtion *c,\
        struct spx_job_context_transport *arg);

spx_private err_t ydb_storage_query_remote(SpxLogDelegate *log,
        struct ydb_tracker *t,string_t groupname,
        string_t machineid,string_t syncgroup);


pthread_t ydb_storage_syncquery_init(
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
    arg.reader_body_process = ydb_storage_syncquery_nio_body_reader;
    arg.writer_body_process = ydb_storage_syncquery_nio_body_writer;
    arg.config = config;
    sync_timer = spx_alloc_alone(sizeof(*sync_timer),err);
    if(NULL == sync_timer){
        SpxLog2(log,SpxLogError,*err,"alloc sync timer is fail.");
        goto r1;
    }
    sloop = ev_loop_new(0);
    if(NULL == sloop){
        *err = errno;
        SpxLog2(log,SpxLogError,*err,\
                "new event loop for sync is fail.");
        goto r1;
    }

    if(!ydb_storage_get_remote(config,&arg)){
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
        if (0 !=(*err =  pthread_create(&(tid), &attr,
                        ydb_storage_syncquery_heartbeat,
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
    if(NULL != sync_timer){
        SpxFree(sync_timer);
    }
    return 0;
}/*}}}*/

spx_private bool_t ydb_storage_get_remote(struct ydb_storage_configurtion *c,\
        struct spx_job_context_transport *arg){/*{{{*/
    err_t err = 0;

    struct spx_vector_iter *iter = spx_vector_iter_init(c->trackers ,&err);
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the trackers iter is fail.");
        return false;
    }

    struct ydb_tracker *t = NULL;
    while(NULL != (t = spx_vector_iter_next(iter))){
        if(NULL == t->sjc){
            t->sjc = (struct spx_job_context *) spx_job_context_new(0,arg,&err);
            if(NULL == t->sjc){
                SpxLog2(c->log,SpxLogError,err,"alloc heartbeat nio context is fail.");
                continue;
            }
        }
        err = ydb_storage_query_remote(c->log,t,c->groupname,\
                c->machineid,c->syncgroup);
        if(0 != err){
            t->sjc->err = err;
            SpxLogFmt2(t->sjc->log,SpxLogError,err,\
                    "query sync storage to tracker %s:%d is fail.",\
                    t->host.ip,t->host.port);
        }
    }
    spx_vector_iter_free(&iter);
    return true;
}/*}}}*/

spx_private err_t ydb_storage_query_remote(SpxLogDelegate *log,
        struct ydb_tracker *t,string_t groupname,
        string_t machineid,string_t syncgroup){/*{{{*/
    err_t err = 0;
    struct spx_job_context *sjc = t->sjc;
    int fd = spx_socket_new(&err);
    if(0 >= fd){
        SpxLog2(log,SpxLogError,err,"create query sync socket fd is fail.");
        return err;
    }
    if(0 != (err = spx_set_nb(fd))){
        SpxLog2(log,SpxLogError,err,"set socket nonblacking is fail.");
        goto r1;
    }
    if(0 != (err = spx_socket_set(fd,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,30))){
        SpxLog2(log,SpxLogError,err,"set socket operator is fail.");
        goto r1;
    }
    if(0 != (err = spx_socket_connect(fd,t->host.ip,t->host.port))){
        SpxLogFmt2(log,SpxLogError,err,\
                "conntect to tracker:%s:%d is fail.",\
                t->host.ip,t->host.port);

        goto r1;
    }

    struct spx_msg_header *writer_header = NULL;
    writer_header = spx_alloc_alone(sizeof(*writer_header),&err);
    if(NULL == writer_header){
        SpxLog2(log,SpxLogError,err,\
                "alloc writer header is fail.");
        goto r1;
    }
    sjc->writer_header = writer_header;
    writer_header->version = YDB_VERSION;
    writer_header->protocol = YDB_QUERY_SYNC_STORAGES;
    writer_header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN \
                             + YDB_SYNCGROUP_LEN ;
    struct spx_msg *ctx = spx_msg_new(writer_header->bodylen,&err);
    if(NULL == ctx){
        SpxLog2(log,SpxLogError,err,\
                "alloc writer body is fail.");
        goto r1;
    }
    sjc->writer_body_ctx = ctx;
    spx_msg_pack_fixed_string(ctx,groupname,YDB_GROUPNAME_LEN);
    spx_msg_pack_fixed_string(ctx,machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_fixed_string(ctx,syncgroup,YDB_SYNCGROUP_LEN);

    sjc->fd = fd;
    spx_nio_regedit_writer(sloop,sjc->fd,sjc);
    return err;
r1:
    SpxClose(fd);
    return err;
}/*}}}*/


spx_private void *ydb_storage_syncquery_heartbeat(){/*{{{*/
    ev_timer_init(sync_timer,ydb_storage_syncquery_handler,(double) 1,(double) 0);//start now
    ev_run(sloop,0);
    return NULL;
}/*}}}*/

spx_private void ydb_storage_syncquery_handler(struct ev_loop *loop,\
        ev_timer *w,int revents){/*{{{*/
    struct spx_job_context *jc = (struct spx_job_context *) w->data;
    struct ydb_storage_configurtion *c = (struct ydb_storage_configurtion *) \
                                         jc->config;
    struct spx_vector_iter *iter = spx_vector_iter_init(c->trackers,&(jc->err));
    if(NULL == iter){
        SpxLog2(jc->log,SpxLogError,jc->err,\
                "init the trackers iter is fail.");
        return;
    }

    struct ydb_tracker *t = NULL;
    while(NULL != (t = spx_vector_iter_next(iter))){
        ydb_storage_query_remote(c->log,
                t,c->groupname,c->machineid,c->syncgroup);
    }
    spx_vector_iter_free(&iter);

    ev_timer_set (sync_timer, (double) 30, 0.);
    ev_timer_again(sloop,sync_timer);
}/*}}}*/

spx_private void ydb_storage_syncquery_nio_body_writer(\
        int fd,struct spx_job_context *jc){/*{{{*/
    spx_nio_writer_body_handler(fd,jc);
    spx_nio_regedit_reader(sloop,jc->fd,jc);
}/*}}}*/

spx_private void ydb_storage_syncquery_nio_body_reader(int fd,
        struct spx_job_context *jc){/*{{{*/
    spx_nio_reader_body_handler(fd,jc);
    if(ENOENT == jc->err){
        SpxLog1(jc->log,SpxLogWarn,\
                "not find out the sync storages.");
        goto r1;
    }

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

    struct ydb_storage_slave *s = NULL;
     size_t unit_size = YDB_MACHINEID_LEN + SpxIpv4Size
                        + SpxI32Size + SpxI32Size;
     size_t numbs = jc->reader_header->bodylen / unit_size;
     size_t i = 0;
     for( ; i < numbs; i++){
        string_t machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(jc->err));
        string_t ip = spx_msg_unpack_string(ctx,SpxIpv4Size,&(jc->err));
        i32_t port = spx_msg_unpack_i32(ctx);
        u32_t status = spx_msg_unpack_u32(ctx);
        spx_map_get(g_ydb_storage_slaves,machineid,spx_string_len(machineid),\
                (void **) &s,NULL);
        if(NULL == s){
            s = spx_alloc_alone(sizeof(*s),&(jc->err));
            if(NULL == s){
                continue;
            }
            s->state = status;
            s->machineid = machineid;
            s->host.port = port;
            s->host.ip = ip;
            spx_map_insert(g_ydb_storage_slaves,machineid,spx_string_len(machineid),
                    s,sizeof(s));
        }
        if(port != s->host.port){
            s->host.port = port;
        }
        if(0 != spx_string_casecmp_string(machineid,s->machineid)){
            spx_string_free(s->machineid);
            s->machineid = machineid;
        }
        s->state = status;

     }
r1:
    spx_job_context_clear(jc);
}/*}}}*/




