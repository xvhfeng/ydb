
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ev.h>

#include "spx_vector.h"
#include "spx_defs.h"
#include "spx_types.h"
#include "spx_socket.h"
#include "spx_alloc.h"
#include "spx_message.h"
#include "spx_io.h"
#include "spx_map.h"
#include "spx_time.h"
#include "spx_periodic.h"
#include "spx_atomic.h"
#include "spx_nio.h"
#include "spx_module.h"
#include "spx_network_module.h"


#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_sync.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_synclog.h"
#include "ydb_storage_dio_context.h"
#include "ydb_storage_dio.h"
#include "ydb_storage_delete.h"

struct spx_threadpool *g_sync_threadpool = NULL;

struct spx_map *g_ydb_storage_remote = NULL;

spx_private err_t ydb_storage_sync_remote_map_vfree(void **arg);

spx_private err_t ydb_storage_sync_query_remote_storage(
        struct ydb_storage_configurtion *c,
        struct ydb_tracker *t, u32_t timeout);

spx_private void ydb_storage_sync_begin(
        void *arg
        );

spx_private err_t ydb_storage_sync_query_sync_beginpoint(
        struct ydb_storage_remote *s,
        int *year,int *month,int *day,u64_t *offset);

spx_private err_t ydb_storage_sync_send_make_state_machine(
        struct ydb_storage_remote *s);

spx_private err_t ydb_storage_sync_doing(
        struct ydb_storage_remote *s);

/*
 * this is over sync of restore and not stop the csync
 */
spx_private err_t ydb_storage_sync_send_consistency(
        struct ydb_storage_remote *s);


spx_private string_t ydb_storage_sync_make_marklog_filename(
        struct ydb_storage_configurtion *c,
        err_t *err);

spx_private err_t ydb_storage_sync_upload_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        struct spx_date *dt_binlog,
        string_t fid);

spx_private err_t ydb_storage_sync_modify_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        struct spx_date *dt_binlog,
        string_t fid,
        string_t ofid);

spx_private err_t ydb_storage_sync_delete_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        struct spx_date *dt_binlog,
        string_t ofid);

spx_private err_t ydb_storage_sync_after(
        int protocol,
        struct ydb_storage_dio_context *dc);

spx_private err_t ydb_storage_sync_log(string_t machineid,
        time_t log_time, char op,string_t fid,string_t ofid);

spx_private void ydb_storage_sync_do_upload_for_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents);

spx_private void ydb_storage_sync_do_upload_for_singlefile(
        struct ev_loop *loop,ev_async *w,int revents);

spx_private void ydb_storage_sync_delete_form_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents);

spx_private void ydb_storage_sync_do_modify_to_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents);

spx_private void ydb_storage_sync_do_modify_to_singlefile(
        struct ev_loop *loop,ev_async *w,int revents);

bool_t ydb_storage_sync_consistency(
        struct ydb_storage_configurtion *c
        ){/*{{{*/
    err_t err = 0;
    if(NULL == g_ydb_storage_remote){
        SpxLog1(c->log,SpxLogMark,
                "no the remote storages in the same sync group."
                "and running the storage force.");
        return true;
    }
    struct spx_map_iter *iter = spx_map_iter_new(g_ydb_storage_remote,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the storage iter for check consistency and to storage running is fail.");
        return false;
    }

    bool_t can_running = true;
    struct spx_map_node *n = NULL;
    time_t now = spx_now();
    while(NULL != (n = spx_map_iter_next(iter,&err))){
        SpxTypeConvert2(struct ydb_storage_remote,s,n->v);
        if(((15 + s->update_timespan + c->query_sync_timespan) > (u64_t) now)
                && (YDB_STORAGE_RUNNING == s->runtime_state
                    || YDB_STORAGE_CSYNCING == s->runtime_state
                    || YDB_STORAGE_DSYNCED ==  s->runtime_state)){
            if(!s->is_restore_over){
                can_running = false;
                break;
            }
        }
    }
    spx_map_iter_free(&iter);
    return can_running;
}/*}}}*/

spx_private err_t ydb_storage_sync_remote_map_vfree(void **arg){/*{{{*/
    if(NULL != *arg){
        struct ydb_storage_remote **s = (struct ydb_storage_remote **) arg;
        if(NULL != (*s)->machineid){
            SpxStringFree((*s)->machineid);
        }
        if(NULL != (*s)->host.ip){
            SpxStringFree((*s)->host.ip);
        }
        if(NULL != (*s)->read_binlog.fname){
            SpxStringFree((*s)->read_binlog.fname);
        }
        if(NULL != (*s)->read_binlog.fp){
            fclose((*s)->read_binlog.fp);
        }
        SpxFree(*s);
    }
    return 0;
}/*}}}*/

void *ydb_storage_sync_query_remote_storages(void *arg){/*{{{*/
    if(NULL == arg){
        return NULL;
    }
    err_t err = 0;
    SpxTypeConvert2(struct ydb_storage_configurtion,c,arg);
    SpxLog1(c->log,SpxLogInfo,
            "query remote in the same syncgroup for sync.");
    struct spx_vector_iter *iter = spx_vector_iter_new(c->trackers ,&err);
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the trackers iter is fail.");
        return NULL;
    }

    struct ydb_tracker *t = NULL;
    while(NULL != (t = spx_vector_iter_next(iter))){
        err = ydb_storage_sync_query_remote_storage(
                c,t,c->timeout);
    }
    spx_vector_iter_free(&iter);
    return NULL;
}/*}}}*/

spx_private err_t ydb_storage_sync_query_remote_storage(
        struct ydb_storage_configurtion *c,
        struct ydb_tracker *t, u32_t timeout
        ){/*{{{*/
    err_t err  = 0;
    if(NULL == t || 0 == timeout){
        err = EINVAL;
        return err;
    }
    struct ydb_storage_transport_context *ystc = NULL;
    if(NULL == t->ystc){
        ystc = spx_alloc_alone(sizeof(*ystc),&err);
        if(NULL == ystc){
            SpxLog2(c->log,SpxLogError,err,
                    "new transport context for query sync storage is fail.");
            return err;
        }
        t->ystc = ystc;
    } else {
        ystc = t->ystc;
    }

    if(NULL == ystc->request){
        ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->request){
            SpxLog2(c->log,SpxLogError,err,
                    "new request for query sync storage is fail.");
            goto r1;
        }
    }

    if(NULL == ystc->request->header){
        struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
        if(NULL == header){
            SpxLog2(c->log,SpxLogError,err,
                    "new header of request for query sync storage is fail.");
            goto r1;
        }
        header->protocol = YDB_S2T_QUERY_SYNC_STORAGES;
        header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN
            + YDB_SYNCGROUP_LEN;
        header->is_keepalive = c->iskeepalive;//persistent connection
        ystc->request->header = header;
    }


    if(NULL == ystc->request->body) {
        struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,&err);
        if(NULL == body){
            SpxLog2(c->log,SpxLogError,err,
                    "new body of request for query sync storage is fail.");
            goto r1;
        }
        ystc->request->body = body;
        spx_msg_pack_fixed_string(body,
                c->groupname,YDB_GROUPNAME_LEN);
        spx_msg_pack_fixed_string(body,
                c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_fixed_string(body,
                c->syncgroup,YDB_SYNCGROUP_LEN);
    }

    while(true) {
        if(0 != ystc->fd){
            if(spx_socket_test(ystc->fd)){
                break;
            } else {
                SpxClose(ystc->fd);
                SpxLogFmt1(c->log,SpxLogWarn,
                        "connection to tracker %s:%d is fail.retry...",
                        t->host.ip,t->host.port);
            }
        }

        if(0 == ystc->fd) {
            ystc->fd  = spx_socket_new(&err);
            if(0 >= ystc->fd){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "new socket to tracker %s:%d is fail.",
                        t->host.ip,t->host.port);
                goto r1;
            }

            if(0 != (err = spx_socket_set(ystc->fd,SpxKeepAlive,SpxAliveTimeout,\
                            SpxDetectTimes,SpxDetectTimeout,\
                            SpxLinger,SpxLingerTimeout,\
                            SpxNodelay,\
                            true,timeout))){
                SpxClose(ystc->fd);
                SpxLogFmt2(c->log,SpxLogError,err,
                        "set socket to tracker %s:%d is fail.",
                        t->host.ip,t->host.port);
                goto r1;
            }
            if(0 != (err = spx_socket_connect_nb(ystc->fd,
                            t->host.ip,t->host.port,timeout))){
                SpxClose(ystc->fd);
                SpxLogFmt2(c->log,SpxLogError,err,
                        "connect to tracker %s:%d is fail.",
                        t->host.ip,t->host.port);
                goto r1;
            }
        }
    }
    if(0 == ystc->fd){
        goto r1;
    }

    err = spx_write_context_nb(c->log,ystc->fd,ystc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "request to tracker:%s:%d for querying sync storages is fail."
                "and close the fd force.",
                t->host.ip,t->host.port);
        SpxClose(ystc->fd);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->fd,c->timeout)){
        //timeout
        err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "read from tracker %s:%d for query sync storages is fail."
                "and close the fd force.",
                t->host.ip,t->host.port);
        SpxClose(ystc->fd);
        goto r1;
    }

    if(NULL == ystc->response){
        ystc->response = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->response){
            SpxLog2(c->log,SpxLogError,err,
                    "new response for query sync storage is fail.");
            goto r1;
        }
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->fd,&err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "get header of response from tracker:%s:%d "
                "for query sync storages is fail,"
                "and close the fd force.",
                t->host.ip,t->host.port);
        SpxClose(ystc->fd);
        goto r1;
    }

    if(0 == ystc->response->header->bodylen) {
        SpxLog1(c->log,SpxLogWarn,
                "no the remote storage in the same syncgroup.");
        goto r1;
    }
    ystc->response->body = spx_read_body_nb(c->log,
            ystc->fd,ystc->response->header->bodylen,&err);
    if(NULL == ystc->response->body){
        SpxLogFmt2(c->log,SpxLogError,err,
                "get body of response from tracker:%s:%d "
                "for query sync storages is fail."
                "and close the fd force.",
                t->host.ip,t->host.port);
        SpxClose(ystc->fd);
        goto r1;
    }

    struct spx_msg *body = ystc->response->body;
    spx_msg_seek(body,0,SpxMsgSeekSet);
    u32_t count = spx_msg_size(body) /
        (YDB_MACHINEID_LEN  + SpxIpv4Size + sizeof(u32_t) + sizeof(u32_t));
    if(0 == count){
        SpxLogFmt1(c->log,SpxLogMark,
                "no sync storages from tracker:%s:%d",
                t->host.ip,t->host.port);
        goto r1;
    }
    u64_t secs = spx_now();
    u32_t i = 0;
    for( ; i < count ; i++){
        string_t machineid = spx_msg_unpack_string(body,YDB_MACHINEID_LEN,&err);
        string_t ip = spx_msg_unpack_string(body,SpxIpv4Size,&err);
        i32_t port = spx_msg_unpack_i32(body);
        u32_t state = spx_msg_unpack_i32(body);

        SpxLogFmt1(c->log,SpxLogInfo,
                "get remote storage:%s,state:%s.",
                machineid,
                ydb_state_desc[state]);

        struct ydb_storage_remote *remote_storage = NULL;
        remote_storage = spx_map_get(g_ydb_storage_remote,
                machineid,spx_string_rlen(machineid),NULL);
        if(NULL != remote_storage){
            SpxStringFree(machineid);//machineid is the same,so not useful
            remote_storage->runtime_state = state;
            if(0 != spx_string_cmp(ip,remote_storage->host.ip)){
                SpxStringFree(remote_storage->host.ip);
                remote_storage->host.ip = ip;
            } else {
                SpxStringFree(ip);
            }
            if(port != remote_storage->host.port){
                remote_storage->host.port = port;
            }
            remote_storage->update_timespan = secs;
        } else {
            remote_storage = (struct ydb_storage_remote *)
                spx_alloc_alone(sizeof(struct ydb_storage_remote),&err);
            if(NULL == remote_storage){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "new sync remote storage:%s get from tracker:%s:%d is fail.",
                        machineid,t->host.ip,t->host.port);
                continue;
            }
            remote_storage->c = c;
            remote_storage->machineid = machineid;
            remote_storage->runtime_state = state;
            remote_storage->host.ip = ip;
            remote_storage->host.port = port;
            remote_storage->update_timespan = secs;
            if(0 != (err = spx_map_insert(g_ydb_storage_remote,
                            machineid,spx_string_rlen(machineid),
                            remote_storage,sizeof(remote_storage)))){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "new sync remote storage:%s get from tracker:%s:%d is fail.",
                        machineid,t->host.ip,t->host.port);
                continue;
            }
        }
    }
r1:
    if(NULL != ystc && NULL != ystc->response){
        if(NULL != ystc->response->header){
            SpxFree(ystc->response->header);
        }
        if(NULL != ystc->response->body){
            SpxMsgFree(ystc->response->body);
        }
    }
    return err;
}/*}}}*/

void *ydb_storage_sync_heartbeat(void *arg){/*{{{*/
    err_t err =0;
    SpxTypeConvert2(struct ydb_storage_configurtion,c,arg);
    if(NULL == g_ydb_storage_remote){
        SpxLog1(c->log,SpxLogMark,
                "no the remote storages in the same sync group."
                "and running the storage force.");
        return NULL;
    }
    struct spx_map_iter *iter = spx_map_iter_new(g_ydb_storage_remote,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the storage iter for check consistency and to storage running is fail.");
        return NULL;
    }

    struct spx_map_node *n = NULL;
    time_t now = spx_now();
    while(NULL != (n = spx_map_iter_next(iter,&err))){
        SpxTypeConvert2(struct ydb_storage_remote,s,n->v);
        SpxLogFmt1(c->log,SpxLogInfo,
                "check remote storage:%s begin csyncing..."
                "storage update time:%lld,query sync timespan:%d,now:%lld.",
                s->machineid,s->update_timespan,c->query_sync_timespan,now);
        // 15 is magic and not modify
        if(((15 + s->update_timespan + c->query_sync_timespan) >= (u64_t) now)
                &&( YDB_STORAGE_DSYNCED == s->runtime_state
                    || YDB_STORAGE_CSYNCING == s->runtime_state
                    || YDB_STORAGE_RUNNING == s->runtime_state)){
            if(!s->is_doing){
                SpxLogFmt1(c->log,SpxLogInfo,
                        "remote storage:%s host:%s:%d begin csync when state is %s.",
                        s->machineid,s->host.ip,s->host.port,
                        ydb_state_desc[s->runtime_state]);
                spx_threadpool_execute(g_sync_threadpool,
                        ydb_storage_sync_begin,
                        s);
            }
        }
    }
    spx_map_iter_free(&iter);
    return NULL;
}/*}}}*/

spx_private void ydb_storage_sync_begin(
        void *arg
        ){/*{{{*/
    if(NULL == arg){
        return;
    }
    err_t err =0;
    SpxTypeConvert2(struct ydb_storage_remote ,s,arg);
    SpxTypeConvert2(struct ydb_storage_configurtion,c,s->c);
    if(!SpxAtomicVIsCas(s->is_doing,false,true)){
        SpxLog1(c->log,SpxLogWarn,
                "set state to csync is fail.");
        return;
    }


    struct ydb_storage_sync_beginpoint beginpoint;
    SpxZero(beginpoint);

    err =  ydb_storage_sync_query_sync_beginpoint(
            s,&(beginpoint.date.year),
            &(beginpoint.date.month),
            &(beginpoint.date.day),
            &(beginpoint.offset));

    if(0 != err){
        SpxLogFmt2(c->log,SpxLogWarn,err,
                "query beginpoint to remote:%s is fail."
                "then exit csync and retry again...",
                s->machineid);
        SpxAtomicVCas(s->is_doing,true,false);
        return;//stop the thread
    }

    SpxLogFmt1(c->log,SpxLogInfo,
            "remote storage:%s begin csyncing...",
            s->machineid);

    if(0 != beginpoint.date.year){
        s->read_binlog.date.year = beginpoint.date.year;
        s->read_binlog.date.month = beginpoint.date.month;
        s->read_binlog.date.day = beginpoint.date.day;
        s->read_binlog.offset = beginpoint.offset;
    } else {
        struct spx_date startup;
        SpxZero(startup);
        spx_get_date((time_t *) &(g_ydb_storage_runtime->this_startup_time),
                &startup);
        s->read_binlog.date.year = startup.year;
        s->read_binlog.date.month = startup.month;
        s->read_binlog.date.day = startup.day;
        s->read_binlog.offset = 0;
    }

    while(true) {
        if(0 != (err = ydb_storage_sync_send_make_state_machine(s))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "send begin sync to remote:%s ip:%s port:%d is fail."
                    "begin syncpoint is %d-%d-%d:%lld"
                    "and sleep %d seconds for remote query sync remote storages.",
                    s->machineid,s->host.ip,s->host.port,
                    s->read_binlog.date.year,
                    s->read_binlog.date.month,
                    s->read_binlog.date.day,
                    s->read_binlog.offset,
                    c->query_sync_timespan);
            spx_periodic_sleep(c->query_sync_timespan,0);
            continue;
        }
        SpxLogFmt1(c->log,SpxLogInfo,
                "remote storage:%s make over the csync-state-machine."
                "csync begin point at %d-%d-%d : %lld",
                s->machineid,
                s->read_binlog.date.year,
                s->read_binlog.date.month,
                s->read_binlog.date.day,
                s->read_binlog.offset);
        break;
    }

    if(0 != (err = ydb_storage_sync_doing(s))){
        SpxLogFmt2(s->c->log,SpxLogError,err,
                "do sync to remote:%s ip:%s port:%d is fail."
                "syncpoint is %d-%d-%d:%lld",
                s->machineid,s->host.ip,s->host.port,
                s->read_binlog.date.year,
                s->read_binlog.date.month,
                s->read_binlog.date.day,
                s->read_binlog.offset);
        SpxAtomicVCas(s->is_doing,true,false);
        return;//stop the thread
    }
}/*}}}*/

spx_private err_t ydb_storage_sync_query_sync_beginpoint(
        struct ydb_storage_remote *s,
        int *year,int *month,int *day,u64_t *offset
        ){/*{{{*/
    err_t err = 0;
    struct ydb_storage_configurtion *c = s->c;
    struct ydb_storage_transport_context *ystc = NULL;
    ystc = spx_alloc_alone(sizeof(*ystc),&err);
    if(NULL == ystc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new transport for query sync beginpoint to remote storage:%s is fail.",
                s->machineid);
        return 0;
    }
    ystc->fd  = spx_socket_new(&err);
    if(0 >= ystc->fd){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new socket to remote storage:%s host:%s:%d "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    if(0 != (err = spx_socket_set(ystc->fd,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "set socket to remote storage:%s host:%s:%d "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
    if(0 != (err = spx_socket_connect_nb(ystc->fd,
                    s->host.ip,s->host.port,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "connect to remote storage:%s host:%s:%d "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
    if(NULL == ystc->request){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request to remote storage:%s host:%s:%d "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
    if(NULL == header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request header to remote storage:%s host:%s:%d "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
    ystc->request->header = header;
    header->protocol = YDB_S2S_QUERY_CSYNC_BEGINPOINT;
    header->bodylen = YDB_MACHINEID_LEN;
    header->is_keepalive = c->iskeepalive;//persistent connection

    struct spx_msg *body = spx_msg_new(header->bodylen,&err);
    if(NULL == body){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request body to remote storage:%s host:%s:%d "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
    ystc->request->body = body;
    spx_msg_pack_string(body,c->machineid);

    err = spx_write_context_nb(c->log,ystc->fd,ystc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write request to remote storage:%s host:%s:%d "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->fd,c->timeout)){
        //timeout
        err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "recv response from remote storage:%s host:%s:%d  "
                "for query sync beginpoint is timeout.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    if(NULL == ystc->response){
        ystc->response = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->response){
            SpxLog2(c->log,SpxLogError,err,
                    "new response for query sync beginpoint is fail.");
            goto r1;
        }
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->fd,&err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recv response header from remote storage:%s host:%s:%d  "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = ystc->response->header->err;
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogWarn,err,
                "recv response from remote storage:%s host:%s:%d  "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    ystc->response->body = spx_read_body_nb(c->log,
            ystc->fd,ystc->response->header->bodylen,&err);
    if(NULL == ystc->response->body){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recv response body from remote storage:%s host:%s:%d  "
                "for query sync beginpoint is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
    err= 0;

    struct spx_msg *reply_body = ystc->response->body;
    spx_msg_seek(reply_body,0,SpxMsgSeekSet);
    *year = spx_msg_unpack_u32(reply_body);
    *month = spx_msg_unpack_u32(reply_body);
    *day = spx_msg_unpack_u32(reply_body);
    *offset = spx_msg_unpack_u64(reply_body);
r1:
    SpxClose(ystc->fd);
    if(NULL != ystc && NULL != ystc->request){
        if(NULL !=ystc->request->header){
            SpxFree(ystc->request->header);
        }
        if(NULL != ystc->request->body){
            SpxMsgFree(ystc->request->body);
        }
        SpxFree(ystc->request);
    }
    if(NULL != ystc && NULL != ystc->response){
        if(NULL != ystc->response->header){
            SpxFree(ystc->response->header);
        }
        if(NULL != ystc->response->body){
            SpxMsgFree(ystc->response->body);
        }
        SpxFree(ystc->response);
    }
    SpxFree(ystc);
    return err;
}/*}}}*/


err_t ydb_storage_sync_reply_sync_beginpoint(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc
        ){/*{{{*/
    if(NULL == dc || NULL == dc->jc){
        return EINVAL;
    }

    struct spx_job_context *jc = dc->jc;
    struct spx_task_context *tc = dc->tc;
    struct ydb_storage_configurtion *c = dc->c;
    struct spx_msg *ctx = jc->reader_body_ctx;
    if(NULL == ctx){
        SpxLog1(dc->log,SpxLogError,\
                "reader body ctx is null.");
        return EINVAL;
    }

    string_t synclog_fname = NULL;
    string_t machineid = NULL;
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(jc->err));
    if(NULL == machineid){
        SpxLog2(dc->log,SpxLogError,jc->err,\
                "unpack machineid from msg ctx is fail.");
        goto r1;
    }

    if(NULL == g_ydb_storage_remote){
        SpxLog1(jc->log,SpxLogError,\
                "the remote storages is null.");
        jc->err = ENOENT;
        goto r1;
    }

    struct ydb_storage_remote *s = NULL;
    s = spx_map_get(g_ydb_storage_remote,machineid,
            spx_string_rlen(machineid),NULL);
    if(NULL == s ){
        SpxLogFmt1(jc->log,SpxLogError,
                "not find sync beginpoint of storage:%s is fail.",
                machineid);
        goto r1;
    }

    struct spx_msg_header *response_header =
        spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(dc->log,SpxLogError,jc->err,\
                "alloc response header for query sync beginpoint is fail.");
        goto r1;
    }

    jc->writer_header = response_header;
    response_header->protocol = YDB_S2S_QUERY_CSYNC_BEGINPOINT;
    response_header->version = YDB_VERSION;
    response_header->bodylen = sizeof(u64_t) + 3 * sizeof(u32_t);
    jc->writer_header_ctx = spx_header_to_msg(response_header,\
            SpxMsgHeaderSize,&(jc->err));
    if(NULL == jc->writer_header_ctx){
        SpxLog2(dc->log,SpxLogError,jc->err,\
                "convert response header to msg ctx is fail.");
        goto r1;
    }
    if(0 != response_header->bodylen) {
        struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,\
                &(jc->err));
        if(NULL == response_body_ctx){
            SpxLogFmt2(dc->log,SpxLogError,jc->err,\
                    "alloc reponse body buffer is fail."\
                    "body buffer length is %d.",\
                    response_header->bodylen);
            goto r1;
        }
        synclog_fname = ydb_storage_synclog_make_filename(c->log,
                c->dologpath,machineid,s->synclog.d.year,
                s->synclog.d.month,s->synclog.d.day,&(jc->err));
        if(NULL == synclog_fname){
            SpxLog2(c->log,SpxLogError,jc->err,
                    "make synclog filename is fail.");
            spx_msg_pack_u64(response_body_ctx,0);
        }
        if(!SpxFileExist(synclog_fname)){
            SpxLogFmt1(c->log,SpxLogError,
                    "make synclog of date:%d-%d-%d is not exist.",
                    s->synclog.d.year,s->synclog.d.month,
                    s->synclog.d.day);
            spx_msg_pack_u64(response_body_ctx,0);
        } else {
            struct stat buf;
            SpxZero(buf);
            stat(synclog_fname,&buf);
            spx_msg_pack_u64(response_body_ctx,buf.st_size);
        }
        jc->writer_body_ctx = response_body_ctx;
        spx_msg_pack_u32(response_body_ctx,s->synclog.d.year);
        spx_msg_pack_u32(response_body_ctx,s->synclog.d.month);
        spx_msg_pack_u32(response_body_ctx,s->synclog.d.day);
    }
    goto r2;
r1:
    if(NULL == jc->writer_header) {
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&(jc->err));
        if(NULL == jc->writer_header){
            SpxLog2(jc->log,SpxLogError,jc->err,\
                    "new response header is fail."
                    "no notify client and push jc force.");
            spx_task_pool_push(g_spx_task_pool,tc);
            ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
            spx_job_pool_push(g_spx_job_pool,jc);
            return jc->err;
        }
    }
    jc->writer_header->protocol = YDB_S2S_QUERY_CSYNC_BEGINPOINT;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
r2:
    SpxStringFree(machineid);
    SpxStringFree(synclog_fname);
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return jc->err;
}/*}}}*/

spx_private err_t ydb_storage_sync_send_make_state_machine(
        struct ydb_storage_remote *s
        ){/*{{{*/
    err_t err = 0;
    struct ydb_storage_configurtion *c = s->c;
    struct ydb_storage_transport_context *ystc = NULL;
    ystc = spx_alloc_alone(sizeof(*ystc),&err);
    if(NULL == ystc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new transport for send begin to remote storage:%s is fail.",
                s->machineid);
        return 0;
    }
    ystc->fd  = spx_socket_new(&err);
    if(0 >= ystc->fd){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new socket to remote storage:%s host:%s:%d "
                "for begin sync is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    if(0 != (err = spx_socket_set(ystc->fd,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "set socket to remote storage:%s host:%s:%d "
                "for begin sync is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
    if(0 != (err = spx_socket_connect_nb(ystc->fd,
                    s->host.ip,s->host.port,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "connect to remote storage:%s host:%s:%d "
                "for begin sync is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
    if(NULL == ystc->request){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request to remote storage:%s host:%s:%d "
                "for begin sync is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
    if(NULL == header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request header to remote storage:%s host:%s:%d "
                "for begin sync is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
    ystc->request->header = header;
    header->protocol = YDB_S2S_CSYNC_BEGIN;
    header->bodylen = YDB_MACHINEID_LEN + 3 * sizeof(u32_t) + sizeof(u64_t);
    header->is_keepalive = c->iskeepalive;//persistent connection

    struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,&err);
    if(NULL == body){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request body to remote storage:%s host:%s:%d "
                "for begin sync is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
    ystc->request->body = body;
    spx_msg_pack_fixed_string(body,c->machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_u32(body,s->read_binlog.date.year);
    spx_msg_pack_u32(body,s->read_binlog.date.month);
    spx_msg_pack_u32(body,s->read_binlog.date.day);
    spx_msg_pack_u64(body,s->read_binlog.offset);

    err = spx_write_context_nb(c->log,ystc->fd,ystc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write request to remote storage:%s host:%s:%d "
                "for begin sync is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->fd,c->timeout)){
        //timeout
        err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "recv response from remote storage:%s host:%s:%d  "
                "for begin sync is timeout.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    if(NULL == ystc->response){
        ystc->response = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->response){
            SpxLog2(c->log,SpxLogError,err,
                    "new response for begin sync is fail.");
            goto r1;
        }
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->fd,&err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recv response header from remote storage:%s host:%s:%d  "
                "for begin sync is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = ystc->response->header->err;
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogWarn,err,
                "recv response from remote storage:%s host:%s:%d  "
                "for begin sync is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

r1:
    SpxClose(ystc->fd);
    if(NULL != ystc && NULL != ystc->request){
        if(NULL !=ystc->request->header){
            SpxFree(ystc->request->header);
        }
        if(NULL != ystc->request->body){
            SpxMsgFree(ystc->request->body);
        }
        SpxFree(ystc->request);
    }
    if(NULL != ystc && NULL != ystc->response){
        if(NULL != ystc->response->header){
            SpxFree(ystc->response->header);
        }
        if(NULL != ystc->response->body){
            SpxMsgFree(ystc->response->body);
        }
        SpxFree(ystc->response);
    }
    SpxFree(ystc);
    return err;
}/*}}}*/

err_t ydb_storage_sync_reply_make_state_machine(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc
        ){/*{{{*/
    if(NULL == dc || NULL == dc->jc){
        return EINVAL;
    }

    err_t err = 0;
    struct spx_job_context *jc = dc->jc;
    struct spx_task_context *tc = dc->tc;
    struct ydb_storage_configurtion *c = dc->c;
    struct spx_msg *ctx = jc->reader_body_ctx;
    if(NULL == ctx){
        SpxLog1(dc->log,SpxLogError,\
                "reader body ctx is null.");
        return EINVAL;
    }

    string_t synclog_fname = NULL;
    string_t machineid = NULL;
    int year = 0;
    int month = 0;
    int day = 0;
    u64_t offset = 0;
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(err));
    if(NULL == machineid){
        SpxLog2(dc->log,SpxLogError,err,\
                "unpack machineid from msg ctx is fail.");
        goto r1;
    }
    year = spx_msg_unpack_u32(ctx);
    month = spx_msg_unpack_u32(ctx);
    day = spx_msg_unpack_u32(ctx);
    offset = spx_msg_unpack_u64(ctx);

    if(NULL == g_ydb_storage_remote){
        SpxLog1(jc->log,SpxLogError,\
                "the remote storages is null.");
        err = ENOENT;
        goto r1;
    }

    struct ydb_storage_remote *s = NULL;
    s = spx_map_get(g_ydb_storage_remote,machineid,
            spx_string_rlen(machineid),NULL);
    if(NULL == s){
        SpxLogFmt1(jc->log,SpxLogError,
                "not find storage:%s from remote storages is fail.",
                machineid);
        goto r1;
    }

    ydb_storage_synclog_clear(&(s->synclog));
    err = ydb_storage_synclog_init(&(s->synclog),
            c->log,c->dologpath,s->machineid,
            year,month,day,offset);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "init synclog with storage:%s by date:%d-%d-%d "
                "offset:%ld is fail.",
                machineid,year,month,day,offset);
        goto r1;
    }

    struct spx_msg_header *response_header =
        spx_alloc_alone(sizeof(*response_header),&(err));
    if(NULL == response_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc response header for begin sync is fail.");
        goto r1;
    }

    jc->writer_header = response_header;
    response_header->protocol = YDB_S2S_CSYNC_BEGIN;
    response_header->version = YDB_VERSION;
    response_header->bodylen = sizeof(u64_t) + 3 * sizeof(u32_t);
    jc->writer_header_ctx = spx_header_to_msg(response_header,\
            SpxMsgHeaderSize,&(err));
    if(NULL == jc->writer_header_ctx){
        SpxLog2(dc->log,SpxLogError,err,\
                "convert response header to msg ctx is fail.");
        goto r1;
    }
    if(0 != response_header->bodylen) {
        struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,\
                &(err));
        if(NULL == response_body_ctx){
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "alloc reponse body buffer is fail."\
                    "body buffer length is %d.",\
                    response_header->bodylen);
            goto r1;
        }
        synclog_fname = ydb_storage_synclog_make_filename(c->log,
                c->dologpath,machineid,s->synclog.d.year,
                s->synclog.d.month,s->synclog.d.day,&(err));
        if(NULL == synclog_fname){
            SpxLog2(c->log,SpxLogError,err,
                    "make synclog filename is fail.");
            spx_msg_pack_u64(response_body_ctx,0);
        }
        if(!SpxFileExist(synclog_fname)){
            SpxLogFmt1(c->log,SpxLogError,
                    "make synclog of date:%d-%d-%d is not exist.",
                    s->synclog.d.year,s->synclog.d.month,
                    s->synclog.d.day);
            spx_msg_pack_u64(response_body_ctx,0);
        } else {
            struct stat buf;
            SpxZero(buf);
            stat(synclog_fname,&buf);
            spx_msg_pack_u64(response_body_ctx,buf.st_size);
        }
        jc->writer_body_ctx = response_body_ctx;
        spx_msg_pack_u32(response_body_ctx,s->synclog.d.year);
        spx_msg_pack_u32(response_body_ctx,s->synclog.d.month);
        spx_msg_pack_u32(response_body_ctx,s->synclog.d.day);
    }
    goto r2;
r1:
    if(NULL == jc->writer_header) {
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&(err));
        if(NULL == jc->writer_header){
            SpxLog2(dc->log,SpxLogError,err,\
                    "new response header is fail."
                    "no notify client and push jc force.");
            spx_task_pool_push(g_spx_task_pool,tc);
            ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
            spx_job_pool_push(g_spx_job_pool,jc);
            return err;
        }
    }
    jc->writer_header->protocol = YDB_S2S_CSYNC_BEGIN;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;
r2:
    SpxStringFree(machineid);
    SpxStringFree(synclog_fname);
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_sync_doing(
        struct ydb_storage_remote *s
        ){/*{{{*/
    err_t err = 0;
    struct ydb_storage_configurtion *c = s->c;

    if(spx_date_is_after(&(s->read_binlog.date))){
        struct spx_date today;
        SpxZero(today);
        spx_get_today(&today);

        SpxLogFmt1(c->log,SpxLogWarn,
                "the day:%d-%d-%d is after today:%d-%d-%d,"
                "so must back to today.",
                s->read_binlog.date.year,
                s->read_binlog.date.month,
                s->read_binlog.date.day,
                today.year,today.month,today.day);

        s->read_binlog.date.year = today.year;
        s->read_binlog.date.month = today.month;
        s->read_binlog.date.day = today.day;

        s->read_binlog.fname = ydb_storage_binlog_make_filename(c->log,
                c->dologpath,c->machineid,s->read_binlog.date.year,
                s->read_binlog.date.month,s->read_binlog.date.day,&err);
        if(NULL == s->read_binlog.fname){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "make binlog of date:%d-%d-%d for sync is fail.",
                    s->read_binlog.date.year,
                    s->read_binlog.date.month,
                    s->read_binlog.date.day);
            return err;
        }
        if(SpxFileExist(s->read_binlog.fname)){
            struct stat buf;
            SpxZero(buf);

            if(0 != stat(s->read_binlog.fname,&buf)){
                err = 0 == errno ? EACCES : errno;
                SpxLogFmt1(c->log,SpxLogError,
                        "get binlog of date:%d-%d-%d is fail.",
                        s->read_binlog.date.year,
                        s->read_binlog.date.month,
                        s->read_binlog.date.day);
                SpxStringFree(s->read_binlog.fname);
                return err;
            }
            s->read_binlog.offset = buf.st_size;
        }
    }

    string_t line = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),&err);
    if(NULL == line){
        SpxLog2(c->log,SpxLogError,err,
                "new line for sync is fail.");
        SpxStringFree(s->read_binlog.fname);
        return err;
    }

    bool_t is_send = false;
    u32_t secs = 0;
    while(true){
        if(NULL == s->read_binlog.fp){
            if(NULL == s->read_binlog.fname){
                s->read_binlog.fname = ydb_storage_binlog_make_filename(c->log,
                        c->dologpath,c->machineid,s->read_binlog.date.year,
                        s->read_binlog.date.month,s->read_binlog.date.day,&err);
                if(NULL == s->read_binlog.fname){
                    SpxStringFree(line);
                    SpxLogFmt2(c->log,SpxLogError,err,
                            "make binlog of date:%d-%d-%d for sync is fail.",
                            s->read_binlog.date.year,
                            s->read_binlog.date.month,
                            s->read_binlog.date.day);
                    return err;
                }
            }

            if(!SpxFileExist(s->read_binlog.fname)){
                if (spx_date_is_before(&(s->read_binlog.date))){
                    SpxLogFmt1(c->log,SpxLogMark,
                            "the day:%d-%d-%d sync is over."
                            "then add 1 day.",
                            s->read_binlog.date.year,
                            s->read_binlog.date.month,
                            s->read_binlog.date.day);

                    SpxStringFree(s->read_binlog.fname);
                    spx_date_add(&(s->read_binlog.date),1);
                    s->read_binlog.offset = 0;
                    continue;
                } else {
                    SpxLogFmt1(c->log,SpxLogInfo,
                            "sync data of day:%d-%d-%d is to end."
                            "then waitting and retry again...",
                            s->read_binlog.date.year,
                            s->read_binlog.date.month,
                            s->read_binlog.date.day);

                    if (s->runtime_state == YDB_STORAGE_RUNNING){
                        SpxLogFmt1(c->log,SpxLogDebug,
                                "remote storage:%s."
                                "host:%s:%d. is running so not send consistency." ,
                                s->machineid,s->host.ip,s->host.port);
                    } else {
                        SpxLogFmt1(c->log,SpxLogInfo,
                                "send consistency to remote storage:%s."
                                "host:%s:%d." ,
                                s->machineid,s->host.ip,s->host.port);
                        if(!is_send || c->query_sync_timespan < secs) {
                            if(0 != (err = ydb_storage_sync_send_consistency(s))){
                                SpxLogFmt2(c->log,SpxLogError,err,
                                        "send consistency to remote storage:%s."
                                        "host:%s:%d. is fail. and retry again when next loop..." ,
                                        s->machineid,s->host.ip,s->host.port);
                            }
                            is_send = true;
                            secs = 0;
                        }
                        secs += 0 < c->sync_wait ? c->sync_wait : 1;
                    }

                    if(0 < c->sync_wait) {
                        spx_periodic_sleep(c->sync_wait,0);//add a configurtion item
                    }
                    continue;
                }
            }

            struct stat buf;
            SpxZero(buf);
            lstat(s->read_binlog.fname,&buf);
            if((u64_t) buf.st_size == s->read_binlog.offset){//end of the binlog
                if (spx_date_is_before(&(s->read_binlog.date))){
                    SpxLogFmt1(c->log,SpxLogMark,
                            "the day:%d-%d-%d sync is over."
                            "then add 1 day.",
                            s->read_binlog.date.year,
                            s->read_binlog.date.month,
                            s->read_binlog.date.day);

                    SpxStringFree(s->read_binlog.fname);
                    spx_date_add(&(s->read_binlog.date),1);
                    s->read_binlog.offset = 0;
                    if(NULL != s->read_binlog.fp) {
                        fclose(s->read_binlog.fp);
                        s->read_binlog.fp = NULL;
                    }
                    continue;
                }

                if (s->runtime_state == YDB_STORAGE_RUNNING){
                    SpxLogFmt1(c->log,SpxLogDebug,
                            "remote storage:%s."
                            "host:%s:%d. is running so not send consistency." ,
                            s->machineid,s->host.ip,s->host.port);
                } else {
                    SpxLogFmt1(c->log,SpxLogInfo,
                            "send consistency to remote storage:%s."
                            "host:%s:%d." ,
                            s->machineid,s->host.ip,s->host.port);
                    if(!is_send || c->query_sync_timespan < secs) {
                        if(0 != (err = ydb_storage_sync_send_consistency(s))){
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "send consistency to remote storage:%s."
                                    "host:%s:%d. is fail. and retry again when next loop..." ,
                                    s->machineid,s->host.ip,s->host.port);
                        }
                        is_send = true;
                        secs = 0;
                    }
                    secs += 0 < c->sync_wait ? c->sync_wait : 1;
                }


                if(0 < c->sync_wait) {
                    spx_periodic_sleep(c->sync_wait,0);//add a configurtion item
                }
                continue;
            }

            s->read_binlog.fp = SpxFReadOpen(s->read_binlog.fname);
            if(NULL == s->read_binlog.fp){
                err = 0 == errno ? EACCES : errno;
                SpxLogFmt2(c->log,SpxLogError,err,
                        "open binlog of date:%d-%d-%d for sync is fail.",
                        s->read_binlog.date.year,
                        s->read_binlog.date.month,
                        s->read_binlog.date.day);
                SpxStringFree(line);
                SpxStringFree(s->read_binlog.fname);
                return err;
            }
            if(0 != s->read_binlog.offset){
                if(0 > fseek(s->read_binlog.fp,s->read_binlog.offset,SEEK_SET)){
                    err = 0 == errno ? EACCES : errno;
                    SpxLogFmt2(c->log,SpxLogError,err,
                            "seek binlog of date:%d-%d-%d to %lld for sync is fail.",
                            s->read_binlog.date.year,
                            s->read_binlog.date.month,
                            s->read_binlog.date.day,
                            s->read_binlog.offset);
                    SpxStringFree(s->read_binlog.fname);
                    fclose(s->read_binlog.fp);
                    s->read_binlog.fp = NULL;
                    return err;
                }
            }
        }


        SpxLogFmt1(c->log,SpxLogInfo,
                "sync data of day:%d-%d-%d offset:%lld is to begining...",
                s->read_binlog.date.year,
                s->read_binlog.date.month,
                s->read_binlog.date.day,
                s->read_binlog.offset);

        int size = strlen("\t");
        while(NULL != (fgets(line,SpxLineSize,s->read_binlog.fp))){
            spx_string_updatelen(line);
            s->read_binlog.offset += spx_string_len(line);
            spx_string_strip_linefeed(line);
            int count = 0;
            string_t *strs = spx_string_split(line,"\t",size,&count,&err);
            if(NULL == strs || 0 != err){
                spx_string_clear(line);
                continue;
            }
            switch(*line){
                case (YDB_STORAGE_LOG_UPLOAD):
                    {
                        if(2 != count){
                            SpxLogFmt1(c->log,SpxLogError,
                                    "binlog file line:%s format is fail.",
                                    line);
                            break;
                        }
                        if(0 != (err = ydb_storage_sync_upload_request(
                                        c,s,&(s->read_binlog.date),*(strs + 1)))){
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "sync binlog file line:%s is fail.",
                                    line);
                        }
                        break;
                    }
                case (YDB_STORAGE_LOG_DELETE):
                    {
                        if(2 != count){
                            SpxLogFmt1(c->log,SpxLogError,
                                    "binlog file line:%s format is fail.",
                                    line);
                            break;
                        }
                        if( 0 != (err = ydb_storage_sync_delete_request(
                                        c,s,&(s->read_binlog.date),
                                        *(strs + 1)))) {
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "sync binlog file line:%s is fail.",
                                    line);
                        }
                        break;
                    }
                case (YDB_STORAGE_LOG_MODIFY):
                    {
                        if(3 != count){
                            SpxLogFmt1(c->log,SpxLogError,
                                    "binlog file line:%s format is fail.",
                                    line);
                            break;
                        }
                        if(0 != (err = ydb_storage_sync_modify_request(
                                        c,s,&(s->read_binlog.date),
                                        *(strs + 1),*(strs + 2)))) {
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "sync binlog file line:%s is fail.",
                                    line);
                        }
                        break;
                    }
                default:{
                            SpxLogFmt1(c->log,SpxLogError,
                                    "no the operator for sync in the binlog file "
                                    "of line:%s format is fail.",
                                    line);

                            break;
                        }
            }
            spx_string_free_splitres(strs,count);
            spx_string_clear(line);
            if(0 < c->sync_wait) {
                spx_periodic_sleep(c->sync_wait,0);
            }
        }

        if (spx_date_is_before(&(s->read_binlog.date))){
            SpxLogFmt1(c->log,SpxLogMark,
                    "the day:%d-%d-%d sync is over."
                    "then add 1 day.",
                    s->read_binlog.date.year,
                    s->read_binlog.date.month,
                    s->read_binlog.date.day);

            SpxStringFree(s->read_binlog.fname);
            spx_date_add(&(s->read_binlog.date),1);
            s->read_binlog.offset = 0;
            if(NULL != s->read_binlog.fp) {
                fclose(s->read_binlog.fp);
                s->read_binlog.fp = NULL;
            }
            continue;
        } else {
            if (s->runtime_state == YDB_STORAGE_RUNNING){
                SpxLogFmt1(c->log,SpxLogDebug,
                        "remote storage:%s."
                        "host:%s:%d. is running so not send consistency." ,
                        s->machineid,s->host.ip,s->host.port);
            } else {
                SpxLogFmt1(c->log,SpxLogInfo,
                        "send consistency to remote storage:%s."
                        "host:%s:%d." ,
                        s->machineid,s->host.ip,s->host.port);
                if(!is_send || c->query_sync_timespan < secs) {
                    if(0 != (err = ydb_storage_sync_send_consistency(s))){
                        SpxLogFmt2(c->log,SpxLogError,err,
                                "send consistency to remote storage:%s."
                                "host:%s:%d. is fail. and retry again when next loop..." ,
                                s->machineid,s->host.ip,s->host.port);
                    }
                    is_send = true;
                    secs = 0;
                }
                secs += 0 < c->sync_wait ? c->sync_wait : 1;
            }

            SpxLogFmt1(c->log,SpxLogInfo,
                    "sync data of day:%d-%d-%d is to end."
                    "then waitting and retry again...",
                    s->read_binlog.date.year,
                    s->read_binlog.date.month,
                    s->read_binlog.date.day);
            if(0 < c->sync_wait){
                spx_periodic_sleep(c->sync_wait,0);//add a configurtion item
            }
            if(NULL != s->read_binlog.fp) {
                fclose(s->read_binlog.fp);
                s->read_binlog.fp = NULL;
            }
            continue;
        }
    }
    SpxStringFree(line);
}/*}}}*/

/*
 * this is over sync of restore and not stop the csync
 */
spx_private err_t ydb_storage_sync_send_consistency(
        struct ydb_storage_remote *s
        ){/*{{{*/
    err_t err = 0;
    struct ydb_storage_configurtion *c = s->c;
    struct ydb_storage_transport_context *ystc = NULL;
    ystc = spx_alloc_alone(sizeof(*ystc),&err);
    if(NULL == ystc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new transport for over sync of restoring "
                "to remote storage:%s is fail.",
                s->machineid);
        return 0;
    }
    if(0 == ystc->fd) {
        ystc->fd  = spx_socket_new(&err);
        if(0 >= ystc->fd){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new socket to remote storage:%s host:%s:%d "
                    "for over sync of restoring is fail.",
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }

        if(0 != (err = spx_socket_set(ystc->fd,SpxKeepAlive,SpxAliveTimeout,\
                        SpxDetectTimes,SpxDetectTimeout,\
                        SpxLinger,SpxLingerTimeout,\
                        SpxNodelay,\
                        true,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "set socket to remote storage:%s host:%s:%d "
                    "for over sync of restoring is fail.",
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
        if(0 != (err = spx_socket_connect_nb(ystc->fd,
                        s->host.ip,s->host.port,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "connect to remote storage:%s host:%s:%d "
                    "for over sync of restoring is fail.",
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request){
        ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->request){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request to remote storage:%s host:%s:%d "
                    "for over sync of restoring is fail.",
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request->header){
        struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
        if(NULL == header){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request header to remote storage:%s host:%s:%d "
                    "for over sync of restoring is fail.",
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
        ystc->request->header = header;
        header->protocol = YDB_S2S_RESTORE_CSYNC_OVER;
        header->bodylen = YDB_MACHINEID_LEN;
        header->is_keepalive = c->iskeepalive;//persistent connection
    }

    if(NULL == ystc->request->body){
        struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,&err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request body to remote storage:%s host:%s:%d "
                    "for over sync of restoring is fail.",
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
        ystc->request->body = body;
        spx_msg_pack_fixed_chars(body,c->machineid,YDB_MACHINEID_LEN);
    }


    err = spx_write_context_nb(c->log,ystc->fd,ystc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write request to remote storage:%s host:%s:%d "
                "for over sync of restoring is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->fd,c->timeout)){
        //timeout
        err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "recv response from remote storage:%s host:%s:%d  "
                "for over sync of restoring is timeout.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    if(NULL == ystc->response){
        ystc->response = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->response){
            SpxLog2(c->log,SpxLogError,err,
                    "new response for over sync of restoring is fail.");
            goto r1;
        }
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->fd,&err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recv response header from remote storage:%s host:%s:%d  "
                "for over sync of restoring is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = ystc->response->header->err;
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogWarn,err,
                "recv response from remote storage:%s host:%s:%d  "
                "for over sync of restoring is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
r1:
    SpxClose(ystc->fd);
    if(NULL != ystc && NULL != ystc->request){
        if(NULL !=ystc->request->header){
            SpxFree(ystc->request->header);
        }
        if(NULL != ystc->request->body){
            SpxMsgFree(ystc->request->body);
        }
        SpxFree(ystc->request);
    }
    if(NULL != ystc && NULL != ystc->response){
        if(NULL != ystc->response->header){
            SpxFree(ystc->response->header);
        }
        if(NULL != ystc->response->body){
            SpxMsgFree(ystc->response->body);
        }
        SpxFree(ystc->response);
    }
    SpxFree(ystc);
    return err;
}/*}}}*/

err_t ydb_storage_sync_reply_consistency(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc
        ){/*{{{*/
    if(NULL == dc || NULL == dc->jc){
        return EINVAL;
    }

    struct spx_job_context *jc = dc->jc;
    struct spx_task_context *tc = dc->tc;
    struct spx_msg *ctx = jc->reader_body_ctx;
    if(NULL == ctx){
        SpxLog1(dc->log,SpxLogError,\
                "reader body ctx is null.");
        return EINVAL;
    }

    string_t machineid = NULL;
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(jc->err));
    if(NULL == machineid){
        SpxLog2(dc->log,SpxLogError,jc->err,\
                "unpack machineid from msg ctx is fail.");
        goto r1;
    }

    if(NULL == g_ydb_storage_remote){
        SpxLog1(jc->log,SpxLogError,\
                "the remote storages is null.");
        jc->err = ENOENT;
        goto r1;
    }

    struct ydb_storage_remote *s = NULL;
    s = spx_map_get(g_ydb_storage_remote,machineid,
            spx_string_rlen(machineid),NULL);
    if(NULL == s ){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,\
                "not found the storage:%s from remote storages.",
                machineid);
        jc->err = ENOENT;
        goto r1;
    }
    s->is_restore_over = true;

    struct spx_msg_header *response_header =
        spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(dc->log,SpxLogError,jc->err,\
                "alloc response header for over csync is fail.");
        goto r1;
    }

    jc->writer_header = response_header;
    response_header->protocol = YDB_S2S_RESTORE_CSYNC_OVER;
    response_header->version = YDB_VERSION;
    response_header->bodylen = 0;
    jc->writer_header_ctx = spx_header_to_msg(response_header,\
            SpxMsgHeaderSize,&(jc->err));
    if(NULL == jc->writer_header_ctx){
        SpxLog2(dc->log,SpxLogError,jc->err,\
                "convert response header to msg ctx is fail.");
        goto r1;
    }

    goto r2;
r1:
    if(NULL == jc->writer_header) {
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&(jc->err));
        if(NULL == jc->writer_header){
            SpxLog2(dc->log,SpxLogError,jc->err,\
                    "new response header is fail."
                    "no notify client and push jc force.");
            spx_task_pool_push(g_spx_task_pool,tc);
            ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
            spx_job_pool_push(g_spx_job_pool,jc);
            return jc->err;
        }
    }
    response_header->protocol = YDB_S2S_RESTORE_CSYNC_OVER;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = jc->err;
r2:
    spx_string_free(machineid);
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return jc->err;
}/*}}}*/

/*
 * fileline context format:
 *  machineid:year:month:day:offset
 */
err_t ydb_storage_sync_restore(
        struct ydb_storage_configurtion *c
        ){/*{{{*/
    err_t err = 0;
    if(NULL == g_ydb_storage_remote){
        g_ydb_storage_remote = spx_map_new(c->log,
                spx_pjw,
                spx_collection_string_default_cmper,
                NULL,
                NULL,
                ydb_storage_sync_remote_map_vfree,
                &err);
        if(NULL == g_ydb_storage_remote){
            SpxLog2(c->log,SpxLogError,err,
                    "new remote sync storages is fail.");
            return err;
        }
    }
    string_t fname = NULL;
    FILE *fp = NULL;
    string_t line = NULL;

    fname = ydb_storage_sync_make_marklog_filename(c,&err);
    if(NULL == fname){
        SpxLog2(c->log,SpxLogError,err,
                "make sync marklog filename is fail.");
        goto r2;
    }
    if(!SpxFileExist(fname)){
        SpxLog1(c->log,SpxLogError,
                "the sync marklog filename is not exist.");
        goto r2;
    }
    line = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),&err);
    if(NULL == line){
        SpxLog2(c->log,SpxLogError,err,
                "new line of sync marklog file is  fail.");
        goto r2;
    }

    fp = SpxFReadOpen(fname);
    if(NULL == fp){
        SpxLog2(c->log,SpxLogError,err,
                "open sync marklog file is fail.");
        goto r2;
    }

    int len = strlen(":");
    while(NULL != (fgets(line,SpxLineSize,fp))){
        spx_string_updatelen(line);
        spx_string_strip_linefeed(line);
        if('#' == *line){
            spx_string_clear(line);
            continue;
        }
        int count = 0;
        struct ydb_storage_remote *s = NULL;
        string_t *strs = spx_string_split(line,":",len,&count,&err);
        if(NULL ==  strs || 0 != err || 9 != count){
            spx_string_clear(line);
            continue;
        }
        int i = 0;
        for( ; i < count; i++){
            switch (i) {
                case (0):{
                             string_t machineid = *(strs + i);
                             if(SpxStringIsNullOrEmpty(machineid)){
                                 err = ENOENT;
                                 SpxLogFmt2(c->log,SpxLogError,err,
                                         "no machineid in the line:%s for parser.",
                                         line);
                                 goto r1;
                             }
                             s = spx_alloc_alone(sizeof(*s),&err);
                             if(NULL == s){
                                 SpxLogFmt2(c->log,SpxLogError,err,
                                         "new remote-storage:%s is fail.",
                                         machineid);
                                 goto r1;
                             }
                             s->machineid = spx_string_dup(machineid,&err);
                             if(NULL == s->machineid){
                                 SpxFree(s);
                                 SpxLogFmt2(c->log,SpxLogError,err,
                                         "dup remote-storage id:%s is fail.",
                                         machineid);
                                 goto r1;
                             }
                             err = spx_map_insert(g_ydb_storage_remote,s->machineid,
                                     spx_string_rlen(machineid),s,sizeof(s));
                             if(0 != err){
                                 SpxLogFmt2(c->log,SpxLogError,err,
                                         "add remote-storage:%s to glb is fail.",
                                         machineid);
                                 SpxStringFree(s->machineid);
                                 SpxFree(s);
                                 goto r1;
                             }
                             s->c = c;
                             break;
                         }
                case (1):{
                             string_t syear = *(strs + i);
                             if(SpxStringIsNullOrEmpty(syear)){
                                 s->read_binlog.date.year = 0;
                             } else {
                                 s->read_binlog.date.year = atoi(syear);
                             }
                             break;
                         }
                case (2):{
                             string_t smonth = *(strs + i);
                             if(SpxStringIsNullOrEmpty(smonth)){
                                 s->read_binlog.date.month = 0;
                             } else {
                                 s->read_binlog.date.month = atoi(smonth);
                             }
                             break;
                         }
                case (3):{
                             string_t day = *(strs + i);
                             if(SpxStringIsNullOrEmpty(day)){
                                 s->read_binlog.date.day = 0;
                             } else {
                                 s->read_binlog.date.day = atoi(day);
                             }
                             break;
                         }
                case (4):{
                             string_t off = *(strs + i);
                             if(SpxStringIsNullOrEmpty(off)){
                                 s->read_binlog.offset = 0;
                             } else {
                                 s->read_binlog.offset = atoll(off);
                             }
                         }
                case (5):{
                             string_t syear = *(strs + i);
                             if(SpxStringIsNullOrEmpty(syear)){
                                 s->synclog.d.year = 0;
                             } else {
                                 s->synclog.d.year = atoi(syear);
                             }
                             break;
                         }
                case (6):{
                             string_t smonth = *(strs + i);
                             if(SpxStringIsNullOrEmpty(smonth)){
                                 s->synclog.d.month = 0;
                             } else {
                                 s->synclog.d.month = atoi(smonth);
                             }
                             break;
                         }
                case (7):{
                             string_t day = *(strs + i);
                             if(SpxStringIsNullOrEmpty(day)){
                                 s->synclog.d.day = 0;
                             } else {
                                 s->synclog.d.day = atoi(day);
                             }
                             break;
                         }
                case (8):{
                             string_t off = *(strs + i);
                             if(SpxStringIsNullOrEmpty(off)){
                                 s->synclog.off = 0;
                             } else {
                                 s->synclog.off = atoll(off);
                             }
                         }
                default:{
                            break;
                        }
            }
        }
r1:
        spx_string_clear(line);
        spx_string_free_splitres(strs,count);
    }
r2:
    if(NULL != line){
        SpxStringFree(line);
    }
    if(NULL != fname){
        SpxStringFree(fname);
    }
    if(NULL != fp){
        fclose(fp);
        fp = NULL;
    }
    return err;
}/*}}}*/

err_t ydb_storage_sync_state_writer(
        struct ydb_storage_configurtion *c
        ){/*{{{*/
    err_t err = 0;
    string_t fname = NULL;
    FILE *fp = NULL;
    string_t context = NULL;

    context = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),&err);
    if(NULL == context){
        SpxLog2(c->log,SpxLogError,err,
                "new context of sync marklog is fail.");
        goto r1;
    }

    string_t new_context = NULL;
    new_context = spx_string_cat_printf(&err,context,"%s\n",
            "# machineid:binlog-year:binlog-month:binlog-day:binlog-offset"
            ":synclog-year:synclog-month:synclog-day:synclog-offset");
    if(NULL == new_context){
        SpxLog2(c->log,SpxLogError,err,
                "cat and printf comment context of sync marklog is fail.");
        goto r1;
    }
    context = new_context;

    struct spx_map_iter *iter = spx_map_iter_new(g_ydb_storage_remote,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the trackers iter is fail.");
        goto r1;
    }
    struct spx_map_node *n = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&err))){
        SpxTypeConvert2(struct ydb_storage_remote,s,n->v);
        new_context = spx_string_cat_printf(&err,context,"%s:%d:%d:%d:%lld:%d:%d:%d:%lld\n",
                s->machineid,s->read_binlog.date.year,
                s->read_binlog.date.month,
                s->read_binlog.date.day,
                s->read_binlog.offset,
                s->synclog.d.year,
                s->synclog.d.month,
                s->synclog.d.day,
                s->synclog.off);
        if(NULL == new_context){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "cat and printf context of sync marklog by storage:%s is fail.",
                    s->machineid);
            spx_map_iter_free(&iter);
            break;
        }
        context = new_context;
    }
    spx_map_iter_free(&iter);

    fname = ydb_storage_sync_make_marklog_filename(c,&err);
    if(NULL == fname){
        SpxLog2(c->log,SpxLogError,err,
                "make marklog filename is fail.");
        goto r1;
    }
    fp = SpxFWriteOpen(fname,true);
    if(NULL == fp){
        SpxLogFmt2(c->log,SpxLogError,err,
                "open marklog:%s is fail.",
                fname);
        goto r1;
    }
    size_t len = 0;
    size_t size = spx_string_len(context);
    err = spx_fwrite_string(fp,context,size,&len);
    if(0 != err || size != len){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write marklog context is fail."
                "size:%ld,len:%ld",
                size,len);
        goto r1;
    }
r1:
    if(NULL != fname){
        SpxStringFree(fname);
    }
    if(NULL != fp){
        fclose(fp);
        fp = NULL;
    }
    if(NULL != context){
        SpxStringFree(context);
    }
    return err;
}/*}}}*/

spx_private string_t ydb_storage_sync_make_marklog_filename(
        struct ydb_storage_configurtion *c,
        err_t *err
        ){/*{{{*/
    string_t fname = spx_string_newlen(NULL,SpxStringRealSize(SpxFileNameSize),err);
    if(NULL == fname){
        SpxLog2(c->log,SpxLogError,*err,
                "new sync state filename is fail.");
        return NULL;
    }

    string_t new_fname = NULL;
    if(SpxStringEndWith(c->basepath,SpxPathDlmt)){
        new_fname = spx_string_cat_printf(err,fname,
                "%s.%s-remote-storage.marklog",
                c->basepath,c->machineid);
    } else {
        new_fname = spx_string_cat_printf(err,fname,
                "%s%c.%s-remote-storage.marklog",
                c->basepath,SpxPathDlmt,c->machineid);
    }

    if(NULL == new_fname){
        SpxLog2(c->log,SpxLogError,*err,
                "format remote storage marklog filename is fail.");
        SpxStringFree(fname);
    }
    fname = new_fname;
    return fname;
}/*}}}*/

spx_private err_t ydb_storage_sync_upload_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        struct spx_date *dt_binlog,
        string_t fid
        ){/*{{{*/
    err_t err = 0;
    bool_t is_no_file = false;
    struct ydb_storage_sync_context *yssc = NULL;
    yssc = spx_alloc_alone(sizeof(*yssc),&err);
    if(NULL == yssc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new sync object is fail.",
                "fid:%s.",fid);
        return err;
    }

    struct ydb_storage_fid *fidbuf =
        spx_alloc_alone(sizeof(struct ydb_storage_fid),&err);
    if(NULL == fidbuf){
        SpxLogFmt2(c->log,SpxLogError,err,
                "alloc fid object is fail.",
                "fid:%s.",fid);
        goto r1;
    }

    yssc->fid = fidbuf;
    err = ydb_storage_dio_parser_fileid(c->log,fid,
            &(fidbuf->groupname),&(fidbuf->machineid),
            &(fidbuf->syncgroup),&(fidbuf->issinglefile),
            &(fidbuf->mpidx),&(fidbuf->p1),&(fidbuf->p2),
            &(fidbuf->tidx),&(fidbuf->fcreatetime),
            &(fidbuf->rand),&(fidbuf->begin),&(fidbuf->realsize),
            &(fidbuf->totalsize),&(fidbuf->ver),&(fidbuf->opver),
            &(fidbuf->lastmodifytime),&(fidbuf->hashcode),
            &(fidbuf->has_suffix),&(fidbuf->suffix));
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "parser fid:%s is fail.",
                fid);
        goto r1;
    }

    string_t fname = ydb_storage_dio_make_filename(c->log,
            fidbuf->issinglefile,c->mountpoints,fidbuf->mpidx,
            fidbuf->p1,fidbuf->p2,fidbuf->machineid,
            fidbuf->tidx,fidbuf->fcreatetime,fidbuf->rand,
            fidbuf->suffix,&err);
    if(NULL == fname){
        SpxLogFmt2(c->log,SpxLogError,err,
                "make fname by fid:%s  is fail.",
                fid);
        goto r1;
    }
    yssc->fname = fname;
    u32_t unit = 0;
    u64_t begin = 0;
    u64_t offset = 0;
    u64_t len = 0;

    //check localhost
    if(!SpxFileExist(fname)){
        SpxLogFmt1(c->log,SpxLogDebug,
                "fname:%s is exist and no do sync.",
                fname);
        is_no_file = true;
        err = ENOENT;
    }else {
        int fd = SpxWriteOpen(fname,false);
        if(0 >= fd){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open chunkfile:%s for fid:%s is fail.",
                    fname,fid);
            goto r1;
        }
        yssc->fd = fd;
        if(!fidbuf->issinglefile){
            unit = (int) fidbuf->begin / c->pagesize;
            begin = unit * c->pagesize;
            offset = fidbuf->begin - begin;
            len = offset + fidbuf->totalsize;

            char *ptr = SpxMmap(fd,begin,len);
            if(MAP_FAILED == ptr){
                err = errno;
                SpxLogFmt2(c->log,SpxLogError,err,
                        "mmap fid:%s to file:%s is fail.",
                        fid,fname);
                goto r1;
            }
            yssc->ptr = ptr;

            struct spx_msg *ioctx = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);
            if(NULL == ioctx){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "new metadata for fid:%s with fname:%s is fail.",
                        fid,fname);
                goto r1;
            }
            yssc->md = ioctx;

            if(0 != (err = spx_msg_pack_ubytes(ioctx,
                            ((ubyte_t *) (ptr+ offset)),
                            YDB_CHUNKFILE_MEMADATA_SIZE))){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "pack metadata for fid:%s with fname:%s is fail.",
                        fid,fname);
                goto r1;
            }
            spx_msg_seek(ioctx,0,SpxMsgSeekSet);

            bool_t io_isdelete = false;
            u32_t io_opver = 0;
            u32_t io_ver = 0;
            u64_t io_createtime = 0;
            u64_t io_lastmodifytime = 0;
            u64_t io_totalsize = 0;
            u64_t io_realsize = 0;
            string_t io_suffix = NULL;
            string_t io_hashcode = NULL;

            err = ydb_storage_dio_parser_metadata(c->log,ioctx,
                    &io_isdelete,&io_opver,
                    &io_ver,&io_createtime,
                    &io_lastmodifytime,&io_totalsize,&io_realsize,
                    &io_suffix,&io_hashcode);
            if(io_isdelete
                    || fidbuf->opver < io_opver){
                SpxLogFmt1(c->log,SpxLogInfo,
                        "the fid:%s is not last in the fname:%s,so not do sync.",
                        fid,fname);
                is_no_file = true;
                err = ENOENT;
            }

            if(NULL != io_suffix){
                SpxStringFree(io_suffix);
            }
            if(NULL != io_hashcode){
                SpxStringFree(io_hashcode);
            }
        }
    }

    yssc->sock  = spx_socket_new(&err);
    if(0 >= yssc->sock){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new socket to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    if(0 != (err = spx_socket_set(yssc->sock,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "set socket to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }
    if(0 != (err = spx_socket_connect_nb(yssc->sock,
                    s->host.ip,s->host.port,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "connect to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    yssc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
    if(NULL == yssc->request){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
    if(NULL == header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request's header to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }
    header->protocol = YDB_S2S_CSYNC_ADD;
    if(is_no_file){
        header->bodylen = SpxBoolTransportSize
            + sizeof(u64_t)
            + YDB_MACHINEID_LEN
            + spx_string_len(fid);
    } else {
        header->offset = SpxBoolTransportSize
            + sizeof(u64_t)
            + YDB_MACHINEID_LEN
            + spx_string_len(fid);
        if(fidbuf->issinglefile){
            header->bodylen = header->offset
                + fidbuf->realsize;
        } else {
            header->bodylen = header->offset
                + YDB_CHUNKFILE_MEMADATA_SIZE
                + fidbuf->realsize;
        }
    }
    header->err = is_no_file ? ENOENT : 0;
    header->is_keepalive = c->iskeepalive;//persistent connection
    yssc->request->header = header;

    struct spx_msg *body = NULL;
    if(is_no_file){ //no the file
        body = spx_msg_new(header->bodylen,&err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request's body to remote storage:%s,"
                    "host:%s:%d for csync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }
        spx_msg_pack_false(body);
        spx_msg_pack_u64(body,spx_zero(dt_binlog));
        spx_msg_pack_fixed_string(body,c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_string(body,fid);
    } else {
        body = spx_msg_new(header->offset,&err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request's body to remote storage:%s,"
                    "host:%s:%d for csync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }
        spx_msg_pack_true(body);
        spx_msg_pack_u64(body,spx_zero(dt_binlog));
        spx_msg_pack_fixed_string(body,c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_string(body,fid);

        yssc->request->is_sendfile = true;
        yssc->request->sendfile_fd = yssc->fd;
        if(fidbuf->issinglefile){
            yssc->request->sendfile_size = fidbuf->realsize;
        } else {
            yssc->request->sendfile_size = YDB_CHUNKFILE_MEMADATA_SIZE + fidbuf->realsize;
        }
        yssc->request->sendfile_begin = fidbuf->begin;
    }
    yssc->request->body = body;

    err = spx_write_context_nb(c->log,yssc->sock,yssc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "send request to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    if(!spx_socket_read_timeout(yssc->sock,c->timeout)){
        //timeout
        err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "response it timeout from  remote storage:%s,"
                "host:%s:%d for csync file:%s .",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    if(NULL == yssc->response){
        yssc->response = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == yssc->response){
            SpxLog2(c->log,SpxLogError,err,
                    "new response for query sync beginpoint is fail.");
            goto r1;
        }
    }

    yssc->response->header =  spx_read_header_nb(c->log,yssc->sock,&err);
    if(NULL == yssc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recving data for csync fid:%s,"
                "from remote storage:%s,ip:%s,port:%d is fail.",
                fid,s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = yssc->response->header->err;
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "request file:%s from remote storage:%s,host:%s:%d is fail.",
                fid,s->machineid,s->host.ip,s->host.port);
    }
r1:
    ydb_storage_sync_context_free(&yssc);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_sync_modify_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        struct spx_date *dt_binlog,
        string_t fid,
        string_t ofid
        ){/*{{{*/
    err_t err = 0;
    bool_t is_no_file = false;
    struct ydb_storage_sync_context *yssc = NULL;
    yssc = spx_alloc_alone(sizeof(*yssc),&err);
    if(NULL == yssc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "alloc csync remote object is fail.",
                "fid:%s.",fid);
        return err;
    }

    struct ydb_storage_fid *fidbuf =
        spx_alloc_alone(sizeof(struct ydb_storage_fid),&err);
    if(NULL == fidbuf){
        SpxLogFmt2(c->log,SpxLogError,err,
                "alloc fid object is fail.",
                "fid:%s.",fid);
        goto r1;
    }

    yssc->fid = fidbuf;
    err = ydb_storage_dio_parser_fileid(c->log,fid,
            &(fidbuf->groupname),&(fidbuf->machineid),
            &(fidbuf->syncgroup),&(fidbuf->issinglefile),
            &(fidbuf->mpidx),&(fidbuf->p1),&(fidbuf->p2),
            &(fidbuf->tidx),&(fidbuf->fcreatetime),
            &(fidbuf->rand),&(fidbuf->begin),&(fidbuf->realsize),
            &(fidbuf->totalsize),&(fidbuf->ver),&(fidbuf->opver),
            &(fidbuf->lastmodifytime),&(fidbuf->hashcode),
            &(fidbuf->has_suffix),&(fidbuf->suffix));
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "parser fid:%s is fail.",
                fid);
        goto r1;
    }

    string_t fname = ydb_storage_dio_make_filename(c->log,
            fidbuf->issinglefile,c->mountpoints,fidbuf->mpidx,
            fidbuf->p1,fidbuf->p2,fidbuf->machineid,
            fidbuf->tidx,fidbuf->fcreatetime,fidbuf->rand,
            fidbuf->suffix,&err);
    if(NULL == fname){
        SpxLogFmt2(c->log,SpxLogError,err,
                "make fname by fid:%s  is fail.",
                fid);
        goto r1;
    }
    yssc->fname = fname;
    u32_t unit = 0;
    u64_t begin = 0;
    u64_t offset = 0;
    u64_t len = 0;

    //check localhost
    if(!SpxFileExist(fname)){
        SpxLogFmt1(c->log,SpxLogDebug,
                "fname:%s is exist and no do csync.",
                fname);
        is_no_file = true;
        err = ENOENT;
    }else {
        int fd = SpxWriteOpen(fname,false);
        if(0 >= fd){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open chunkfile:%s for fid:%s is fail.",
                    fname,fid);
            goto r1;
        }
        yssc->fd = fd;
        if(!fidbuf->issinglefile){
            unit = (int) fidbuf->begin / c->pagesize;
            begin = unit * c->pagesize;
            offset = fidbuf->begin - begin;
            len = offset + fidbuf->totalsize;

            char *ptr = SpxMmap(fd,begin,len);
            if(MAP_FAILED == ptr){
                err = errno;
                SpxLogFmt2(c->log,SpxLogError,err,
                        "mmap fid:%s to file:%s is fail.",
                        fid,fname);
                goto r1;
            }
            yssc->ptr = ptr;

            struct spx_msg *ioctx = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);
            if(NULL == ioctx){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "new metadata for fid:%s with fname:%s is fail.",
                        fid,fname);
                goto r1;
            }
            yssc->md = ioctx;

            if(0 != (err = spx_msg_pack_ubytes(ioctx,
                            ((ubyte_t *) (ptr+ offset)),
                            YDB_CHUNKFILE_MEMADATA_SIZE))){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "pack metadata for fid:%s with fname:%s is fail.",
                        fid,fname);
                goto r1;
            }
            spx_msg_seek(ioctx,0,SpxMsgSeekSet);

            bool_t io_isdelete = false;
            u32_t io_opver = 0;
            u32_t io_ver = 0;
            u64_t io_createtime = 0;
            u64_t io_lastmodifytime = 0;
            u64_t io_totalsize = 0;
            u64_t io_realsize = 0;
            string_t io_suffix = NULL;
            string_t io_hashcode = NULL;

            err = ydb_storage_dio_parser_metadata(c->log,ioctx,
                    &io_isdelete,&io_opver,
                    &io_ver,&io_createtime,
                    &io_lastmodifytime,&io_totalsize,&io_realsize,
                    &io_suffix,&io_hashcode);
            if(io_isdelete
                   || fidbuf->opver < io_opver){
                SpxLogFmt1(c->log,SpxLogInfo,
                        "the fid:%s is not last in the fname:%s,so not do csync.",
                        fid,fname);
                is_no_file = true;
                err = ENOENT;
            }

            if(NULL != io_suffix){
                SpxStringFree(io_suffix);
            }
            if(NULL != io_hashcode){
                SpxStringFree(io_hashcode);
            }
        }
    }

    yssc->sock  = spx_socket_new(&err);
    if(0 >= yssc->sock){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new socket to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    if(0 != (err = spx_socket_set(yssc->sock,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "set socket to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }
    if(0 != (err = spx_socket_connect_nb(yssc->sock,
                    s->host.ip,s->host.port,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "connect to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    yssc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
    if(NULL == yssc->request){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
    if(NULL == header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request's header to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }
    header->protocol = YDB_S2S_CSYNC_MODIFY;
    if(is_no_file){
        header->bodylen = SpxBoolTransportSize
            + sizeof(u64_t)
            + 2 * sizeof(u32_t)
            + spx_string_len(ofid)
            + spx_string_len(fid)
            + YDB_MACHINEID_LEN;
    } else {
        header->offset = SpxBoolTransportSize
            + sizeof(u64_t)
            + 2 * sizeof(u32_t)
            + spx_string_len(ofid)
            + spx_string_len(fid)
            + YDB_MACHINEID_LEN;
        if(fidbuf->issinglefile){
            header->bodylen = header->offset + fidbuf->realsize;
        } else {
            header->bodylen = header->offset + YDB_CHUNKFILE_MEMADATA_SIZE + fidbuf->realsize;
        }
    }
    header->err = is_no_file ? ENOENT : 0;
    header->is_keepalive = c->iskeepalive;//persistent connection
    yssc->request->header = header;

    struct spx_msg *body = NULL;
    if(is_no_file){ //no the file
        body = spx_msg_new(header->bodylen,&err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request's body to remote storage:%s,"
                    "host:%s:%d for csync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }
        spx_msg_pack_false(body);
        spx_msg_pack_u64(body,spx_zero(dt_binlog));
        spx_msg_pack_u32(body,spx_string_len(ofid));
        spx_msg_pack_u32(body,spx_string_len(fid));
        spx_msg_pack_fixed_string(body,c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_string(body,ofid);
        spx_msg_pack_string(body,fid);
    } else {
        body = spx_msg_new(header->offset,&err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request's body to remote storage:%s,"
                    "host:%s:%d for csync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }
        spx_msg_pack_true(body);
        spx_msg_pack_u64(body,spx_zero(dt_binlog));
        spx_msg_pack_u32(body,spx_string_len(ofid));
        spx_msg_pack_u32(body,spx_string_len(fid));
        spx_msg_pack_fixed_string(body,c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_string(body,ofid);
        spx_msg_pack_string(body,fid);

        yssc->request->is_sendfile = true;
        yssc->request->sendfile_fd = yssc->fd;
        if(fidbuf->issinglefile){
            yssc->request->sendfile_size = fidbuf->realsize;
        } else {
            yssc->request->sendfile_size = YDB_CHUNKFILE_MEMADATA_SIZE + fidbuf->realsize;
        }
        yssc->request->sendfile_begin = fidbuf->begin;
    }
    yssc->request->body = body;

    err = spx_write_context_nb(c->log,yssc->sock,yssc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "send request to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    if(!spx_socket_read_timeout(yssc->sock,c->timeout)){
        //timeout
        err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "response it timeout from  remote storage:%s,"
                "host:%s:%d for csync file:%s .",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    if(NULL == yssc->response){
        yssc->response = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == yssc->response){
            SpxLog2(c->log,SpxLogError,err,
                    "new response for query sync beginpoint is fail.");
            goto r1;
        }
    }

    yssc->response->header =  spx_read_header_nb(c->log,yssc->sock,&err);
    if(NULL == yssc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recving data for csync fid:%s,"
                "from remote storage:%s,ip:%s,port:%d is fail.",
                fid,s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = yssc->response->header->err;
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "request file:%s from remote storage:%s,host:%s:%d is fail.",
                fid,s->machineid,s->host.ip,s->host.port);
    }
r1:
    ydb_storage_sync_context_free(&yssc);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_sync_delete_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        struct spx_date *dt_binlog,
        string_t ofid
        ){/*{{{*/
    err_t err = 0;
    struct ydb_storage_sync_context *yssc = NULL;
    yssc = spx_alloc_alone(sizeof(*yssc),&err);
    if(NULL == yssc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "alloc csync remote object is fail.",
                "fid:%s.",ofid);
        return err;
    }

    yssc->sock  = spx_socket_new(&err);
    if(0 >= yssc->sock){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new socket to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                ofid);
        goto r1;
    }

    if(0 != (err = spx_socket_set(yssc->sock,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "set socket to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                ofid);
        goto r1;
    }
    if(0 != (err = spx_socket_connect_nb(yssc->sock,
                    s->host.ip,s->host.port,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "connect to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                ofid);
        goto r1;
    }

    yssc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
    if(NULL == yssc->request){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                ofid);
        goto r1;
    }

    struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
    if(NULL == header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request's header to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                ofid);
        goto r1;
    }

    header->protocol = YDB_S2S_CSYNC_DELETE;
    header->bodylen = sizeof(u64_t)
        +  YDB_MACHINEID_LEN
        +spx_string_len(ofid);
    header->is_keepalive = c->iskeepalive;//persistent connection
    yssc->request->header = header;

    struct spx_msg *body = NULL;
    body = spx_msg_new(header->bodylen,&err);
    if(NULL == body){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request's body to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                ofid);
        goto r1;
    }
    spx_msg_pack_u64(body,spx_zero(dt_binlog));
    spx_msg_pack_fixed_string(body,c->machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_string(body,ofid);
    yssc->request->body = body;

    err = spx_write_context_nb(c->log,yssc->sock,yssc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "send request to remote storage:%s,"
                "host:%s:%d for csync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                ofid);
        goto r1;
    }

    if(!spx_socket_read_timeout(yssc->sock,c->timeout)){
        //timeout
        err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "response it timeout from  remote storage:%s,"
                "host:%s:%d for csync file:%s .",
                s->machineid,s->host.ip,s->host.port,
                ofid);
        goto r1;
    }

    if(NULL == yssc->response){
        yssc->response = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == yssc->response){
            SpxLog2(c->log,SpxLogError,err,
                    "new response for query sync beginpoint is fail.");
            goto r1;
        }
    }

    yssc->response->header =  spx_read_header_nb(c->log,yssc->sock,&err);
    if(NULL == yssc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recving data for csync fid:%s,"
                "from remote storage:%s,ip:%s,port:%d is fail.",
                ofid,s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = yssc->response->header->err;
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "request file:%s from remote storage:%s,host:%s:%d is fail.",
                ofid,s->machineid,s->host.ip,s->host.port);
    }
r1:
    ydb_storage_sync_context_free(&yssc);
    return err;
}/*}}}*/

void ydb_storage_sync_context_free(
        struct ydb_storage_sync_context **yssc
        ){/*{{{*/
    if(NULL == yssc || NULL == *yssc){
        return;
    }
    if(NULL != (*yssc)->fid){
        if(NULL != (*yssc)->fid->syncgroup){
            SpxStringFree((*yssc)->fid->syncgroup);
        }
        if(NULL != (*yssc)->fid->suffix){
            SpxStringFree((*yssc)->fid->suffix);
        }
        if(NULL != (*yssc)->fid->machineid){
            SpxStringFree((*yssc)->fid->machineid);
        }
        if(NULL != (*yssc)->fid->groupname){
            SpxStringFree((*yssc)->fid->groupname);
        }
        if(NULL != (*yssc)->fid->hashcode){
            SpxStringFree((*yssc)->fid->hashcode);
        }
        SpxFree((*yssc)->fid);
    }
    if(NULL != (*yssc)->request){
        if(NULL != ((*yssc)->request->body)){
            SpxMsgFree((*yssc)->request->body);
        }
        if(NULL != ((*yssc)->request->header)){
            SpxFree((*yssc)->request->header);
        }
        SpxFree((*yssc)->request);
    }
    if(NULL != (*yssc)->response){
        if(NULL != ((*yssc)->response->body)){
            SpxMsgFree((*yssc)->response->body);
        }
        if(NULL != ((*yssc)->response->header)){
            SpxFree((*yssc)->response->header);
        }
        SpxFree((*yssc)->response);
    }
    if(NULL != (*yssc)->md){
        SpxMsgFree((*yssc)->md);
    }
    if(NULL != (*yssc)->fname){
        SpxStringFree((*yssc)->fname);
    }
    if(NULL != (*yssc)->smd){
        SpxStringFree((*yssc)->smd);
    }
    if(NULL != (*yssc)->ptr){
        munmap((*yssc)->ptr,(*yssc)->len);
        (*yssc)->ptr = NULL;
    }
    if(0 != (*yssc)->fd){
        SpxClose((*yssc)->fd);
    }
    if(0 != (*yssc)->sock){
        SpxClose((*yssc)->sock);
    }
    SpxFree(*yssc);
    return;
}/*}}}*/

spx_private err_t ydb_storage_sync_after(
        int protocol,
        struct ydb_storage_dio_context *dc
        ){/*{{{*/
    err_t err = 0;
    struct spx_job_context *jc = dc->jc;

    struct spx_msg_header *h = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*h),&err);
    if(NULL == h){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail.");
        return err;
    }
    jc->writer_header = h;
    h->protocol = protocol;
    h->bodylen = 0;
    h->version = YDB_VERSION;
    h->offset = 0;
    jc->is_sendfile = false;
    return err;
}/*}}}*/

spx_private err_t ydb_storage_sync_log(string_t machineid,
        time_t log_time, char op,string_t fid,string_t ofid
        ){/*{{{*/
    err_t err = 0;
    struct ydb_storage_remote *s = NULL;
    s = spx_map_get(g_ydb_storage_remote,machineid,
            spx_string_rlen(machineid),NULL);
    if(NULL == s ){
        err = 0 == err ? ENOENT : err;
        return err;
    }
    ydb_storage_synclog_write(&(s->synclog),log_time,op,fid,ofid);
    return err;
}/*}}}*/

err_t ydb_storage_sync_upload(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc
        ){/*{{{*/
    err_t err = 0;
    struct spx_msg_header *rqh = dc->jc->reader_header;
    struct spx_job_context *jc = dc->jc;

    bool_t is_has_file = false;
    is_has_file = spx_msg_unpack_bool(jc->reader_body_ctx);
    dc->createtime = spx_msg_unpack_u64(jc->reader_body_ctx);
    dc->sync_machineid = spx_msg_unpack_string(jc->reader_body_ctx,
            YDB_MACHINEID_LEN,&err);
    if(is_has_file) {
        dc->rfid = spx_msg_unpack_string(jc->reader_body_ctx,
                rqh->offset - SpxBoolTransportSize - sizeof(u64_t) - YDB_MACHINEID_LEN,
                &err);
    } else {
        dc->rfid = spx_msg_unpack_string(jc->reader_body_ctx,
                rqh->bodylen - SpxBoolTransportSize - sizeof(u64_t) - YDB_MACHINEID_LEN,
                &err);

    }
    if(!is_has_file){
        ydb_storage_sync_log(dc->sync_machineid,
                dc->createtime,YDB_STORAGE_LOG_UPLOAD,dc->rfid,NULL);
        goto r1;
    }

    YdbStorageParserFileidWithDIOContext(dc->log,dc->rfid,dc);
    if(dc->issinglefile) {
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_sync_do_upload_for_singlefile,dc);
    } else {
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_sync_do_upload_for_chunkfile,dc);
    }
    ev_async_start(loop,&(dc->async));
    ev_async_send(loop,&(dc->async));
    return err;
r1:
    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(jc->log,SpxLogError,err,\
                "dispatch network module is fail,"
                "and push jcontext to pool force.");
        spx_job_pool_push(g_spx_job_pool,jc);
        return err;
    }
    jc->writer_header->protocol = YDB_S2S_CSYNC_ADD;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext_err =
        spx_get_thread(g_spx_network_module,i);
    jc->tc = threadcontext_err;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return err;

}/*}}}*/

spx_private void ydb_storage_sync_do_upload_for_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents
        ){/*{{{*/
    ev_async_stop(loop,w);
    err_t  err = 0;
    ev_async_stop(loop,w);
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_job_context *jc = dc->jc;
    struct spx_task_context *tc = dc->tc;
    struct ydb_storage_configurtion *c = dc->c;
    struct ydb_storage_storefile *sf = dc->storefile;

    dc->filename = ydb_storage_dio_make_filename(dc->log,
            dc->issinglefile,c->mountpoints,dc->mp_idx,dc->p1,dc->p2,
            dc->machineid,dc->tidx,dc->file_createtime,
            dc->rand,dc->suffix,&err);

    u32_t unit = (int) dc->begin / c->pagesize;
    u64_t begin = unit * c->pagesize;
    u64_t offset = dc->begin - begin;
    u64_t len = offset + dc->totalsize;
    bool_t is_exist = SpxFileExist(dc->filename);

    sf->singlefile.fd = SpxWriteOpen(dc->filename,false);
    if(0 >= sf->singlefile.fd){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "open file:%s for sync is fail.",
                dc->filename);
        goto r1;
    }

    if(!is_exist){
        if(0 != (err = ftruncate(sf->singlefile.fd,c->chunksize))){
            SpxLogFmt2(c->log,SpxLogError,err,\
                    "truncate chunkfile:%s to size:%lld is fail.",
                    sf->singlefile.filename,c->chunksize);
            goto r1;
        }
    }

    sf->singlefile.mptr = SpxMmap(sf->singlefile.fd,begin,len);
    if(NULL == sf->singlefile.mptr){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "mmap file:%s is fail.",
                dc->filename);
        goto r1;
    }

    if(jc->is_lazy_recv){
        spx_lazy_mmap_nb(dc->log,sf->singlefile.mptr,
                jc->fd,jc->reader_header->bodylen - jc->reader_header->offset,
                offset);
    }  else {
        memcpy(sf->singlefile.mptr + offset,
                jc->reader_body_ctx->buf + jc->reader_header->offset,
                jc->reader_header->bodylen - jc->reader_header->offset);
    }

    if(0 != (err = ydb_storage_sync_log(dc->sync_machineid,
            dc->createtime,YDB_STORAGE_LOG_UPLOAD,dc->rfid,NULL))){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "log sync info:%s from remote storage:%s is fail.",
                dc->rfid,dc->sync_machineid);
        goto r1;
    }

    if(0 != (err = ydb_storage_sync_after(YDB_S2S_CSYNC_ADD,dc))){
        SpxLog2(dc->log,SpxLogError,err,\
                "make the response for uploading is fail.");
        goto r1;
    }
    goto r2;
r1:
    if(NULL == jc->writer_header) {
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&(jc->err));
        if(NULL == jc->writer_header){
            SpxLog2(jc->log,SpxLogError,jc->err,\
                    "new response header is fail."
                    "no notify client and push jc force.");
            spx_task_pool_push(g_spx_task_pool,tc);
            ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
            spx_job_pool_push(g_spx_job_pool,jc);
            return ;
        }
    }
    jc->writer_header->protocol = YDB_S2S_CSYNC_ADD;;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
r2:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

spx_private void ydb_storage_sync_do_upload_for_singlefile(
        struct ev_loop *loop,ev_async *w,int revents
        ){/*{{{*/
    ev_async_stop(loop,w);
    err_t  err = 0;
    ev_async_stop(loop,w);
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = dc->c;
    struct ydb_storage_storefile *sf = dc->storefile;
    struct spx_task_context *tc = dc->tc;

    dc->filename = ydb_storage_dio_make_filename(dc->log,
            dc->issinglefile,c->mountpoints,dc->mp_idx,dc->p1,dc->p2,
            dc->machineid,dc->tidx,dc->file_createtime,
            dc->rand,dc->suffix,&err);

    if(!SpxFileExist(dc->filename)){
        sf->singlefile.fd = SpxWriteOpen(dc->filename,true);
        if(0 >= sf->singlefile.fd){
            err = errno;
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open file:%s is fail.",
                    dc->filename);
            goto r1;
        }
        if(0 != (err = ftruncate(sf->singlefile.fd,dc->totalsize))){
            SpxLogFmt2(c->log,SpxLogError,err,\
                    "truncate chunkfile:%s to size:%lld is fail.",
                    sf->singlefile.filename,c->chunksize);
            goto r1;
        }

        sf->singlefile.mptr = SpxMmap(sf->singlefile.fd,0,dc->totalsize);
        if(NULL == sf->singlefile.mptr){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "mmap file:%s is fail.",
                    dc->filename);
            goto r1;
        }

        if(jc->is_lazy_recv){
            spx_lazy_mmap_nb(dc->log,sf->singlefile.mptr,
                    jc->fd,dc->totalsize,0);
        }  else {
            memcpy(sf->singlefile.mptr,
                    jc->reader_body_ctx->buf + jc->reader_header->offset,
                    jc->reader_header->bodylen - jc->reader_header->offset);
        }
    }

    if(0 != (err = ydb_storage_sync_log(dc->sync_machineid,
            dc->createtime,YDB_STORAGE_LOG_UPLOAD,dc->rfid,NULL))){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "log sync info:%s from remote storage:%s is fail.",
                dc->rfid,dc->sync_machineid);
        goto r1;
    }

    if(0 != (err = ydb_storage_sync_after(YDB_S2S_CSYNC_ADD,dc))){
        SpxLog2(dc->log,SpxLogError,err,\
                "make the response for uploading is fail.");
        goto r1;
    }
    goto r2;
r1:
    if(NULL == jc->writer_header) {
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&(jc->err));
        if(NULL == jc->writer_header){
            SpxLog2(jc->log,SpxLogError,jc->err,\
                    "new response header is fail."
                    "no notify client and push jc force.");
            spx_task_pool_push(g_spx_task_pool,tc);
            ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
            spx_job_pool_push(g_spx_job_pool,jc);
            return ;
        }
    }
    jc->writer_header->protocol = YDB_S2S_CSYNC_ADD;;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
r2:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

err_t ydb_storage_sync_delete(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc
        ){/*{{{*/
    err_t err = 0;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    struct spx_msg_header *repheader = jc->reader_header;

    dc->createtime = spx_msg_unpack_u64(jc->reader_body_ctx);
    dc->sync_machineid = spx_msg_unpack_string(jc->reader_body_ctx,
            YDB_MACHINEID_LEN,&err);
    dc->rfid = spx_msg_unpack_string(jc->reader_body_ctx,
            repheader->bodylen - sizeof(u64_t) - YDB_MACHINEID_LEN,
            &err);

    YdbStorageParserFileidWithDIOContext(c->log,dc->rfid,dc);
    dc->filename = ydb_storage_dio_make_filename(dc->log,dc->issinglefile,
            c->mountpoints,
            dc->mp_idx,
            dc->p1,dc->p2,
            dc->machineid,dc->tidx,dc->file_createtime,
            dc->rand,dc->suffix,&err);
    if(NULL == dc->filename){
        SpxLog2(dc->log,SpxLogError,err,\
                "make filename is fail.");
        goto r1;
    }

    if(!SpxFileExist(dc->filename)) {
        SpxLogFmt1(dc->log,SpxLogWarn,\
                "deleting-file:%s is not exist.",
                dc->filename);

        if(0 != (err = ydb_storage_sync_log(dc->sync_machineid,
                        dc->createtime,YDB_STORAGE_LOG_DELETE,dc->rfid,NULL))){
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "log sync info:%s from remote storage:%s is fail.",
                    dc->rfid,dc->sync_machineid);
            goto r1;
        }
        goto r1;
    }

    if(dc->issinglefile){
        if(0 != remove(dc->filename)){
            err = errno;
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "delete file :%s is fail.",
                    dc->filename);
        }
        if(0 != (err = ydb_storage_sync_log(dc->sync_machineid,
                        dc->createtime,YDB_STORAGE_LOG_DELETE,dc->rfid,NULL))){
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "log sync info:%s from remote storage:%s is fail.",
                    dc->rfid,dc->sync_machineid);
            goto r1;
        }
        goto r1;
    } else {
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_sync_delete_form_chunkfile,dc);
        ev_async_start(loop,&(dc->async));
        ev_async_send(loop,&(dc->async));
    }
    return err;
r1:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail."
                "no notify client and push jc force.");
        spx_job_pool_push(g_spx_job_pool,jc);
        return err;
    }
    jc->writer_header->protocol = YDB_S2S_CSYNC_DELETE;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return 0;
}/*}}}*/

spx_private void ydb_storage_sync_delete_form_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents
        ){/*{{{*/
    ev_async_stop(loop,w);
    err_t err = 0;
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    if(0 != (err =  ydb_storage_dio_delete_context_from_chunkfile(
                    c,dc->filename,dc->begin,dc->totalsize,
                    dc->opver,dc->ver,dc->lastmodifytime,
                    dc->realsize,spx_now()))){
        if(ENOENT != err) {
            SpxLog2(dc->log,SpxLogError,err,
                    "delete context form chunkfile is fail.");
            goto r1;
        }
        err = 0;
        SpxErrReset;
    }
    if(0 != (err = ydb_storage_sync_log(dc->sync_machineid,
                    dc->createtime,YDB_STORAGE_LOG_DELETE,dc->rfid,NULL))){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "log sync info:%s from remote storage:%s is fail.",
                dc->rfid,dc->sync_machineid);
        goto r1;
    }

    if(0 != (err = ydb_storage_sync_after(YDB_S2S_CSYNC_DELETE,dc))){
        SpxLogFmt2(dc->log,SpxLogError,err,
                "delete file:%s is fail.",dc->rfid);
        goto r1;
    }
    goto r2;
r1:
    if(NULL == jc->writer_header) {
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
        if(NULL == jc->writer_header){
            SpxLog2(dc->log,SpxLogError,err,\
                    "new response header is fail."
                    "no notify client and push jc force.");
            spx_task_pool_push(g_spx_task_pool,tc);
            ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
            spx_job_pool_push(g_spx_job_pool,jc);
            return;
        }
    }
    jc->writer_header->protocol = jc->reader_header->protocol;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;
r2:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

err_t ydb_storage_sync_modify(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc
        ){/*{{{*/
    err_t err = 0;
    struct ydb_storage_configurtion *c = dc->jc->config;
    struct spx_job_context *jc = dc->jc;
    bool_t is_has_file = false;
    u32_t ofid_len = 0;
    u32_t fid_len = 0;
    is_has_file = spx_msg_unpack_bool(jc->reader_body_ctx);
    dc->createtime = spx_msg_unpack_u64(jc->reader_body_ctx);
    ofid_len = spx_msg_unpack_u32(jc->reader_body_ctx);
    fid_len = spx_msg_unpack_u32(jc->reader_body_ctx);
    dc->sync_machineid = spx_msg_unpack_string(jc->reader_body_ctx,
            YDB_MACHINEID_LEN,&err);
    dc->rfid = spx_msg_unpack_string(jc->reader_body_ctx,
            ofid_len,&err);
    dc->fid = spx_msg_unpack_string(jc->reader_body_ctx,
            fid_len,&err);
    if(!is_has_file){
        if(0 != (err = ydb_storage_sync_log(dc->sync_machineid,
                        dc->createtime,YDB_STORAGE_LOG_MODIFY,dc->rfid,dc->fid))){
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "log sync info:%s from remote storage:%s is fail.",
                    dc->rfid,dc->sync_machineid);
        }
        goto r1;
    }

    YdbStorageParserFileidWithDIOContext(c->log,dc->fid,dc);
    if(dc->issinglefile){
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_sync_do_modify_to_singlefile,dc);
    } else {
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_sync_do_modify_to_chunkfile,dc);
    }
    ev_async_start(loop,&(dc->async));
    ev_async_send(loop,&(dc->async));
    return err;
r1:
    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(jc->log,SpxLogError,err,\
                "dispatch network module is fail,"
                "and push jcontext to pool force.");
        spx_job_pool_push(g_spx_job_pool,jc);
        return err;
    }
    jc->writer_header->protocol = YDB_S2S_CSYNC_MODIFY;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext_err =
        spx_get_thread(g_spx_network_module,i);
    jc->tc = threadcontext_err;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return err;

}/*}}}*/

spx_private void ydb_storage_sync_do_modify_to_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents
        ){/*{{{*/
    ev_async_stop(loop,w);
    err_t  err = 0;
    ev_async_stop(loop,w);
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = dc->c;
    struct ydb_storage_storefile *sf = dc->storefile;
    struct spx_task_context *tc = dc->tc;

    dc->filename = ydb_storage_dio_make_filename(dc->log,
            dc->issinglefile,c->mountpoints,dc->mp_idx,dc->p1,dc->p2,
            dc->machineid,dc->tidx,dc->file_createtime,
            dc->rand,dc->suffix,&err);

    u32_t unit = (int) dc->begin / c->pagesize;
    u64_t begin = unit * c->pagesize;
    u64_t offset = dc->begin - begin;
    u64_t len = offset + dc->totalsize;
    bool_t is_exist = SpxFileExist(dc->filename);
    sf->singlefile.fd = SpxWriteOpen(dc->filename,false);
    if(0 >= sf->singlefile.fd){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "open file:%s iafail.",
                dc->filename);
        goto r1;
    }
    if(!is_exist){
        if(0 != (err = ftruncate(sf->singlefile.fd,c->chunksize))){
            SpxLogFmt2(c->log,SpxLogError,err,\
                    "truncate chunkfile:%s to size:%lld is fail.",
                    sf->singlefile.filename,c->chunksize);
            goto r1;
        }
    }
    sf->singlefile.mptr = SpxMmap(sf->singlefile.fd,begin,len);
    if(NULL == sf->singlefile.mptr){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "mmap file:%s iafail.",
                dc->filename);
        goto r1;
    }


    if(jc->is_lazy_recv){
        spx_lazy_mmap_nb(dc->log,sf->singlefile.mptr,
                jc->fd,jc->reader_header->bodylen - jc->reader_header->offset,
                offset);
    }  else {
        memcpy(sf->singlefile.mptr + offset,
                jc->reader_body_ctx->buf + jc->reader_header->offset,
                jc->reader_header->bodylen - jc->reader_header->offset);
    }

        if(0 != (err = ydb_storage_sync_log(dc->sync_machineid,
                        dc->createtime,YDB_STORAGE_LOG_MODIFY,dc->rfid,dc->fid))){
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "log sync info:%s from remote storage:%s is fail.",
                    dc->rfid,dc->sync_machineid);
            goto r1;
        }


    if(0 != (err = ydb_storage_sync_after(YDB_S2S_CSYNC_MODIFY,dc))){
        SpxLog2(dc->log,SpxLogError,err,\
                "make the response for uploading is fail.");
        goto r1;
    }
    goto r2;
r1:
    if(NULL == jc->writer_header) {
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&(jc->err));
        if(NULL == jc->writer_header){
            SpxLog2(jc->log,SpxLogError,jc->err,\
                    "new response header is fail."
                    "no notify client and push jc force.");
            spx_task_pool_push(g_spx_task_pool,tc);
            ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
            spx_job_pool_push(g_spx_job_pool,jc);
            return ;
        }
    }
    jc->writer_header->protocol = YDB_S2S_CSYNC_MODIFY;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
r2:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

spx_private void ydb_storage_sync_do_modify_to_singlefile(
        struct ev_loop *loop,ev_async *w,int revents
        ){/*{{{*/
    ev_async_stop(loop,w);
    err_t  err = 0;
    ev_async_stop(loop,w);
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_job_context *jc = dc->jc;
    struct spx_task_context *tc = dc->tc;
    struct ydb_storage_configurtion *c = dc->c;
    struct ydb_storage_storefile *sf = dc->storefile;

    dc->filename = ydb_storage_dio_make_filename(dc->log,
            dc->issinglefile,c->mountpoints,dc->mp_idx,dc->p1,dc->p2,
            dc->machineid,dc->tidx,dc->file_createtime,
            dc->rand,dc->suffix,&err);

    if(!SpxFileExist(dc->filename)){
        sf->singlefile.fd = SpxWriteOpen(dc->filename,true);
        if(0 >= sf->singlefile.fd){
            err = errno;
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open file:%s is fail.",
                    dc->filename);
            goto r1;
        }
        if(0 != (err = ftruncate(sf->singlefile.fd,dc->totalsize))){
            SpxLogFmt2(c->log,SpxLogError,err,\
                    "truncate chunkfile:%s to size:%lld is fail.",
                    sf->singlefile.filename,c->chunksize);
            goto r1;
        }

        sf->singlefile.mptr = SpxMmap(sf->singlefile.fd,0,dc->totalsize);
        if(NULL == sf->singlefile.mptr){
            err = errno;
            SpxLogFmt2(c->log,SpxLogError,err,
                    "mmap file:%s is fail.",
                    dc->filename);
            goto r1;
        }

        if(jc->is_lazy_recv){
            spx_lazy_mmap_nb(dc->log,sf->singlefile.mptr,
                    jc->fd,dc->totalsize,0);
        }  else {
            memcpy(sf->singlefile.mptr,
                    jc->reader_body_ctx->buf + jc->reader_header->offset,
                    jc->reader_header->bodylen - jc->reader_header->offset);
        }
    }

    if(0 != (err = spx_modify_filetime(
                    dc->filename,
                    dc->file_createtime))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "modify file:%s creatime is fail.",
                dc->filename);
        goto r1;
    }

    if(0 != (err = ydb_storage_sync_log(dc->sync_machineid,
                    dc->createtime,YDB_STORAGE_LOG_MODIFY,dc->rfid,dc->fid))){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "log sync info:%s from remote storage:%s is fail.",
                dc->rfid,dc->sync_machineid);
        goto r1;
    }

    if(0 != (err = ydb_storage_sync_after(YDB_S2S_CSYNC_MODIFY,dc))){
        SpxLog2(dc->log,SpxLogError,err,\
                "make the response for uploading is fail.");
        goto r1;
    }
    goto r2;
r1:
    if(NULL == jc->writer_header) {
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&(jc->err));
        if(NULL == jc->writer_header){
            SpxLog2(jc->log,SpxLogError,jc->err,\
                    "new response header is fail."
                    "no notify client and push jc force.");
            spx_task_pool_push(g_spx_task_pool,tc);
            ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
            spx_job_pool_push(g_spx_job_pool,jc);
            return ;
        }
    }
    jc->writer_header->protocol = YDB_S2S_CSYNC_MODIFY;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
r2:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

