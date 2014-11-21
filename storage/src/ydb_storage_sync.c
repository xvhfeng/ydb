
#include <stdlib.h>
#include <stdio.h>
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


#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_sync.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_synclog.h"
#include "ydb_storage_dio_context.h"

struct spx_threadpool *g_sync_threadpool = NULL;

struct spx_map *g_ydb_storage_remote = NULL;
spx_private err_t ydb_storage_sync_remote_map_vfree(void **arg);
spx_private void *ydb_storage_sync_query_storages(void *arg);
spx_private err_t ydb_storage_sync_query_storage(
        struct ydb_storage_configurtion *c,
        struct ydb_tracker *t, u32_t timeout );

spx_private void ydb_storage_sync_do_csync(
        void *arg);

spx_private err_t ydb_storage_sync_query_csync_beginpoint(
        struct ydb_storage_remote *s,
        int *year,int *month,int *day,u64_t *offset);

spx_private string_t ydb_storage_sync_make_marklog_filename(
        struct ydb_storage_configurtion *c,
        err_t *err);

spx_private err_t ydb_storage_sync_delete_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        int protocol,
        string_t ofid);




struct spx_periodic *ydb_storage_sync_query_storages_init(
        struct ydb_storage_configurtion *c,err_t *err){/*{{{*/
    return spx_periodic_exec_and_async_run(c->log,
            c->timeout,0,
            ydb_storage_sync_query_storages,
            (void *) c,
            c->stacksize,
            err);
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

spx_private void *ydb_storage_sync_query_storages(void *arg){/*{{{*/
    err_t err = 0;
    if(NULL == arg){
        return NULL;
    }

    SpxTypeConvert2(struct ydb_storage_configurtion,c,arg);
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
            return NULL;
        }
    }

    struct spx_vector_iter *iter = spx_vector_iter_new(c->trackers,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the trackers iter is fail.");
        return NULL;
    }

    struct ydb_tracker *t = NULL;
    while(NULL != (t = spx_vector_iter_next(iter))){
        ydb_storage_sync_query_storage(c,t,c->timeout);
    }
    spx_vector_iter_free(&iter);


    struct spx_map_iter *iter_storage = spx_map_iter_new(g_ydb_storage_remote,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the trackers iter is fail.");
        return NULL;
    }

    struct spx_map_node *n = NULL;
    while(NULL != (n = spx_map_iter_next(iter_storage,&err))){
        SpxTypeConvert2(struct ydb_storage_remote,s,n->v);
        if(YDB_STORAGE_RUNNING == s->runtime_state
                || YDB_STORAGE_DSYNCED == s->runtime_state
                || YDB_STORAGE_CSYNCING == s->runtime_state){
            if(s->is_doing){
                spx_threadpool_execute(g_sync_threadpool,ydb_storage_sync_do_csync,s);
            }
        }
    }
    spx_vector_iter_free(&iter);


    return NULL;
}/*}}}*/

spx_private err_t ydb_storage_sync_query_storage(
        struct ydb_storage_configurtion *c,
        struct ydb_tracker *t, u32_t timeout ){/*{{{*/
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
            SpxLogFmt2(c->log,SpxLogError,err,
                    "set socket to tracker %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }
        if(0 != (err = spx_socket_connect_nb(ystc->fd,
                        t->host.ip,t->host.port,timeout))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "connect to tracker %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }
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
        header->is_keepalive = true;//persistent connection
        ystc->request->header = header;
    }


    if(NULL == ystc->request->body) {
        struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,&err);
        if(NULL == body){
            SpxLog2(c->log,SpxLogError,err,
                    "new body of request for query sync storage is fail.");
            goto r1;
        }
        spx_msg_pack_fixed_string(body,
                c->groupname,YDB_GROUPNAME_LEN);
        spx_msg_pack_fixed_string(body,
                c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_fixed_string(body,
                c->syncgroup,YDB_SYNCGROUP_LEN);
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
        SpxLogFmt1(c->log,SpxLogError,
                "read from tracker %s:%d for query sync storages is fail."
                "and close the fd force.",
                t->host.ip,t->host.port);
        SpxClose(ystc->fd);
        goto r1;
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
    u32_t i = 0;
    for( ; i < count ; i++){
        string_t machineid = spx_msg_unpack_string(body,YDB_MACHINEID_LEN,&err);
        string_t ip = spx_msg_unpack_string(body,SpxIpv4Size,&err);
        i32_t port = spx_msg_unpack_i32(body);
        u32_t state = spx_msg_unpack_u32(body);
        struct ydb_storage_remote *remote_storage = NULL;
        err = spx_map_get(g_ydb_storage_remote,machineid,spx_string_len(machineid),
                (void **) &remote_storage,NULL);
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
            remote_storage->update_timespan = spx_now();
        } else {
            remote_storage = (struct ydb_storage_remote *)
                spx_alloc_alone(sizeof(struct ydb_storage_remote),&err);
            if(NULL == remote_storage){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "new sync remote storage:%s get from tracker:%s:%d is fail."
                        machineid,t->host.ip,t->host.port);
                continue;
            }
            remote_storage->c = c;
            remote_storage->machineid = machineid;
            remote_storage->runtime_state = state;
            remote_storage->host.ip = ip;
            remote_storage->host.port = port;
            if(0 != (err = spx_map_insert(g_ydb_storage_remote,
                            machineid,spx_string_len(machineid),
                            remote_storage,sizeof(remote_storage)))){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "new sync remote storage:%s get from tracker:%s:%d is fail."
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
        SpxFree(ystc->response);
    }
    return err;
}/*}}}*/

spx_private void ydb_storage_sync_do_csync(
        void *arg
        ){
    err_t err =0;
    SpxTypeConvert2(struct ydb_storage_remote,s,arg);
    if(!SpxAtomicVIsCas(s->is_doing,false,true)){
        return;
    }

    struct ydb_storage_csync_beginpoint beginpoint;
    SpxZero(beginpoint);

    err =  ydb_storage_sync_query_csync_beginpoint(
            s,&(beginpoint.date.year),
            &(beginpoint.date.month),
            &(beginpoint.date.day),
            &(beginpoint.offset));

    if(0 != err){
        if(ENOENT != err){
            SpxAtomicVCas(s->is_doing,true,false);
            return;//stop the thread
        }
    }

    if(0 != beginpoint.date.year){
        s->read_binlog.date.year = beginpoint.date.year;
        s->read_binlog.date.month = beginpoint.date.month;
        s->read_binlog.date.day = beginpoint.date.day;
        s->read_binlog.offset = beginpoint.offset;
    }
}

spx_private err_t ydb_storage_csync_begin_request(
        struct ydb_storage_remote *s,
        int *year,int *month,int *day,u64_t *offset
        ){/*{{{*/
    err_t err = 0;
    struct ydb_storage_configurtion *c = s->c;
    struct ydb_storage_transport_context *ystc = NULL;
    ystc = spx_alloc_alone(sizeof(*ystc),&err);
    if(NULL == ystc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new transport for query csync beginpoint to remote storage:%s is fail.",
                s->machineid);
        return 0;
    }
    if(0 == ystc->fd) {
        ystc->fd  = spx_socket_new(&err);
        if(0 >= ystc->fd){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new socket to remote storage:%s host:%s:%d "
                    "for query csync beginpoint is fail."
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
                    "for query csync beginpoint is fail."
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
        if(0 != (err = spx_socket_connect_nb(ystc->fd,
                        s->host.ip,s->host.port,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "connect to remote storage:%s host:%s:%d "
                    "for query csync beginpoint is fail."
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request){
        ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->request){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request to remote storage:%s host:%s:%d "
                    "for query csync beginpoint is fail."
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request->header){
        struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
        if(NULL == header){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request header to remote storage:%s host:%s:%d "
                    "for query csync beginpoint is fail."
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
        ystc->request->header = header;
        header->protocol = YDB_S2S_CSYNC_BEGIN;
        header->bodylen = YDB_MACHINEID_LEN + sizeof(u32_t) + sizeof(u64_t);
        header->is_keepalive = false;//persistent connection
    }

    if(NULL == ystc->request->body){
        struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,&err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request body to remote storage:%s host:%s:%d "
                    "for query csync beginpoint is fail."
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
        ystc->request->body = body;
        spx_msg_pack_fixed_chars(body,c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_u32(body,s->read_binlog.date.year);
        spx_msg_pack_u32(body,s->read_binlog.date.month);
        spx_msg_pack_u32(body,s->read_binlog.date.day);
        spx_msg_pack_u64(body,s->read_binlog.offset);
    }


    err = spx_write_context_nb(c->log,ystc->fd,ystc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write request to remote storage:%s host:%s:%d "
                "for query csync beginpoint is fail."
                s->machineid,s->host.ip,s->host.port);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->fd,c->timeout)){
        //timeout
        SpxLogFmt1(c->log,SpxLogError,
                "recv response from remote storage:%s host:%s:%d  "
                "for query csync beginpoint is timeout."
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->fd,&err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recv response header from remote storage:%s host:%s:%d  "
                "for query csync beginpoint is fail."
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = ystc->response->header->err;
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogWarn,err,
                "recv response from remote storage:%s host:%s:%d  "
                "for query csync beginpoint is fail."
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    ystc->response->body = spx_read_body_nb(c->log,
            ystc->fd,ystc->response->header->bodylen,&err);
    if(NULL == ystc->response->body){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recv response body from remote storage:%s host:%s:%d  "
                "for query csync beginpoint is fail."
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    struct spx_msg *body = ystc->response->body;
    spx_msg_seek(body,0,SpxMsgSeekSet);
    *year = spx_msg_unpack_u32(body);
    *month = spx_msg_unpack_u32(body);
    *day = spx_msg_unpack_u32(body);
    *offset = spx_msg_unpack_u64(body);
r1:
    SpxClose(ystc->fd);
    if(NULL != ystc && NULL != ystc->request){
        if(NULL !=ystc->request->header){
            SpxFree(ystc->request->header);
        }
        if(NULL != ystc->request->body){
            SpxFree(ystc->request->body);
        }
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
    return err;
}/*}}}*/


err_t ydb_storage_csync_begin_response(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc){/*{{{*/
    if(NULL == dc || NULL == dc->jc){
        return EINVAL;
    }

    struct spx_job_context *jc = dc->jc;
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
    int year = 0;
    int month = 0;
    int day = 0;
    u64_t offset = 0;
    year = spx_msg_unpack_u32(ctx);
    month = spx_msg_unpack_u32(ctx);
    day = spx_msg_unpack_u32(ctx);
    offset = spx_msg_unpack_u64(ctx);

    if(NULL == g_ydb_storage_remote){
        SpxLog1(jc->log,SpxLogError,\
                "the remote storages is null.");
        jc->err = ENOENT;
        goto r1;
    }

    struct ydb_storage_remote *s = NULL;
    jc-> err = spx_map_get(g_ydb_storage_remote,machineid,
            spx_string_len(machineid),(void **) &s,NULL);
    if(NULL == s || 0 != jc->err){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,\
                "find csync beginpoint of storage:%s is fail.",
                machineid);
        goto r1;
    }

    if(year < s->synclog.date.year
            || month < s->synclog.date.month
            || day < s->synclog.date.day
            || offset < s->synclog.offset){

    }

    struct spx_msg_header *response_header =
        spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(dc->log,SpxLogError,jc->err,\
                "alloc response header for query csync beginpoint is fail.");
        goto r1;
    }

    jc->writer_header = response_header;
    response_header->protocol = YDB_S2S_CSYNC_BEGIN;
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
        jc->writer_body_ctx = response_body_ctx;
        spx_msg_pack_u32(response_body_ctx,s->synclog.date.year);
        spx_msg_pack_u32(response_body_ctx,s->synclog.date.month);
        spx_msg_pack_u32(response_body_ctx,s->synclog.date.day);
        spx_msg_pack_u64(response_body_ctx,s->synclog.offset);
    }

r1:
    spx_string_free(machineid);
    return jc->err;
}/*}}}*/

spx_private err_t ydb_storage_csync_begin(
        struct ydb_storage_remote *s){
    err_t err = 0;
    struct ydb_storage_configurtion *c = s->c;

    if(spx_date_is_after(&(s->read_binlog.date))){
        struct spx_date today;
        SpxZero(today);
        spx_get_today(&today);
        s->read_binlog.date.year = today.year;
        s->read_binlog.date.month = today.month;
        s->read_binlog.date.day = today.day;

        s->read_binlog.fname = ydb_storage_binlog_make_filename(c->log,
                c->dologpath,c->machineid,s->read_binlog.date.year,
                s->read_binlog.date.month,s->read_binlog.date.day,&err);
        if(NULL == s->read_binlog.fname){

        }
        if(SpxFileExist(s->read_binlog.fname)){
            struct stat buf;
            SpxZero(buf);

            if(0 != stat(s->read_binlog.fname,&buf)){
            }
            s->read_binlog.offset = buf.st_size;
        }
    }

    string_t line = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),&err);
    if(NULL == line){

    }


    while(true){
        if(NULL != s->read_binlog.fp){
            if(NULL == s->read_binlog.fname){
                s->read_binlog.fname = ydb_storage_binlog_make_filename(c->log,
                        c->dologpath,c->machineid,s->read_binlog.date.year,
                        s->read_binlog.date.month,s->read_binlog.date.day,&err);
                if(NULL == s->read_binlog.fname){

                }
            }

            if(!SpxFileExist(s->read_binlog.fname)){
                if(spx_date_is_today(&(s->read_binlog.date))){
                    spx_periodic_sleep(30,0);//add a configurtion item
                    continue;
                } else if (spx_date_is_before(&(s->read_binlog.date))){
                    //warn

                    SpxStringFree(s->read_binlog.fname);
                    spx_date_add(&(s->read_binlog.date),1);
                    s->read_binlog.offset = 0;
                    continue;
                }
            }

            s->read_binlog.fp = SpxFReadOpen(s->read_binlog.fname);
            if(NULL == s->read_binlog.fp){

            }
            if(0 != s->read_binlog.offset){
                fseek(s->read_binlog.fp,s->read_binlog.offset,SEEK_SET);
            }
        }


        int size = strlen("\t");
        while(NULL != (fgets(line,SpxLineSize,s->read_binlog.fp))){
            spx_string_updatelen(line);
            s->read_binlog.offset += spx_string_len(line);
            spx_string_strip_linefeed(line);
            int count = 0;
            string_t *strs = spx_string_split(line,"\t",size,&count,&err);
            if(NULL == strs || 0 != err){
                continue;
            }
            switch(*line){
                case (YDB_STORAGE_ADD):{
                                           if(2 != count){

                                               break;
                                           }
                                           break;
                                       }
                case (YDB_STORAGE_DELETE):{
                                              if(2 != count){

                                                  break;
                                              }
                                              err =  ydb_storage_sync_delete_request(
                                                      c,s,YDB_S2S_CSYNC_DELETE,*(strs + 1));
                                              break;
                                          }
                case (YDB_STORAGE_MODIFY):{
                                              if(3 != count){

                                                  break;
                                              }
                                              break;
                                          }
                default:{
                            break;
                        }
            }
            spx_string_free_splitres(strs,count);
            spx_string_clear(line);
        }


        if(spx_date_is_today(&(s->read_binlog.date))){
            spx_periodic_sleep(30,0);//add a configurtion item
            continue;
        } else if (spx_date_is_before(&(s->read_binlog.date))){
            //warn
            SpxStringFree(s->read_binlog.fname);
            spx_date_add(&(s->read_binlog.date),1);
            s->read_binlog.offset = 0;
            continue;
        }
    }

    SpxStringFree(line);

}

/*
 * this is over csync of restore and not stop the csync
 */
spx_private err_t ydb_storage_request_restore_csync_over(
        struct ydb_storage_remote *s){/*{{{*/
    err_t err = 0;
    struct ydb_storage_configurtion *c = s->c;
    struct ydb_storage_transport_context *ystc = NULL;
    ystc = spx_alloc_alone(sizeof(*ystc),&err);
    if(NULL == ystc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new transport for over csync of restoring "
                "to remote storage:%s is fail.",
                s->machineid);
        return 0;
    }
    if(0 == ystc->fd) {
        ystc->fd  = spx_socket_new(&err);
        if(0 >= ystc->fd){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new socket to remote storage:%s host:%s:%d "
                    "for over csync of restoring is fail."
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
                    "for over csync of restoring is fail."
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
        if(0 != (err = spx_socket_connect_nb(ystc->fd,
                        s->host.ip,s->host.port,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "connect to remote storage:%s host:%s:%d "
                    "for over csync of restoring is fail."
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request){
        ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->request){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request to remote storage:%s host:%s:%d "
                    "for over csync of restoring is fail."
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request->header){
        struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
        if(NULL == header){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request header to remote storage:%s host:%s:%d "
                    "for over csync of restoring is fail."
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
        ystc->request->header = header;
        header->protocol = YDB_S2S_RESTORE_CSYNC_OVER;
        header->bodylen = YDB_MACHINEID_LEN;
        header->is_keepalive = false;//persistent connection
    }

    if(NULL == ystc->request->body){
        struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,&err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request body to remote storage:%s host:%s:%d "
                    "for over csync of restoring is fail."
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
                "for over csync of restoring is fail."
                s->machineid,s->host.ip,s->host.port);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->fd,c->timeout)){
        //timeout
        SpxLogFmt1(c->log,SpxLogError,
                "recv response from remote storage:%s host:%s:%d  "
                "for over csync of restoring is timeout."
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->fd,&err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recv response header from remote storage:%s host:%s:%d  "
                "for over csync of restoring is fail."
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = ystc->response->header->err;
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogWarn,err,
                "recv response from remote storage:%s host:%s:%d  "
                "for over csync of restoring is fail."
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
            SpxFree(ystc->request->body);
        }
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
    return err;
}/*}}}*/

err_t ydb_storage_response_restore_csync_over(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc){/*{{{*/
    if(NULL == dc || NULL == dc->jc){
        return EINVAL;
    }

    struct spx_job_context *jc = dc->jc;
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
    jc-> err = spx_map_get(g_ydb_storage_remote,machineid,
            spx_string_len(machineid),(void **) &s,NULL);
    if(NULL == s || 0 != jc->err){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,\
                "find csync beginpoint of storage:%s is fail.",
                machineid);
        goto r1;
    }
    s->is_restore_over = true;

    struct spx_msg_header *response_header =
        spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(dc->log,SpxLogError,jc->err,\
                "alloc response header for query csync beginpoint is fail.");
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

r1:
    spx_string_free(machineid);
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
        SpxLog2(c->log,SpxLogError,err,
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
            continue;
        }
        int count = 0;
        struct ydb_storage_remote *s = NULL;
        string_t *strs = spx_string_split(line,":",len,&count,&err);
        if(NULL ==  strs || 0 != *err || 5 != count){

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
                                     spx_string_len(machineid),s,sizeof(s));
                             if(0 != err){
                                 SpxLogFmt2(c->log,SpxLogError,err,
                                         "add remote-storage:%s to glb is fail.",
                                         machineid);
                                 SpxStringFree(s->machineid);
                                 SpxFree(s);
                                 goto r1;
                             }
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
                                 s->synclog.date.year = 0;
                             } else {
                                 s->synclog.date.year = atoi(syear);
                             }
                             break;
                         }
                case (6):{
                             string_t smonth = *(strs + i);
                             if(SpxStringIsNullOrEmpty(smonth)){
                                 s->synclog.date.month = 0;
                             } else {
                                 s->synclog.date.month = atoi(smonth);
                             }
                             break;
                         }
                case (7):{
                             string_t day = *(strs + i);
                             if(SpxStringIsNullOrEmpty(day)){
                                 s->synclog.date.day = 0;
                             } else {
                                 s->synclog.date.day = atoi(day);
                             }
                             break;
                         }
                case (8):{
                             string_t off = *(strs + i);
                             if(SpxStringIsNullOrEmpty(off)){
                                 s->synclog.offset = 0;
                             } else {
                                 s->synclog.offset = atoll(off);
                             }
                         }
                default:{
                            break;
                        }
            }
        }
r1:
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

    context = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),&err){
        SpxLog2(c->log,SpxLogError,err,
                "new context of sync marklog is fail.");
        goto r1;
    }

    struct spx_map_iter *iter = spx_map_iter_new(g_ydb_storage_remote,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init the trackers iter is fail.");
        goto r1;
    }

    struct spx_map_node *n = NULL;
    string_t new_context = NULL;
    new_context = spx_string_cat_printf(&err,context,"%s\n",
            "# machineid:binlog-year:binlog-month:binlog-day:binlog-offset"
            ":synclog-year:synclog-month:synclog-day:synclog-offset");
    if(NULL == new_context){
        SpxLogFmt2(c->log,SpxLogError,err,
                "cat and printf context of sync marklog by storage:%s is fail.",
                s->machineid);
        break;
    }
    context = new_context;

    while(NULL != (n = spx_map_iter_next(iter,&err))){
        SpxTypeConvert2(struct ydb_storage_remote,s,n->v);
        new_context = spx_string_cat_printf(&err,context,"%s:%d:%d:%d:%lld:%d:%d:%d:%lld\n",
                s->machineid,s->read_binlog.date.year,
                s->read_binlog.date.month,
                s->read_binlog,date.day,
                s->read_binlog.offset,
                s->synclog.date.year,
                s->synclog.date.month,
                s->synclog.date.day,
                s->synclog.offset);
        if(NULL == new_context){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "cat and printf context of sync marklog by storage:%s is fail.",
                    s->machineid);
            break;
        }
        context = new_context;
    }
    spx_vector_iter_free(&iter);

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
    int len = 0;
    size_t spx_string_len(context);
    err = spx_fwrite_string(fp,context,size,&len);
    if(0 != err || size != len){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write marklog context is fail.");
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
    string_t fname = spx_string_newlwn(NULL,SpxStringRealSize(SpxFileNameSize),err);
    if(NULL == fname){
        SpxLogFmt2(c->log,SpxLogError,*err,
                "new sync state filename is fail.");
        return NULL;
    }

    string_t new_fname = NULL;
    if(SpxStringEndWith(c->basepath,SpxLineEndDlmt)){
        new_fname = spx_string_cat_printf(err,fname,
                "%s.%s-remote-storage.marklog",
                c->basepath,c->machineid);
    } else {
        new_fname = spx_string_cat_printf(err,fname,
                "%s%c.%s-remote-storage.marklog",
                c->basepath,SpxLineEndDlmt,c->machineid);
    }

    if(NULL == new_fname){
        SpxLogFmt2(c->log,SpxLogError,*err,
                "format remote storage marklog filename is fail.");
        SpxStringFree(fname);
    }
    fname = new_fname;
    return fname;
}/*}}}*/

spx_private err_t ydb_storage_sync_upload_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        string_t fid
        ){

}


spx_private err_t ydb_storage_sync_modify_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        string_t fid
        string_t ofid
        ){

}


spx_private err_t ydb_storage_sync_delete_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        int protocol,
        string_t ofid
        ){/*{{{*/
    err_t err  = 0;
    struct ydb_storage_dsync_context *ysdc = NULL;
    ysdc = spx_alloc_alone(sizeof(*ysdc),&err);
    if(NULL == ysdc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "alloc dsync remote object is fail.",
                "fid:%s.",fid);
        return err;
    }

    ystc->sock  = spx_socket_new(&err);
    if(0 >= ystc->sock){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new socket to remote storage:%s,"
                "net address %s:%d is fail.",
                s->machineid,
                s->host.ip,s->host.port);
        goto r1;
    }

    if(0 != (err = spx_socket_set(ystc->sock,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "set socket to remote storage:%s,"
                "net address %s:%d is fail.",
                s->machineid,
                s->host.ip,s->host.port);
        goto r1;
    }
    if(0 != (err = spx_socket_connect_nb(ystc->sock,
                    s->host.ip,s->host.port,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "connect to remote storage:%s,"
                "address %s:%d is fail.",
                s->machineid,
                s->host.ip,s->host.port);
        goto r1;
    }

    ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
    if(NULL == ystc->request){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request for sync logfile "
                "to remote storage %s address %s:%d is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
    if(NULL == header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request header for sync logfile "
                "to remote storage:%s,ip:%s,port:%d.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
    header->protocol = protocol;
    header->bodylen = spx_string_len(ofid);
    header->is_keepalive = false;//persistent connection
    ystc->request->header = header;

    struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,&err);
    if(NULL == body){
        SpxLog2(c->log,SpxLogError,err,
                "new body of request for sync logfile is fail.");
        goto r1;
    }
    spx_msg_pack_string(ctx,ofid);

    err = spx_write_context_nb(c->log,ystc->sock,ystc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "send request for sync logfile "
                "to remote storage:%s,ip:%s,port:%d is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->sock,c->timeout)){
        //timeout
        SpxLogFmt1(c->log,SpxLogError,
                "recving data for query mountpoint state "
                "from remote storage:%s,ip:%s,port:%d is timeout.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->sock,&err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recving data for query mountpoint state "
                "from remote storage:%s,ip:%s,port:%d is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = ystc->response->header->err;
    if(0 != err){
        if(ENOENT == err){
            SpxLogFmt1(c->log,SpxLogWarn,
                    "logfile of machineid:%s date:%d-%d-%d is not exist.",
                    machineid,dtlogf->year,dtlogf->month,,
                    dtlogf->day);
        } else {
            SpxLogFmt1(c->log,SpxLogWarn,
                    "get logfile of machineid:%s date:%d-%d-%d is fail.",
                    machineid,dtlogf->year,dtlogf->month,,
                    dtlogf->day);
        }
        goto r1;
    }

r1:
    SpxClose(ystc->fd);
    SpxClose(binlog_fd);
    if(NULL != ptr){
        munmap(ptr,filesize);
    }
    if(0 != err){
        if(SpxFileExist(logfname)){
            SpxLogFmt2(c->log,SpxLogError,err,\
                    "write the logfile file machineid:%s "
                    "date:%d-%d-%d to memory is fail."
                    " and remove it.",
                    c->machineid,binlog_dt->year,
                    binlog_dt->month,binlog_dt->day);
            remove(logfname);
        }
    }
    if(NULL != ystc && NULL != ystc->request){
        if(NULL !=ystc->request->header){
            SpxFree(ystc->request->header);
        }
        if(NULL != ystc->request->body){
            SpxFree(ystc->request->body);
        }
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
    return err;
}/*}}}*/



