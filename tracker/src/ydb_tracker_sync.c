
#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "spx_types.h"
#include "spx_defs.h"
#include "spx_message.h"
#include "spx_io.h"
#include "spx_alloc.h"
#include "spx_string.h"
#include "spx_time.h"
#include "spx_map.h"
#include "spx_collection.h"
#include "spx_job.h"
#include "spx_task.h"

#include "ydb_protocol.h"

#include "ydb_tracker_heartbeat.h"
#include "ydb_tracker_configurtion.h"



err_t ydb_tracker_query_sync_storage(struct ev_loop *loop,\
        struct spx_task_context *tcontext){/*{{{*/
    if(NULL == tcontext || NULL == tcontext->jcontext){
        return EINVAL;
    }

    struct spx_job_context *jc = tcontext->jcontext;
    struct spx_msg *ctx = jc->reader_body_ctx;
    if(NULL == ctx){
        SpxLog1(tcontext->log,SpxLogError,\
                "reader body ctx is null.");
        return EINVAL;
    }

    string_t groupname = NULL;
    string_t machineid = NULL;
    string_t syncgroup = NULL;
    string_t buf = NULL;

    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(jc->err));
    if(NULL == groupname){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "unpack groupname from msg ctx is fail.");
        return jc->err;
    }
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(jc->err));
    if(NULL == machineid){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "unpack machineid from msg ctx in the group:%s is fail.",\
                groupname);
        goto r1;
    }
    syncgroup = spx_msg_unpack_string(ctx,YDB_SYNCGROUP_LEN,&(jc->err));
    if(NULL == syncgroup){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "unpack syncgroup from msg ctx in the group:%s is fail.",\
                groupname);
        goto r1;
    }

    SpxLogFmt1(jc->log,SpxLogInfo,
            "accept query sync storage from storage:%s in the group:%s with syncgroup:%s.",
            machineid,groupname,syncgroup);

    if(NULL == ydb_remote_storages){
        SpxLogFmt1(jc->log,SpxLogError,\
                "the storages of group:%s syncgroup:%s. is not exist",
                groupname,syncgroup);
        jc->err = ENOENT;
        goto r1;
    }

    struct spx_map *map = NULL;
    jc-> err = spx_map_get(ydb_remote_storages,groupname,
            spx_string_len(groupname),(void **) &map,NULL);
    if(0 != jc->err){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,\
                "find storages from group:%s is fail.",
                groupname);
        goto r1;
    }
    if(NULL == map){
        SpxLogFmt1(jc->log,SpxLogError,\
                "the storages of group:%s is not exist",
                groupname);
        jc->err = ENOENT;
        goto r1;
    }
    struct ydb_remote_storage *s = NULL;
    buf = spx_string_new(NULL,&(jc->err));
    if(NULL == buf){
        SpxLog2(jc->log,SpxLogError,jc->err,
                "alloc the response buffer is fail.");
        goto r1;
    }

    struct spx_map_iter *iter = spx_map_iter_new(map,&(jc->err));
    if(NULL == iter){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,
                "new iter of storages of group:%s is fail.",
                groupname);
        goto r1;
    }
    struct spx_map_node *n = NULL;
    string_t newbuf = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&(jc->err)))){
        s = (struct ydb_remote_storage *) n->v;
        if(NULL == s){
            continue;
        }

        if((0 == spx_string_casecmp_string(s->syncgroup,syncgroup))
                && (0 != spx_string_casecmp_string(s->machineid,machineid))){

            newbuf =   spx_string_catalign(buf,s->machineid,spx_string_len(s->machineid),
                    YDB_MACHINEID_LEN,&(jc->err));
            if(NULL == newbuf){
                SpxLogFmt2(jc->log,SpxLogError,jc->err,
                        "cat machineid:%s of group:%s to response buffer is fail.",
                        s->machineid,s->groupname);
                break;
            }
            buf = newbuf;
            newbuf = spx_string_catalign(buf,s->ip,spx_string_len(s->ip),
                    SpxIpv4Size,&(jc->err));
            if(NULL == newbuf){
                SpxLogFmt2(jc->log,SpxLogError,jc->err,
                        "cat storage:%s  ip:%s of group:%s to response buffer is fail.",
                        s->machineid,s->ip,s->groupname);
                break;
            }
            buf = newbuf;
            newbuf = spx_string_pack_i32(buf,s->port,&(jc->err));
            if(NULL == newbuf){
                SpxLogFmt2(jc->log,SpxLogError,jc->err,
                        "cat storage:%s  port:%d of group:%s to response buffer is fail.",
                        s->machineid,s->port,s->groupname);
                break;
            }
            buf = newbuf;
            newbuf = spx_string_pack_i32(buf,s->status,&(jc->err));
            if(NULL == newbuf){
                SpxLogFmt2(jc->log,SpxLogError,jc->err,
                        "cat storage:%s  status of group:%s to response buffer is fail.",
                        s->machineid,s->groupname);
                break;
            }
            buf = newbuf;
        }
    }
    spx_map_iter_free(&iter);
    if(0 != jc->err){
        goto r1;
    }

    struct spx_msg_header *response_header =
        spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "alloc response header for finding storage to deleting is fail.");
        goto r1;
    }
    jc->writer_header = response_header;
    response_header->protocol = YDB_S2T_QUERY_SYNC_STORAGES;
    response_header->version = YDB_VERSION;
    if(SpxStringIsNullOrEmpty(buf)){
        response_header->bodylen = 0;
    } else {
        response_header->bodylen = spx_string_len(buf);
    }
//    jc->writer_header_ctx = spx_header_to_msg(response_header,
//            SpxMsgHeaderSize,&(jc->err));
//    if(NULL == jc->writer_header_ctx){
//        SpxLog2(tcontext->log,SpxLogError,jc->err,
//                "convert response header to msg ctx is fail.");
//        goto r1;
//    }
    if(0 != response_header->bodylen) {
        struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,\
                &(jc->err));
        if(NULL == response_body_ctx){
            SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                    "alloc reponse body buffer is fail."\
                    "body buffer length is %d.",\
                    response_header->bodylen);
            goto r1;
        }
        jc->writer_body_ctx = response_body_ctx;
        spx_msg_pack_string(response_body_ctx,buf);
    }

r1:
    SpxStringFree(machineid);
    SpxStringFree(groupname);
    SpxStringFree(syncgroup);
    SpxStringFree(buf);
    return jc->err;
}/*}}}*/

err_t ydb_tracker_query_base_storage(struct ev_loop *loop,\
        struct spx_task_context *tcontext){/*{{{*/
    if(NULL == tcontext || NULL == tcontext->jcontext){
        return EINVAL;
    }

    struct spx_job_context *jc = tcontext->jcontext;
    struct spx_msg *ctx = jc->reader_body_ctx;
    SpxTypeConvert2(struct ydb_tracker_configurtion,c,jc->config);
    time_t now = spx_now();
    if(NULL == ctx){
        SpxLog1(tcontext->log,SpxLogError,\
                "reader body ctx is null.");
        return EINVAL;
    }

    string_t groupname = NULL;
    string_t machineid = NULL;
    string_t syncgroup = NULL;
    string_t buf = NULL;

    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(jc->err));
    if(NULL == groupname){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "unpack groupname from msg ctx is fail.");
        return jc->err;
    }
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(jc->err));
    if(NULL == machineid){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "unpack machineid from msg ctx in the group:%s is fail.",\
                groupname);
        goto r1;
    }
    syncgroup = spx_msg_unpack_string(ctx,YDB_SYNCGROUP_LEN,&(jc->err));
    if(NULL == syncgroup){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "unpack syncgroup from msg ctx in the group:%s is fail.",\
                groupname);
        goto r1;
    }

    SpxLogFmt1(jc->log,SpxLogInfo,
            "accept query base storage from storage:%s in the group:%s with syncgroup:%s.",
            machineid,groupname,syncgroup);

    if(NULL == ydb_remote_storages){
        SpxLogFmt1(jc->log,SpxLogError,\
                "the storages of group:%s syncgroup:%s. is not exist",
                groupname,syncgroup);
        jc->err = ENOENT;
        goto r1;
    }

    struct spx_map *map = NULL;
    jc-> err = spx_map_get(ydb_remote_storages,groupname,
            spx_string_len(groupname),(void **) &map,NULL);
    if(0 != jc->err){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,\
                "find storages from group:%s is fail.",
                groupname);
        goto r1;
    }
    if(NULL == map){
        SpxLogFmt1(jc->log,SpxLogError,\
                "the storages of group:%s is not exist",
                groupname);
        jc->err = ENOENT;
        goto r1;
    }
    struct ydb_remote_storage *s = NULL;
    buf = spx_string_new(NULL,&(jc->err));
    if(NULL == buf){
        SpxLog2(jc->log,SpxLogError,jc->err,
                "alloc the response buffer is fail.");
        goto r1;
    }

    struct spx_map_iter *iter = spx_map_iter_new(map,&(jc->err));
    if(NULL == iter){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,
                "new iter of storages of group:%s is fail.",
                groupname);
        goto r1;
    }
    struct spx_map_node *n = NULL;
    struct ydb_remote_storage *base = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&(jc->err)))){
        s = (struct ydb_remote_storage *) n->v;
        if(NULL == s){
            continue;
        }

        if((0 == spx_string_casecmp_string(s->syncgroup,syncgroup))
                && (0 != spx_string_casecmp_string(s->machineid,machineid))
                && s->status != YDB_STORAGE_CLOSED
                && s->status != YDB_STORAGE_INITING
                && (s->last_heartbeat + c->heartbeat > (u64_t) now)
                ){
            if(NULL == base){
                base = s;
            } else {
                if(base->this_startup_time > s->this_startup_time){
                    base = s;
                }
            }
        }
    }
    spx_map_iter_free(&iter);

    if(NULL == base){
        SpxLogFmt1(c->log,SpxLogWarn,
                "no the base storage for querying from group:%s,"
                "syncgroup:%s,storage:%s,"
                "ip:%s.",
                groupname,syncgroup,machineid,jc->client_ip);
        jc->err = ENOENT;
        goto r1;
    }

    struct spx_msg_header *response_header =
        spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "alloc response header for finding storage to deleting is fail.");
        goto r1;
    }
    jc->writer_header = response_header;
    response_header->protocol = YDB_S2T_QUERY_BASE_STORAGE;
    response_header->version = YDB_VERSION;
    response_header->bodylen = YDB_MACHINEID_LEN;

//    jc->writer_header_ctx = spx_header_to_msg(response_header,
//            SpxMsgHeaderSize,&(jc->err));
//    if(NULL == jc->writer_header_ctx){
//        SpxLog2(tcontext->log,SpxLogError,jc->err,
//                "convert response header to msg ctx is fail.");
//        goto r1;
//    }
    if(0 != response_header->bodylen) {
        struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,\
                &(jc->err));
        if(NULL == response_body_ctx){
            SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                    "alloc reponse body buffer is fail."\
                    "body buffer length is %d.",\
                    response_header->bodylen);
            goto r1;
        }
        jc->writer_body_ctx = response_body_ctx;
        spx_msg_pack_fixed_string(response_body_ctx,s->machineid,YDB_MACHINEID_LEN);
    }

r1:
    SpxStringFree(machineid);
    SpxStringFree(groupname);
    SpxStringFree(syncgroup);
    SpxStringFree(buf);
    return jc->err;
}/*}}}*/

err_t ydb_tracker_query_timespan_for_begining_sync(struct ev_loop *loop,\
        struct spx_task_context *tcontext){/*{{{*/
    if(NULL == tcontext || NULL == tcontext->jcontext){
        return EINVAL;
    }

    struct spx_job_context *jc = tcontext->jcontext;
    struct spx_msg *ctx = jc->reader_body_ctx;
    if(NULL == ctx){
        SpxLog1(tcontext->log,SpxLogError,\
                "reader body ctx is null.");
        return EINVAL;
    }

    string_t groupname = NULL;
    string_t machineid = NULL;
    string_t syncgroup = NULL;
    string_t buf = NULL;

    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(jc->err));
    if(NULL == groupname){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "unpack groupname from msg ctx is fail.");
        return jc->err;
    }
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(jc->err));
    if(NULL == machineid){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "unpack machineid from msg ctx in the group:%s is fail.",\
                groupname);
        goto r1;
    }
    syncgroup = spx_msg_unpack_string(ctx,YDB_SYNCGROUP_LEN,&(jc->err));
    if(NULL == syncgroup){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "unpack syncgroup from msg ctx in the group:%s is fail.",\
                groupname);
        goto r1;
    }

    SpxLogFmt1(jc->log,SpxLogInfo,
            "accept query sync beginning timespan from storage:%s in the group:%s with syncgroup:%s.",
            machineid,groupname,syncgroup);

    if(NULL == ydb_remote_storages){
        SpxLogFmt1(jc->log,SpxLogError,\
                "the storages of group:%s syncgroup:%s. is not exist",
                groupname,syncgroup);
        jc->err = ENOENT;
        goto r1;
    }

    struct spx_map *map = NULL;
    jc-> err = spx_map_get(ydb_remote_storages,groupname,
            spx_string_len(groupname),(void **) &map,NULL);
    if(0 != jc->err){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,\
                "find storages from group:%s is fail.",
                groupname);
        goto r1;
    }
    if(NULL == map){
        SpxLogFmt1(jc->log,SpxLogError,\
                "the storages of group:%s is not exist",
                groupname);
        jc->err = ENOENT;
        goto r1;
    }
    struct ydb_remote_storage *s = NULL;
    buf = spx_string_new(NULL,&(jc->err));
    if(NULL == buf){
        SpxLog2(jc->log,SpxLogError,jc->err,
                "alloc the response buffer is fail.");
        goto r1;
    }

    struct spx_map_iter *iter = spx_map_iter_new(map,&(jc->err));
    if(NULL == iter){
        SpxLogFmt2(jc->log,SpxLogError,jc->err,
                "new iter of storages of group:%s is fail.",
                groupname);
        goto r1;
    }
    struct spx_map_node *n = NULL;
    u64_t timespan = 0;
    while(NULL != (n = spx_map_iter_next(iter,&(jc->err)))){
        s = (struct ydb_remote_storage *) n->v;
        if(NULL == s){
            continue;
        }
        if((0 == spx_string_casecmp_string(s->syncgroup,syncgroup))
                && (0 != spx_string_casecmp_string(s->machineid,machineid))){
            if(0 == timespan){
                timespan = s->first_startup_time;
            }else{
                if(timespan > s->first_startup_time){
                    timespan = s->first_startup_time;
                }
            }
        }
    }
    spx_map_iter_free(&iter);

    struct spx_msg_header *response_header =
        spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "alloc response header for finding storage to deleting is fail.");
        goto r1;
    }
    jc->writer_header = response_header;
    response_header->protocol = YDB_S2T_QUERY_SYNC_BEGIN_TIMESPAN;
    response_header->version = YDB_VERSION;
    response_header->bodylen = sizeof(u64_t);
//    jc->writer_header_ctx = spx_header_to_msg(response_header,
//            SpxMsgHeaderSize,&(jc->err));
//    if(NULL == jc->writer_header_ctx){
//        SpxLog2(tcontext->log,SpxLogError,jc->err,
//                "convert response header to msg ctx is fail.");
//        goto r1;
//    }
    if(0 != response_header->bodylen) {
        struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,\
                &(jc->err));
        if(NULL == response_body_ctx){
            SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                    "alloc reponse body buffer is fail."\
                    "body buffer length is %d.",\
                    response_header->bodylen);
            goto r1;
        }
        jc->writer_body_ctx = response_body_ctx;
        spx_msg_pack_u64(response_body_ctx,timespan);
    }

r1:
    SpxStringFree(machineid);
    SpxStringFree(groupname);
    SpxStringFree(syncgroup);
    SpxStringFree(buf);
    return jc->err;
}/*}}}*/

