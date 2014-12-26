/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_balance.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/25 18时01分52秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#include <stdlib.h>

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
/*
 * upload:request msg:groupname
 *          size YDB_GROUPNAME_LEN
 *  modify,delete,select : msg:key(begin with group,machineid)
 *                      size:key-len
 *
 */

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_loop(\
        string_t groupname,struct spx_job_context *jc);
spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_freedisk(\
        string_t groupname,struct spx_job_context *jc);
spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_turn(\
        string_t groupname,struct spx_job_context *jc);
spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_master(
        string_t groupname,struct spx_job_context *jc);
spx_private struct ydb_remote_storage *ydb_tracker_find_storage_for_operator(\
        string_t groupname,string_t machineid,string_t syncgroup,\
        struct spx_job_context *jc,
        bool_t check_freedisk,bool_t check_syncgroup);

spx_private struct ydb_remote_storage *curr_storage = NULL;
//spx_private size_t ydb_remote_storage_idx = 0;
spx_private struct spx_map_iter *curr_iter = NULL;

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_loop(
        string_t groupname,struct spx_job_context *jc){/*{{{*/
    if(NULL == ydb_remote_storages){
        jc->err = ENOENT;
        return NULL;
    }

    struct ydb_tracker_configurtion *c = (struct ydb_tracker_configurtion *) jc->config;
    struct spx_map *map = spx_map_get(ydb_remote_storages,
            groupname,spx_string_len(groupname),NULL);
    if(NULL == map){
        jc->err = ENOENT;
        return NULL;
    }

    if(NULL == curr_iter){// not free and keep status
        curr_iter = spx_map_iter_new(map,&(jc->err));
        if(NULL == curr_iter){
            return NULL;
        }
    }

    struct ydb_remote_storage *storage = NULL;
    int trytimes = 1;
    while(true){
        struct spx_map_node *n = spx_map_iter_next(curr_iter,&(jc->err));
        if(NULL == n){
            if(!trytimes){
                break;
            }
            spx_map_iter_reset(curr_iter);
            trytimes--;
            continue;
        }

        storage = (struct ydb_remote_storage *) n->v;
        if(NULL == storage){
            continue;
        }

        time_t now = spx_now();
        if(YDB_STORAGE_RUNNING != storage->status
                || 0 >= storage->freesize
                || c->heartbeat + storage->last_heartbeat <(u64_t) now){
            continue;
        }
        break;
    }
    return storage;
}/*}}}*/

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_freedisk(
        string_t groupname,struct spx_job_context *jc){/*{{{*/
 if(NULL == ydb_remote_storages){
        jc->err = ENOENT;
        return NULL;
    }

    struct spx_map *map = spx_map_get(ydb_remote_storages,
            groupname,spx_string_len(groupname),NULL);
    if(NULL == map){
        jc->err = ENOENT;
        return NULL;
    }

    struct spx_map_iter *iter = spx_map_iter_new(map,&(jc->err));
        if(NULL == iter){
            return NULL;
        }

    struct ydb_remote_storage *storage = NULL;
    struct ydb_remote_storage *dest = NULL;
    struct ydb_tracker_configurtion *c = ToYdbTrackerConfigurtion(jc->config);
    u64_t freedisk = 0;
    time_t now = spx_now();
    while(true){
        struct spx_map_node *n = spx_map_iter_next(iter,&(jc->err));
        if(NULL == n){
            break;
        }

        storage = (struct ydb_remote_storage *) n->v;
        if(NULL == storage){
            continue;
        }

        if(YDB_STORAGE_RUNNING != storage->status
                || 0 >= storage->freesize
                || c->heartbeat + storage->last_heartbeat <(u64_t) now){
            continue;
        }
        if(freedisk < storage->freesize){
            freedisk = storage->freesize;
            dest = storage;
        }

        break;
    }
    spx_map_iter_free(&iter);
    return dest;
}/*}}}*/

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_turn(
        string_t groupname,struct spx_job_context *jc){/*{{{*/
    if(NULL == curr_storage){
        curr_storage = ydb_tracker_find_storage_by_loop(groupname,jc);
    }
    time_t now = spx_now();
    struct ydb_tracker_configurtion *c = ToYdbTrackerConfigurtion(jc->config);
    if(YDB_STORAGE_RUNNING != curr_storage->status
            || 0 >= curr_storage->freesize
            || c->heartbeat + curr_storage->last_heartbeat <(u64_t) now){
        curr_storage = ydb_tracker_find_storage_by_loop(groupname,jc);
    }
    return curr_storage;
}/*}}}*/

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_master(
        string_t groupname,struct spx_job_context *jc){/*{{{*/
    struct ydb_tracker_configurtion *c = ToYdbTrackerConfigurtion(jc->config);
        if(SpxStringIsNullOrEmpty(c->master)){
            jc->err = ENOENT;
            return NULL;
        }
        return ydb_tracker_find_storage_for_operator(groupname,c->master,NULL,jc,true,false);
}/*}}}*/

/*
 * file key:groupname + machineid + mount point idx + filename
 * file key length 7 + 7 + 2
 * filename:file type bit + version + timestamp + machineid + thread idx + file idx
 * file name legnth 1 + 2 + 8 + 8 + 8 + 8 + 8
 * filename data: offset + length
 * filename data length 8 + 8
 */

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_for_operator(\
        string_t groupname,string_t machineid,string_t syncgroup,\
        struct spx_job_context *jc,
        bool_t check_freedisk,bool_t check_syncgroup){/*{{{*/
    if(NULL == ydb_remote_storages){
        jc->err = ENOENT;
        return NULL;
    }

    struct spx_map *map = spx_map_get(ydb_remote_storages,
            groupname,spx_string_len(groupname),NULL);
    if(NULL == map){
        jc->err = ENOENT;
        return NULL;
    }


    time_t now = spx_now();
    struct ydb_tracker_configurtion *c = ToYdbTrackerConfigurtion(jc->config);
    struct ydb_remote_storage *storage = spx_map_get(map,
            machineid,spx_string_len(machineid),NULL);
    if(NULL != storage){
        if((YDB_STORAGE_RUNNING == storage->status)
                && (c->heartbeat + storage->last_heartbeat >= (u64_t) now)){
            if(check_freedisk) {
                if (0 < storage->freesize) {
                    return storage;
                }
            } else {
                return storage;
            }
        }
    }

    struct spx_map_iter *iter = spx_map_iter_new(map,&(jc->err));
    if(NULL == iter){
        return NULL;
    }
    struct spx_map_node *n = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&(jc->err)))){
        storage = (struct ydb_remote_storage *) n->v;
        if(NULL == storage){
            continue;
        }

        if((YDB_STORAGE_RUNNING != storage->status)
                || (c->heartbeat + storage->last_heartbeat <(u64_t) now)){
            storage = NULL;
            continue;
        }

        if(check_freedisk && (0 >= storage->freesize) ){
            storage = NULL;
            continue;
        }
        if(check_syncgroup && (0 != spx_string_casecmp_string(syncgroup,storage->syncgroup))){
            storage = NULL;
            continue;
        }
        break;
    }
    spx_map_iter_free(&iter);
    return storage;
}/*}}}*/

err_t ydb_tracker_query_upload_storage(struct ev_loop *loop,struct spx_task_context *tcontext){/*{{{*/
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
    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(jc->err));
    if(NULL == groupname){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "unpack groupname is fail.");
        return jc->err;
    }

    SpxLogFmt1(jc->log,SpxLogInfo,
            "accept query upload storage from client:%s in the group:%s.",
            jc->client_ip,groupname);

    struct ydb_tracker_configurtion *c = ToYdbTrackerConfigurtion(jc->config);

    struct ydb_remote_storage *storage = NULL;
    switch(c->balance){
        case YDB_TRACKER_BALANCE_LOOP:
            {
                storage = ydb_tracker_find_storage_by_loop(groupname,jc);
                break;
            }

        case YDB_TRACKER_BALANCE_MAXDISK:
            {
                storage = ydb_tracker_find_storage_by_freedisk(groupname,jc);
                break;
            }
        case YDB_TRACKER_BALANCE_TURN :
            {
                storage = ydb_tracker_find_storage_by_turn(groupname,jc);
                break;
            }
        case YDB_TRACKER_BALANCE_MASTER :
            {
                storage = ydb_tracker_find_storage_by_master(groupname,jc);
                break;
            }
        default:{
                    storage = ydb_tracker_find_storage_by_loop(groupname,jc);
                    break;
                }
    }
    if(NULL == storage){
        jc->err = 0 == jc->err ? ENOENT : jc->err;
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "find storage by %s from group:%s is fail.",\
                tracker_balance_mode_desc[c->balance],groupname);
        goto r1;
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "alloc reponse for query storage is fail.");
        goto r1;
    }
    jc->writer_header = response_header;
    response_header->protocol = YDB_C2T_QUERY_UPLOAD_STORAGE;
    response_header->version = YDB_VERSION;
    response_header->bodylen = SpxIpv4Size +  sizeof(u32_t);
//    jc->writer_header_ctx = spx_header_to_msg(response_header,SpxMsgHeaderSize,&(jc->err));
//    if(NULL == jc->writer_header_ctx){
//        SpxLog2(tcontext->log,SpxLogError,jc->err,
//                "header of query storage to msg ctx is fail.");
//        goto r1;
//    }
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(jc->err));
    if(NULL == response_body_ctx){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "alloc body buffer of query storage is fail."\
                "the buffer length is %d.",
                response_header->bodylen);
        goto r1;
    }
    jc->writer_body_ctx = response_body_ctx;
    spx_msg_pack_u32(response_body_ctx,storage->port);
    spx_msg_pack_fixed_string(response_body_ctx,storage->ip,SpxIpv4Size);
r1:
    SpxStringFree(groupname);
    return jc->err;
}/*}}}*/

err_t ydb_tracker_query_modify_storage(struct ev_loop *loop,struct spx_task_context *tcontext){/*{{{*/
    if(NULL == tcontext || NULL == tcontext->jcontext){
        return EINVAL;
    }

    struct spx_job_context *jc = tcontext->jcontext;
    string_t groupname = NULL;
    string_t machineid = NULL;
    string_t syncgroup = NULL;
    struct spx_msg *ctx = jc->reader_body_ctx;
    if(NULL == ctx){
        SpxLog1(tcontext->log,SpxLogError,\
                "reader body ctx is null.");
        return EINVAL;
    }

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
            "accept query modify storage:%s from client:%s in the group:%s with syncgroup:%s.",
            machineid,jc->client_ip,groupname,syncgroup);

    struct ydb_remote_storage *storage = ydb_tracker_find_storage_for_operator(\
            groupname,machineid,syncgroup,jc,true,true);
    if(NULL == storage){
        jc->err = 0 == jc->err ? ENOENT : jc->err;
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "find storage for modify is fail.");
        goto r1;
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "alloc reponse header for finding storage to mpdify is fail.");
        goto r1;
    }
    jc->writer_header = response_header;
    response_header->protocol = YDB_C2T_QUERY_MODIFY_STORAGE;
    response_header->version = YDB_VERSION;
    response_header->bodylen = SpxIpv4Size +  sizeof(u32_t);
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(jc->err));
    if(NULL == response_body_ctx){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "alloc reponse body buffer is fail."\
                "body buffer length is %d.",\
                response_header->bodylen);
        goto r1;
    }
    jc->writer_body_ctx = response_body_ctx;
    spx_msg_pack_fixed_string(response_body_ctx,storage->ip,SpxIpv4Size);
    spx_msg_pack_u32(response_body_ctx,storage->port);

r1:
    SpxStringFree(machineid);
    SpxStringFree(groupname);
    SpxStringFree(syncgroup);
    return jc->err;
}/*}}}*/

err_t ydb_tracker_query_delete_storage(struct ev_loop *loop,struct spx_task_context *tcontext){/*{{{*/
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
            "accept query delete storage:%s from client:%s in the group:%s with syncgroup:%s.",
            machineid,jc->client_ip,groupname,syncgroup);

    struct ydb_remote_storage *storage = ydb_tracker_find_storage_for_operator(\
            groupname,machineid,syncgroup,jc,false,true);
    if(NULL == storage){
        jc->err = 0 == jc->err ? ENOENT : jc->err;
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "find storage for delete is fail.");
        goto r1;
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "alloc response header for finding storage to deleting is fail.");
        goto r1;
    }
    jc->writer_header = response_header;
    response_header->protocol = YDB_C2T_QUERY_DELETE_STORAGE;
    response_header->version = YDB_VERSION;
    response_header->bodylen = SpxIpv4Size +  sizeof(u32_t);
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(jc->err));
    if(NULL == response_body_ctx){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "alloc reponse body buffer is fail."\
                "body buffer length is %d.",\
                response_header->bodylen);
        goto r1;
    }
    jc->writer_body_ctx = response_body_ctx;
    spx_msg_pack_fixed_string(response_body_ctx,storage->ip,SpxIpv4Size);
    spx_msg_pack_u32(response_body_ctx,storage->port);
r1:
    SpxStringFree(machineid);
    SpxStringFree(groupname);
    SpxStringFree(syncgroup);
    return jc->err;
}/*}}}*/

err_t ydb_tracker_query_select_storage(struct ev_loop *loop,struct spx_task_context *tcontext){/*{{{*/
    if(NULL == tcontext ||NULL == tcontext->jcontext){
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
            "accept query find storage:%s from client:%s in the group:%s with syncgroup:%s.",
            machineid,jc->client_ip,groupname,syncgroup);

    struct ydb_remote_storage *storage = ydb_tracker_find_storage_for_operator(\
            groupname,machineid,syncgroup,jc,false,true);

    SpxLogFmt1(jc->log,SpxLogInfo,
            "query find machineid:%s return machineid:%s.",
            machineid,storage->machineid);

    if(NULL == storage){
        jc->err = 0 == jc->err ? ENOENT : jc->err;
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "find storage for select is fail.");
        goto r1;
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLog2(tcontext->log,SpxLogError,jc->err,\
                "alloc response header for finding storage to select is fail.");
        goto r1;
    }
    jc->writer_header = response_header;
    response_header->protocol = YDB_C2T_QUERY_SELECT_STORAGE;
    response_header->version = YDB_VERSION;
    response_header->bodylen = SpxIpv4Size +  sizeof(u32_t);
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(jc->err));
    if(NULL == response_body_ctx){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "alloc reponse body buffer is fail."\
                "body buffer length is %d.",\
                response_header->bodylen);
        goto r1;
    }
    jc->writer_body_ctx = response_body_ctx;
    spx_msg_pack_fixed_string(response_body_ctx,storage->ip,SpxIpv4Size);
    spx_msg_pack_u32(response_body_ctx,storage->port);
r1:
    SpxStringFree(machineid);
    SpxStringFree(groupname);
    SpxStringFree(syncgroup);
    return jc->err;
}/*}}}*/
