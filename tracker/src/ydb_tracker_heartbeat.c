/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_heartbeat.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/24 11时27分22秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#include <stdlib.h>
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
#include "spx_module.h"
#include "spx_network_module.h"

#include "ydb_protocol.h"

#include "ydb_tracker_heartbeat.h"

/*
 * the ydb_remote_storage value is a map,key(groupname) -- value(map)
 * and the value is a map too,key is machineid -- value(struct
 * ydb_remote_storage);
 *
 *
 *
 * */

struct spx_map *ydb_remote_storages = NULL;


spx_private err_t  ydb_remote_storage_value_free(void **arg);
spx_private err_t  ydb_remote_storage_key_free(void **arg);
spx_private err_t ydb_remote_storage_free(void **arg);
spx_private err_t ydb_remote_storage_report(
        struct ev_loop *loop,int proto,struct spx_task_context *tcontext);

spx_private err_t ydb_remote_storage_value_free(void **arg){/*{{{*/
    if(NULL != *arg){
        struct spx_map **map = (struct spx_map **) arg;
        spx_map_free(map);
    }
    return 0;
}/*}}}*/

spx_private err_t  ydb_remote_storage_key_free(void **arg){/*{{{*/
    if(NULL != *arg){
        string_t s = (string_t ) *arg;
        SpxStringFree(s);
    }
    return 0;
}/*}}}*/

spx_private err_t ydb_remote_storage_free(void **arg){/*{{{*/
    if(NULL != *arg){
        struct ydb_remote_storage **s = (struct ydb_remote_storage **) arg;
        if(NULL != (*s)->machineid){
            SpxStringFree((*s)->machineid);
        }
        if(NULL != (*s)->groupname){
            SpxStringFree((*s)->groupname);
        }
        if(NULL != (*s)->syncgroup){
            SpxStringFree((*s)->syncgroup);
        }
        if(NULL != (*s)->ip){
            SpxStringFree((*s)->ip);
        }
        SpxFree(*s);
    }
    return 0;
}/*}}}*/

/*
 * msg:groupname + machienid + ip + port + first-start-timestamp + disksize
 *      + freesize + status
 *
 * length:GROUPNAMELEN + MACHINEIDLEN + SpxIpv4Size + sizeof(int) +
 * sizeof(u64_t) + sizeof(u64_t) + sizeof(64_t) + sizeof(int)
 *
 *
 */
spx_private err_t ydb_remote_storage_report(
        struct ev_loop *loop,
        int proto,struct spx_task_context *tcontext){/*{{{*/
    if(NULL == tcontext){
        return EINVAL;
    }
    struct spx_job_context *jc = tcontext->jcontext;
    if(NULL == jc){
        return EINVAL;
    }
    struct spx_msg *ctx = jc->reader_body_ctx;
    if(NULL == ctx){
        SpxLogFmt1(tcontext->log,SpxLogError,\
                "reader body ctx from client:%s is null.",
                jc->client_ip);
        return EINVAL;
    }

    string_t groupname = NULL;
    string_t machineid = NULL;
    string_t syncgroup = NULL;
    string_t ip = NULL;
    struct spx_map *map = NULL;
    struct ydb_remote_storage *storage = NULL;

    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(jc->err));
    if(NULL == groupname){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "unpack groupname from msg ctx is fail."
                "client ip:%s.",
                jc->client_ip);
        return jc->err;
    }
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(jc->err));
    if(NULL == machineid){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "unpack machineid from msg ctx from client:%s in the group:%s is fail.",
                jc->client_ip,groupname);
        goto r1;
    }
    syncgroup = spx_msg_unpack_string(ctx,YDB_SYNCGROUP_LEN,&(jc->err));
    if(NULL == syncgroup){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,\
                "unpack syncgroup from msg ctx from client:%s in the group:%s is fail.",
                jc->client_ip,groupname);
        goto r1;
    }

    SpxLogFmt1(jc->log,SpxLogDebug,
            "heartbeat storage:%s from client:%s in the group:%s with syncgroup:%s.",
            machineid,jc->client_ip,groupname,syncgroup);

    ip = spx_msg_unpack_string(ctx,SpxIpv4Size,&(jc->err));
    if(NULL == ip){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,
                "unpack ip from storage:%s in the group:%s with syncgroup:%s."
                "client ip:%s.",
                machineid,groupname,syncgroup,jc->client_ip);
        goto r1;
    }

    SpxLogFmt1(jc->log,SpxLogDebug,
            "accept heartbeat from storage:%s in the group:%s with syncgroup:%s.",
            machineid,groupname,syncgroup);

    if(NULL == ydb_remote_storages){
        ydb_remote_storages = spx_map_new(jc->log,
                spx_pjw,
                spx_collection_string_default_cmper,
                NULL,
                ydb_remote_storage_key_free,
                ydb_remote_storage_value_free,
                &(jc->err));
        if(NULL == ydb_remote_storages){
            SpxLogFmt2(tcontext->log,SpxLogError,jc->err,
                "new remote storages for heartbeat from storage:%s in the group:%s with syncgroup:%s."
                "client ip:%s.",
                machineid,groupname,syncgroup,jc->client_ip);
            goto r1;
        }
    }

    map = spx_map_get(ydb_remote_storages,groupname,spx_string_len(groupname),NULL);
    if(NULL == map){
        map = spx_map_new(jc->log,
                spx_pjw,
                spx_collection_string_default_cmper,
                NULL,
                ydb_remote_storage_key_free,
                ydb_remote_storage_free,
                &(jc->err));
        if(NULL == map){
            SpxLogFmt2(tcontext->log,SpxLogError,jc->err,
                "not found group:%s from remote storages "
                "for heartbeat from storage:%s with syncgroup:%s."
                "client ip:%s. and new a map for group is fail.",
                groupname,machineid,syncgroup,jc->client_ip);
            goto r1;
        }
        jc->err = spx_map_insert(ydb_remote_storages,
                groupname,spx_string_len(groupname),map,sizeof(*map));
        if(0 != jc->err){
            SpxLogFmt2(tcontext->log,SpxLogError,jc->err,
                "not found group:%s from remote storages "
                "for heartbeat from storage:%s with syncgroup:%s."
                "client ip:%s. and insert a map named group to remote storages is fail.",
                groupname,machineid,syncgroup,jc->client_ip);
            spx_map_free(&map);
            goto r1;
        }
    }

    storage = spx_map_get(map,machineid,spx_string_len(machineid),NULL);
    if(NULL == storage){
        storage = spx_alloc_alone(sizeof(*storage),&(jc->err));
        if(NULL == storage){
            SpxLogFmt2(tcontext->log,SpxLogError,jc->err,
                    "not found storage:%s from group:%s with syncgroup:%s."
                    "and new a storage is fail.",
                    machineid,groupname,syncgroup);
            goto r1;
        }
        jc->err = spx_map_insert(map,machineid,spx_string_len(machineid),storage,sizeof(*storage));
        if(0 != jc->err){
            SpxLogFmt2(tcontext->log,SpxLogError,jc->err,
                    "insert storage:%s to group:%s with syncgroup:%s."
                    "and new a storage is fail."
                    "and free storage.",
                    machineid,groupname,syncgroup);
            SpxFree(storage);
            goto r1;
        }
    }

    storage->port = spx_msg_unpack_i32(ctx);
    u64_t first_start = spx_msg_unpack_u64(ctx);
    if(0 == storage->first_startup_time || first_start < storage->first_startup_time){
        storage->first_startup_time = first_start;
    }
    storage->this_startup_time = spx_msg_unpack_u64(ctx);
    storage->disksize = spx_msg_unpack_u64(ctx);
    storage->freesize = spx_msg_unpack_u64(ctx);
    storage->status = spx_msg_unpack_u32(ctx);
    storage->last_heartbeat = spx_now();
    if(NULL == storage->groupname){
        storage->groupname = groupname;
    } else {
        SpxStringFree(groupname);
    }
    if(NULL == storage->machineid){
        storage->machineid = machineid;
    } else {
        SpxStringFree(machineid);
    }

    if(NULL != storage->syncgroup){
        SpxStringFree(storage->syncgroup);
    }
    storage->syncgroup = syncgroup;

    if(NULL == storage->ip){
        storage->ip = ip;
    }else {
        string_t oip = storage->ip;
        storage->ip = ip;
        SpxStringFree(oip);
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(jc->err));
    if(NULL == response_header){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,
                "new header for heartbeat from storage:%s "
                " in the group:%s with syncgroup:%s.",
                machineid,groupname,syncgroup);
        return jc->err;
    }

    jc->writer_header = response_header;
    response_header->protocol = proto;
    response_header->version = YDB_VERSION;
    response_header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN \
                               + YDB_SYNCGROUP_LEN + SpxIpv4Size \
                               + 4 * sizeof(u64_t) + 2 * sizeof(u32_t);
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(jc->err));
    if(NULL == response_body_ctx){
        SpxLogFmt2(tcontext->log,SpxLogError,jc->err,
                "new body ctx with len:%d for heartbeat from storage:%s "
                " in the group:%s with syncgroup:%s.",
                response_header->bodylen,
                machineid,groupname,syncgroup);
        return jc->err;
    }
    jc->writer_body_ctx = response_body_ctx;
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->groupname,YDB_GROUPNAME_LEN);
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->syncgroup,YDB_SYNCGROUP_LEN);
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->ip,SpxIpv4Size);
    spx_msg_pack_i32(response_body_ctx,storage->port);
    spx_msg_pack_u64(response_body_ctx,storage->first_startup_time);
    spx_msg_pack_u64(response_body_ctx,storage->this_startup_time);
    spx_msg_pack_u64(response_body_ctx,storage->disksize);
    spx_msg_pack_u64(response_body_ctx,storage->freesize);
    spx_msg_pack_u32(response_body_ctx,storage->status);
    return 0;
r1:
    if(NULL != groupname){
        SpxStringFree(groupname);
    }
    if(NULL != machineid){
        SpxStringFree(machineid);
    }
    if(NULL != syncgroup){
        SpxStringFree(syncgroup);
    }
    if(NULL != ip){
        SpxStringFree(ip);
    }
    return jc->err;
}/*}}}*/

err_t ydb_tracker_regedit_from_storage(struct ev_loop *loop,struct spx_task_context *tcontext){/*{{{*/
    if(NULL == tcontext){
        return EINVAL;
    }
    struct spx_job_context *jcontext = tcontext->jcontext;
    if(NULL == jcontext){
        return EINVAL;
    }
    SpxLogFmt1(jcontext->log,SpxLogInfo,
            "regedit storage from ip:%s.",
            jcontext->client_ip);
    return  ydb_remote_storage_report(loop,YDB_S2T_REGEDIT,tcontext);
}/*}}}*/

err_t ydb_tracker_heartbeat_from_storage(struct ev_loop *loop,struct spx_task_context *tcontext){/*{{{*/
    if(NULL == tcontext){
        return EINVAL;
    }
    struct spx_job_context *jcontext = tcontext->jcontext;
    if(NULL == jcontext){
        return EINVAL;
    }
    SpxLogFmt1(jcontext->log,SpxLogInfo,
            "report storage from ip:%s.",
            jcontext->client_ip);
    return  ydb_remote_storage_report(loop,YDB_S2T_HEARTBEAT,tcontext);
}/*}}}*/

err_t ydb_tracker_shutdown_from_storage(struct ev_loop *loop,struct spx_task_context *tcontext){/*{{{*/
    if(NULL == tcontext){
        return EINVAL;
    }
    struct spx_job_context *jcontext = tcontext->jcontext;
    if(NULL == jcontext){
        return EINVAL;
    }
    SpxLogFmt1(jcontext->log,SpxLogInfo,
            "shutdown storage from ip:%s.",
            jcontext->client_ip);

    return  ydb_remote_storage_report(loop,YDB_S2T_SHUTDOWN,tcontext);
}/*}}}*/
