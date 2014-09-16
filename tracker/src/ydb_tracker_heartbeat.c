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
#include "spx_ref.h"
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
spx_private err_t ydb_remote_storage_report(struct ev_loop *loop,int proto,struct spx_task_context *tcontext);

spx_private err_t ydb_remote_storage_value_free(void **arg){
    if(NULL != *arg){
        struct spx_map **map = (struct spx_map **) arg;
        spx_map_free(map);
    }
    return 0;
}
spx_private err_t  ydb_remote_storage_key_free(void **arg){
    if(NULL != *arg){
        string_t s = (string_t ) *arg;
        spx_string_free(s);
    }
    return 0;
}

spx_private err_t ydb_remote_storage_free(void **arg){
    if(NULL != *arg){
        struct ydb_remote_storage **s = (struct ydb_remote_storage **) arg;
        if(NULL != (*s)->machineid){
            spx_string_free((*s)->machineid);
        }
        if(NULL != (*s)->groupname){
            spx_string_free((*s)->groupname);
        }
        if(NULL != (*s)->syncgroup){
            spx_string_free((*s)->syncgroup);
        }
        if(NULL != (*s)->ip){
            spx_string_free((*s)->ip);
        }
        SpxFree(*s);
    }
    return 0;
}

/*
 * msg:groupname + machienid + ip + port + first-start-timestamp + disksize
 *      + freesize + status
 *
 * length:GROUPNAMELEN + MACHINEIDLEN + SpxIpv4Size + sizeof(int) +
 * sizeof(u64_t) + sizeof(u64_t) + sizeof(64_t) + sizeof(int)
 *
 *
 */
spx_private err_t ydb_remote_storage_report(struct ev_loop *loop,int proto,struct spx_task_context *tcontext){
    if(NULL == tcontext){
        return EINVAL;
    }
    struct spx_job_context *jcontext = tcontext->jcontext;
    if(NULL == jcontext){
        return EINVAL;
    }
    struct spx_msg *ctx = jcontext->reader_body_ctx;

    string_t groupname = NULL;
    string_t machineid = NULL;
    string_t syncgroup = NULL;
    string_t ip = NULL;
    struct spx_map *map = NULL;
    struct ydb_remote_storage *storage = NULL;

    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(jcontext->err));
    if(NULL == groupname){
        return jcontext->err;
    }

    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(jcontext->err));
    if(NULL == machineid){
        goto r1;
    }

    syncgroup = spx_msg_unpack_string(ctx,YDB_SYNCGROUP_LEN,&(jcontext->err));
    if(NULL == syncgroup){
        goto r1;
    }

    ip = spx_msg_unpack_string(ctx,SpxIpv4Size,&(jcontext->err));
    if(NULL == ip){
        goto r1;
    }

    if(NULL == ydb_remote_storages){
        ydb_remote_storages = spx_map_new(jcontext->log,
                spx_pjw,
                spx_collection_string_default_cmper,
                NULL,
                ydb_remote_storage_key_free,
                ydb_remote_storage_value_free,
                &(jcontext->err));
        if(NULL == ydb_remote_storages){
            goto r1;
        }
    }

    size_t size = 0;
    spx_map_get(ydb_remote_storages,groupname,spx_string_len(groupname),(void **) &map,&size);
    if(NULL == map){
        map = spx_map_new(jcontext->log,
                spx_pjw,
                spx_collection_string_default_cmper,
                NULL,
                ydb_remote_storage_key_free,
                ydb_remote_storage_free,
                &(jcontext->err));
        if(NULL == map){
            goto r1;
        }
        jcontext->err = spx_map_insert(ydb_remote_storages,groupname,spx_string_len(groupname),map,sizeof(*map));
        if(0 != jcontext->err){
            spx_map_free(&map);
            goto r1;
        }
    }

    spx_map_get(map,machineid,spx_string_len(machineid),(void **)storage,&size);
    if(NULL == storage){
        storage = spx_alloc_alone(sizeof(*storage),&(jcontext->err));
        if(NULL == storage){
            goto r1;
        }
        jcontext->err = spx_map_insert(map,machineid,spx_string_len(machineid),storage,sizeof(*storage));
        if(0 != jcontext->err){
            SpxFree(storage);
            goto r1;
        }
    }

    storage->port = spx_msg_unpack_i32(ctx);
    u64_t first_start = spx_msg_unpack_u64(ctx);
    if(0 == storage->fisrt_start || first_start < storage->fisrt_start){
        storage->fisrt_start = first_start;
    }
    storage->disksize = spx_msg_unpack_u64(ctx);
    storage->freesize = spx_msg_unpack_u64(ctx);
    storage->status = spx_msg_unpack_u32(ctx);
    storage->last_heartbeat = spx_now();

    if(NULL != storage->groupname){
        spx_string_free(storage->groupname);
    }
    storage->groupname = groupname;

    if(NULL != storage->machineid){
        spx_string_free(storage->machineid);
    }
    storage->machineid = machineid;

    if(NULL != storage->syncgroup){
        spx_string_free(storage->syncgroup);
    }
    storage->syncgroup = syncgroup;

    if(NULL == storage->ip){
        storage->ip = ip;
    }else {
        spx_string_free(storage->ip);
        storage->ip = ip;
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(jcontext->err));
    if(NULL == response_header){
        return jcontext->err;
    }

    jcontext->writer_header = response_header;
    response_header->protocol = proto;
    response_header->version = YDB_VERSION;
    response_header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN \
                               + YDB_SYNCGROUP_LEN + SpxIpv4Size \
                               + 3 * sizeof(u64_t) + 2 * sizeof(u32_t);
    jcontext->writer_header_ctx = spx_header_to_msg(response_header,SpxMsgHeaderSize,&(jcontext->err));
    if(NULL == jcontext->writer_header_ctx){
        return jcontext->err;
    }
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(jcontext->err));
    if(NULL == response_body_ctx){
        return jcontext->err;
    }
    jcontext->writer_body_ctx = response_body_ctx;
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->groupname,YDB_GROUPNAME_LEN);
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->syncgroup,YDB_SYNCGROUP_LEN);
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->ip,SpxIpv4Size);
    spx_msg_pack_i32(response_body_ctx,storage->port);
    spx_msg_pack_u64(response_body_ctx,storage->fisrt_start);
    spx_msg_pack_u64(response_body_ctx,storage->disksize);
    spx_msg_pack_u64(response_body_ctx,storage->freesize);
    spx_msg_pack_u32(response_body_ctx,storage->status);
    return 0;
r1:
    if(NULL != groupname){
        spx_string_free(groupname);
    }
    if(NULL != machineid){
        spx_string_free(machineid);
    }
    if(NULL != syncgroup){
        spx_string_free(syncgroup);
    }
    if(NULL != ip){
        spx_string_free(ip);
    }
    return jcontext->err;
}

err_t ydb_tracker_regedit_from_storage(struct ev_loop *loop,struct spx_task_context *tcontext){
    if(NULL == tcontext){
        return EINVAL;
    }
    struct spx_job_context *jcontext = tcontext->jcontext;
    if(NULL == jcontext){
        return EINVAL;
    }
    return  ydb_remote_storage_report(loop,YDB_REGEDIT_STORAGE,tcontext);
}

err_t ydb_tracker_heartbeat_from_storage(struct ev_loop *loop,struct spx_task_context *tcontext){
    if(NULL == tcontext){
        return EINVAL;
    }
    struct spx_job_context *jcontext = tcontext->jcontext;
    if(NULL == jcontext){
        return EINVAL;
    }
    return  ydb_remote_storage_report(loop,YDB_HEARTBEAT_STORAGE,tcontext);
}

err_t ydb_tracker_shutdown_from_storage(struct ev_loop *loop,struct spx_task_context *tcontext){
    if(NULL == tcontext){
        return EINVAL;
    }
    struct spx_job_context *jcontext = tcontext->jcontext;
    if(NULL == jcontext){
        return EINVAL;
    }
    return  ydb_remote_storage_report(loop,YDB_SHUTDOWN_STORAGE,tcontext);
}
