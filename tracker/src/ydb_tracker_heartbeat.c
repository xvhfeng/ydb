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


#include "include/spx_types.h"
#include "include/spx_defs.h"
#include "include/spx_nio_context.h"
#include "include/spx_message.h"
#include "include/spx_io.h"
#include "include/spx_alloc.h"
#include "include/spx_string.h"
#include "include/spx_time.h"
#include "include/spx_map.h"
#include "include/spx_collection.h"
#include "include/spx_ref.h"

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
spx_private err_t ydb_remote_storage_report(int fd,int proto,struct spx_nio_context *nio_context);

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
spx_private err_t ydb_remote_storage_report(int fd,int proto,struct spx_nio_context *nio_context){
    struct spx_msg_header *header = nio_context->reader_header;
    struct spx_msg *ctx = spx_msg_new(header->bodylen,&(nio_context->err));
    if(NULL == ctx){
        return EINVAL;
    }
    nio_context->reader_body_ctx = ctx;
    size_t len = 0;
    nio_context->err = spx_read_to_msg_nb(fd,ctx,header->bodylen,&len);
    if(0 != nio_context->err){
        return nio_context->err;
    }
    if(header->bodylen != len){
        nio_context->err = ENOENT;
        return nio_context->err;
    }

    string_t groupname = NULL;
    string_t machineid = NULL;
    string_t ip = NULL;
    struct spx_map *map = NULL;
    struct ydb_remote_storage *storage = NULL;

    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(nio_context->err));
    if(NULL == groupname){
        return nio_context->err;
    }

    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(nio_context->err));
    if(NULL == machineid){
        goto r1;
    }

    ip = spx_msg_unpack_string(ctx,SpxIpv4Size,&(nio_context->err));
    if(NULL == ip){
        goto r1;
    }

    if(NULL == ydb_remote_storages){
        ydb_remote_storages = spx_map_new(nio_context->log,
                spx_pjw,
                spx_collection_string_default_cmper,
                NULL,
                ydb_remote_storage_key_free,
                ydb_remote_storage_value_free,
                &(nio_context->err));
        if(NULL == ydb_remote_storages){
            goto r1;
        }
    }

    size_t size = 0;
    spx_map_get(ydb_remote_storages,groupname,spx_string_len(groupname),(void **) &map,&size);
    if(NULL == map){
        map = spx_map_new(nio_context->log,
                spx_pjw,
                spx_collection_string_default_cmper,
                NULL,
                ydb_remote_storage_key_free,
                ydb_remote_storage_free,
                &(nio_context->err));
        if(NULL == map){
            goto r1;
        }
        nio_context->err = spx_map_insert(ydb_remote_storages,groupname,spx_string_len(groupname),map,sizeof(*map));
        if(0 != nio_context->err){
            spx_map_free(&map);
            goto r1;
        }
    }

    spx_map_get(map,machineid,spx_string_len(machineid),(void **)storage,&size);
    if(NULL == storage){
        storage = spx_alloc_alone(sizeof(*storage),&(nio_context->err));
        if(NULL == storage){
            goto r1;
        }
        nio_context->err = spx_map_insert(map,machineid,spx_string_len(machineid),storage,sizeof(*storage));
        if(0 != nio_context->err){
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
    if(NULL == storage->groupname && NULL == storage->machineid){
        storage->machineid = machineid;
        storage->groupname = groupname;
    }else {
        spx_string_free(groupname);
        spx_string_free(machineid);
    }

    if(NULL == storage->ip){
        storage->ip = ip;
    }else {
        if(0 != spx_string_cmp(ip,storage->ip)){
            spx_string_free(storage->ip);
            storage->ip = ip;
        } else {
            spx_string_free(ip);
        }
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(nio_context->err));
    if(NULL == response_header){
        return nio_context->err;
    }
    nio_context->writer_header = response_header;
    response_header->protocol = proto;
    response_header->version = YDB_VERSION;
    response_header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN + SpxIpv4Size + 3 * sizeof(u64_t) + 2 * sizeof(u32_t);
    nio_context->writer_header_ctx = spx_header_to_msg(response_header,SpxMsgHeaderSize,&(nio_context->err));
    if(NULL == nio_context->writer_header_ctx){
        return nio_context->err;
    }
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(nio_context->err));
    if(NULL == response_body_ctx){
        return nio_context->err;
    }
    nio_context->writer_body_ctx = response_body_ctx;
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->groupname,YDB_GROUPNAME_LEN);
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_ubytes(response_body_ctx,(ubyte_t *) storage->ip,SpxIpv4Size);
    spx_msg_pack_i32(response_body_ctx,storage->port);
    spx_msg_pack_u64(response_body_ctx,storage->fisrt_start);
    spx_msg_pack_u64(response_body_ctx,storage->disksize);
    spx_msg_pack_u64(response_body_ctx,storage->freesize);
    spx_msg_pack_u32(response_body_ctx,storage->status);
    return 0;
r1:
    spx_string_free(groupname);
    spx_string_free(machineid);
    return nio_context->err;
}
err_t ydb_tracker_regedit_from_storage(int fd,struct spx_nio_context *nio_context) {
    return ydb_remote_storage_report(fd,YDB_REGEDIT_STORAGE,nio_context);
}

err_t ydb_tracker_heartbeat_from_storage(int fd,struct spx_nio_context *nio_context){
    return ydb_remote_storage_report(fd,YDB_HEARTBEAT_STORAGE,nio_context);
}

err_t ydb_tracker_shutdown_from_storage(int fd,struct spx_nio_context *nio_context){
    return ydb_remote_storage_report(fd,YDB_SHUTDOWN_STORAGE,nio_context);
}

