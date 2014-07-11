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
#include "ydb_tracker_configurtion.h"
/*
 * upload:request msg:groupname
 *          size YDB_GROUPNAME_LEN
 *  modify,delete,select : msg:key(begin with group,machineid)
 *                      size:key-len
 *
 */

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_loop(\
        string_t groupname,struct spx_nio_context *nio_context);
spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_freedisk(\
        string_t groupname,struct spx_nio_context *nio_context);
spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_turn(\
        string_t groupname,struct spx_nio_context *nio_context);
spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_master(
        string_t groupname,struct spx_nio_context *nio_context);
spx_private struct ydb_remote_storage *ydb_tracker_find_storage_for_operator(\
        string_t groupname,string_t machineid,struct spx_nio_context *nio_context,
        bool_t check_freedisk);

spx_private struct ydb_remote_storage *curr_storage = NULL;
spx_private size_t ydb_remote_storage_idx = 0;
spx_private struct spx_map_iter *curr_iter = NULL;

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_loop(string_t groupname,struct spx_nio_context *nio_context){
    if(NULL == ydb_remote_storages){
        nio_context->err = ENOENT;
        return NULL;
    }

    struct spx_map *map = NULL;
    nio_context-> err = spx_map_get(ydb_remote_storages,groupname,spx_string_len(groupname),(void **) &map,NULL);
    if(0 != nio_context->err){
        return NULL;
    }
    if(NULL == map){
        nio_context->err = ENOENT;
        return NULL;
    }

    if(NULL == curr_iter){// not free and keep status
        curr_iter = spx_map_iter_new(map,&(nio_context->err));
        if(NULL == curr_iter){
            return NULL;
        }
    }

    struct ydb_remote_storage *storage = NULL;
    int trytimes = 1;
    while(true){
        struct spx_map_node *n = spx_map_iter_next(curr_iter,&(nio_context->err));
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

        size_t *heartbeat = 0;
        spx_properties_get(nio_context->config,ydb_tracker_config_heartbeat_key,(void **) &heartbeat,NULL);
        time_t now = spx_now();
        if(YDB_STORAGE_RUNNING != storage->status
                || 0 >= storage->freesize
                || *heartbeat + storage->last_heartbeat <(u64_t) now){
            continue;
        }
        break;
    }
    return storage;
}

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_freedisk(string_t groupname,struct spx_nio_context *nio_context){
 if(NULL == ydb_remote_storages){
        nio_context->err = ENOENT;
        return NULL;
    }

    struct spx_map *map = NULL;
    nio_context-> err = spx_map_get(ydb_remote_storages,groupname,spx_string_len(groupname),(void **) &map,NULL);
    if(0 != nio_context->err){
        return NULL;
    }
    if(NULL == map){
        nio_context->err = ENOENT;
        return NULL;
    }

    struct spx_map_iter *iter = spx_map_iter_new(map,&(nio_context->err));
        if(NULL == iter){
            return NULL;
        }

    struct ydb_remote_storage *storage = NULL;
    struct ydb_remote_storage *dest = NULL;
    u64_t freedisk = 0;
    while(true){
        struct spx_map_node *n = spx_map_iter_next(iter,&(nio_context->err));
        if(NULL == n){
            break;
        }

        storage = (struct ydb_remote_storage *) n->v;
        if(NULL == storage){
            continue;
        }

        size_t *heartbeat = 0;
        spx_properties_get(nio_context->config,ydb_tracker_config_heartbeat_key,(void **) &heartbeat,NULL);
        time_t now = spx_now();
        if(YDB_STORAGE_RUNNING != storage->status
                || 0 >= storage->freesize
                || *heartbeat + storage->last_heartbeat <(u64_t) now){
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
}

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_turn(string_t groupname,struct spx_nio_context *nio_context){
    if(NULL == curr_storage){
        curr_storage = ydb_tracker_find_storage_by_loop(groupname,nio_context);
    }
    size_t *heartbeat = 0;
    spx_properties_get(nio_context->config,ydb_tracker_config_heartbeat_key,(void **) &heartbeat,NULL);
    time_t now = spx_now();
    if(YDB_STORAGE_RUNNING != curr_storage->status
            || 0 >= curr_storage->freesize
            || *heartbeat + curr_storage->last_heartbeat <(u64_t) now){
        curr_storage = ydb_tracker_find_storage_by_loop(groupname,nio_context);
    }
    return curr_storage;
}

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_by_master(string_t groupname,struct spx_nio_context *nio_context){
        string_t master = NULL;
        spx_properties_get(nio_context->config,ydb_tracker_config_master_key,(void **) &master,NULL);
        if(SpxStringIsNullOrEmpty(master)){
            nio_context->err = ENOENT;
            return NULL;
        }
        return ydb_tracker_find_storage_for_operator(groupname,master,nio_context,true);
}

err_t ydb_tracker_query_upload_storage(int fd,struct spx_nio_context *nio_context){
    if(NULL == nio_context){
        return EINVAL;
    }

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
    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(nio_context->err));
    if(NULL == groupname){
        return nio_context->err;
    }

    int *mode = NULL;
    spx_properties_get(nio_context->config,ydb_tracker_config_balance_key,(void **) &mode,NULL);
    if(NULL == mode){
        nio_context->err = ENOENT;
        goto r1;
    }

    struct ydb_remote_storage *storage = NULL;
    switch(*mode){
        case YDB_TRACKER_BALANCE_LOOP:{
                                          storage = ydb_tracker_find_storage_by_loop(groupname,nio_context);
                                          break;
                                      }

        case YDB_TRACKER_BALANCE_MAXDISK:{
                                             storage = ydb_tracker_find_storage_by_freedisk(groupname,nio_context);
                                             break;
                                         }
        case YDB_TRACKER_BALANCE_TURN :{
                                           storage = ydb_tracker_find_storage_by_turn(groupname,nio_context);
                                           break;
                                       }
        case YDB_TRACKER_BALANCE_MASTER :{
                                             storage = ydb_tracker_find_storage_by_master(groupname,nio_context);
                                             break;
                                         }
        default:{
                    storage = ydb_tracker_find_storage_by_loop(groupname,nio_context);
                    break;
                }
    }
    if(NULL == storage){
        nio_context->err = 0 == nio_context->err ? ENOENT : nio_context->err;
        goto r1;
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(nio_context->err));
    if(NULL == response_header){
        return nio_context->err;
    }
    nio_context->writer_header = response_header;
    response_header->protocol = YDB_TRACKER_QUERY_UPLOAD_STORAGE;
    response_header->version = YDB_VERSION;
    response_header->bodylen = SpxIpv4Size +  sizeof(u32_t);
    nio_context->writer_header_ctx = spx_header_to_msg(response_header,SpxMsgHeaderSize,&(nio_context->err));
    if(NULL == nio_context->writer_header_ctx){
        return nio_context->err;
    }
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(nio_context->err));
    if(NULL == response_body_ctx){
        return nio_context->err;
    }
    nio_context->writer_body_ctx = response_body_ctx;
    spx_msg_pack_fixed_string(response_body_ctx,storage->ip,SpxIpv4Size);
    spx_msg_pack_u32(response_body_ctx,storage->port);
r1:
    spx_string_free(groupname);
    return nio_context->err;
}

/*
 * file key:groupname + machineid + mount point idx + filename
 * file key length 7 + 7 + 2
 * filename:file type bit + version + timestamp + machineid + thread idx + file idx
 * file name legnth 1 + 2 + 8 + 8 + 8 + 8 + 8
 * filename data: offset + length
 * filename data length 8 + 8
 */

spx_private struct ydb_remote_storage *ydb_tracker_find_storage_for_operator(\
        string_t groupname,string_t machineid,struct spx_nio_context *nio_context,
        bool_t check_freedisk){
    if(NULL == ydb_remote_storages){
        nio_context->err = ENOENT;
        return NULL;
    }

    struct spx_map *map = NULL;
    nio_context-> err = spx_map_get(ydb_remote_storages,groupname,spx_string_len(groupname),(void **) &map,NULL);
    if(0 != nio_context->err){
        return NULL;
    }
    if(NULL == map){
        nio_context->err = ENOENT;
        return NULL;
    }

    size_t *heartbeat = 0;
    spx_properties_get(nio_context->config,ydb_tracker_config_heartbeat_key,(void **) &heartbeat,NULL);

    time_t now = spx_now();
    struct ydb_remote_storage *storage = NULL;
    nio_context->err = spx_map_get(map,machineid,spx_string_len(machineid),(void **) storage,NULL);
    if(NULL != storage){
        if(YDB_STORAGE_RUNNING == storage->status
                && (check_freedisk && 0 >= storage->freesize)//for modify
                && *heartbeat + storage->last_heartbeat >= (u64_t) now){
            return storage;
        }
    }

    struct spx_map_iter *iter = spx_map_iter_new(map,&(nio_context->err));
    if(NULL == iter){
        return NULL;
    }
    struct spx_map_node *n = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&(nio_context->err)))){
        storage = (struct ydb_remote_storage *) n->v;
        if(NULL == storage){
            continue;
        }

        if(YDB_STORAGE_RUNNING != storage->status
                || (check_freedisk && 0 >= storage->freesize)
                || *heartbeat + storage->last_heartbeat <(u64_t) now){
            continue;
        }
        break;
    }
    spx_map_iter_free(&iter);
    return storage;
}


err_t ydb_tracker_query_modify_storage(int fd,struct spx_nio_context *nio_context){
    if(NULL == nio_context){
        return EINVAL;
    }

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
    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(nio_context->err));
    if(NULL == groupname){
        return nio_context->err;
    }
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(nio_context->err));
    if(NULL == machineid){
        goto r1;
    }
    struct ydb_remote_storage *storage = ydb_tracker_find_storage_for_operator(groupname,machineid,nio_context,true);
    if(NULL == storage){
        nio_context->err = 0 == nio_context->err ? ENOENT : nio_context->err;
        goto r1;
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(nio_context->err));
    if(NULL == response_header){
        return nio_context->err;
    }
    nio_context->writer_header = response_header;
    response_header->protocol = YDB_TRACKER_QUERY_MODIFY_STORAGE;
    response_header->version = YDB_VERSION;
    response_header->bodylen = SpxIpv4Size +  sizeof(u32_t);
    nio_context->writer_header_ctx = spx_header_to_msg(response_header,SpxMsgHeaderSize,&(nio_context->err));
    if(NULL == nio_context->writer_header_ctx){
        return nio_context->err;
    }
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(nio_context->err));
    if(NULL == response_body_ctx){
        return nio_context->err;
    }
    nio_context->writer_body_ctx = response_body_ctx;
    spx_msg_pack_fixed_string(response_body_ctx,storage->ip,SpxIpv4Size);
    spx_msg_pack_u32(response_body_ctx,storage->port);

r1:
    spx_string_free(machineid);
    spx_string_free(groupname);
    return nio_context->err;
}

err_t ydb_tracker_query_delete_storage(int fd,struct spx_nio_context *nio_context){
    if(NULL == nio_context){
        return EINVAL;
    }

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
    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(nio_context->err));
    if(NULL == groupname){
        return nio_context->err;
    }
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(nio_context->err));
    if(NULL == machineid){
        goto r1;
    }
    struct ydb_remote_storage *storage = ydb_tracker_find_storage_for_operator(groupname,machineid,nio_context,true);
    if(NULL == storage){
        nio_context->err = 0 == nio_context->err ? ENOENT : nio_context->err;
        goto r1;
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(nio_context->err));
    if(NULL == response_header){
        return nio_context->err;
    }
    nio_context->writer_header = response_header;
    response_header->protocol = YDB_TRACKER_QUERY_DELETE_STORAGE;
    response_header->version = YDB_VERSION;
    response_header->bodylen = SpxIpv4Size +  sizeof(u32_t);
    nio_context->writer_header_ctx = spx_header_to_msg(response_header,SpxMsgHeaderSize,&(nio_context->err));
    if(NULL == nio_context->writer_header_ctx){
        return nio_context->err;
    }
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(nio_context->err));
    if(NULL == response_body_ctx){
        return nio_context->err;
    }
    nio_context->writer_body_ctx = response_body_ctx;
    spx_msg_pack_fixed_string(response_body_ctx,storage->ip,SpxIpv4Size);
    spx_msg_pack_u32(response_body_ctx,storage->port);

r1:
    spx_string_free(machineid);
    spx_string_free(groupname);
    return nio_context->err;
}

err_t ydb_tracker_query_select_storage(int fd,struct spx_nio_context *nio_context){
    if(NULL == nio_context){
        return EINVAL;
    }

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
    groupname =  spx_msg_unpack_string(ctx,YDB_GROUPNAME_LEN,&(nio_context->err));
    if(NULL == groupname){
        return nio_context->err;
    }
    machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&(nio_context->err));
    if(NULL == machineid){
        goto r1;
    }
    struct ydb_remote_storage *storage = ydb_tracker_find_storage_for_operator(groupname,machineid,nio_context,true);
    if(NULL == storage){
        nio_context->err = 0 == nio_context->err ? ENOENT : nio_context->err;
        goto r1;
    }
    struct spx_msg_header *response_header = spx_alloc_alone(sizeof(*response_header),&(nio_context->err));
    if(NULL == response_header){
        return nio_context->err;
    }
    nio_context->writer_header = response_header;
    response_header->protocol = YDB_TRACKER_QUERY_SELECT_STORAGE;
    response_header->version = YDB_VERSION;
    response_header->bodylen = SpxIpv4Size +  sizeof(u32_t);
    nio_context->writer_header_ctx = spx_header_to_msg(response_header,SpxMsgHeaderSize,&(nio_context->err));
    if(NULL == nio_context->writer_header_ctx){
        return nio_context->err;
    }
    struct spx_msg *response_body_ctx  = spx_msg_new(response_header->bodylen,&(nio_context->err));
    if(NULL == response_body_ctx){
        return nio_context->err;
    }
    nio_context->writer_body_ctx = response_body_ctx;
    spx_msg_pack_fixed_string(response_body_ctx,storage->ip,SpxIpv4Size);
    spx_msg_pack_u32(response_body_ctx,storage->port);

r1:
    spx_string_free(machineid);
    spx_string_free(groupname);
    return nio_context->err;
}
