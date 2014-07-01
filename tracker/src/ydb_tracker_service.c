/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_service.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/23 18时04分07秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#include <stdlib.h>


#include "include/spx_nio_context.h"
#include "include/spx_defs.h"
#include "include/spx_types.h"
#include "include/spx_alloc.h"
#include "include/spx_nio.h"
#include "include/spx_io.h"

#include "ydb_protocol.h"

#include "ydb_tracker_balance.h"
#include "ydb_tracker_heartbeat.h"

bool_t ydb_tracker_nio_header_validator_handler(struct spx_nio_context *nio_context){
    return true;
}

void ydb_tracker_nio_header_validator_fail_handler(struct spx_nio_context *nio_context){
    return;
}

void ydb_tracker_nio_request_body_handler(int fd,struct spx_nio_context *nio_context){
    err_t err = 0;
    switch (nio_context->request_header->protocol){
        case YDB_TRACKER_REGEDIT_FROM_STORAGE :{
                                                   err = ydb_tracker_regedit_from_storage(fd,nio_context);
                                                   break;
                                               }
        case YDB_TRACKER_HEARTBEAT_FROM_STORAGE:{
                                                    err =  ydb_tracker_heartbeat_from_storage(fd,nio_context);
                                                    break;
                                                }
        case YDB_TRACKER_SHUTDOWN_FROM_STORAGE:{
                                                   err =  ydb_tracker_shutdown_from_storage(fd,nio_context);
                                                   break;
                                               }
        case YDB_TRACKER_QUERY_UPLOAD_STORAGE:{
                                                  err =  ydb_tracker_query_upload_storage(fd,nio_context);
                                                  break;
                                              }
        case YDB_TRACKER_QUERY_DELETE_STORAGE :{
                                                   err =  ydb_tracker_query_modify_storage(fd,nio_context);
                                                   break;
                                               }
        case YDB_TRACKER_QUERY_MODIFY_STORAGE:{
                                                  err =  ydb_tracker_query_delete_storage(fd,nio_context);
                                                  break;
                                              }
        case YDB_TRACKER_QUERY_SELECT_STORAGE:{
                                                  err =  ydb_tracker_query_select_storage(fd,nio_context);
                                                  break;
                                              }
        case YDB_TRACKER_QUERY_SYNC_STORAGES:{
                                                 break;
                                             }
        case YDB_TRACKER_QUERY_STORAGE_STATUS:{
                                                  break;
                                              }
        default:{
                    err = EPERM;
                    break;
                }
    }
    if(0 != err){
        struct spx_msg_header *response_header = NULL;
        if(NULL == nio_context->response_header){
            response_header = spx_alloc_alone(sizeof(*response_header),&(nio_context->err));
            if(NULL == response_header){
                return ;
            }
            nio_context->response_header = response_header;
            response_header->protocol = nio_context->request_header->protocol;
            response_header->version = YDB_VERSION;
            response_header->bodylen = 0;
            response_header->err = err;
            response_header->offset = 0;
        } else{
            response_header = nio_context->response_header ;
            response_header->protocol = nio_context->request_header->protocol;
            response_header->version = YDB_VERSION;
            response_header->bodylen = 0;
            response_header->err = err;
            response_header->offset = 0;
        }
        if(NULL != nio_context->request_header_ctx){
            spx_msg_free(&(nio_context->response_header_ctx));
        }
        nio_context->response_header_ctx = spx_header_to_msg(response_header,SpxMsgHeaderSize,&(nio_context->err));
        if(NULL == nio_context->response_header_ctx){
            return ;
        }
    }
    spx_nio_regedit_writer(nio_context);
}


void ydb_tracker_nio_response_body_handler(int fd,struct spx_nio_context *nio_context){

    if(SpxNioLifeCycleBody != nio_context->lifecycle){
        return;
    }
    struct spx_msg *ctx = spx_header_to_msg(nio_context->response_header,SpxMsgHeaderSize,&(nio_context->err));
    if(NULL == ctx){
        SpxLog2(nio_context->log,SpxLogError,nio_context->err,\
                "response serial to ctx is fail.");
        return;
    }
    size_t len = 0;
    nio_context->err =  spx_write_from_msg_nb(fd,nio_context->response_body_ctx,nio_context->response_header->bodylen,&len);
    if(0 != nio_context->err){
        SpxLogFmt2(nio_context->log,SpxLogError,nio_context->err,\
                "write response header is fail.client:&s.",\
                nio_context->client_ip);
        return;
    }

    if(SpxMsgHeaderSize != len){
        SpxLogFmt1(nio_context->log,SpxLogError,\
                "write response header is fail.len:%d.",\
                len);
        return;
    }
    nio_context->lifecycle = SpxNioLifeCycleNormal;
    return;
}
