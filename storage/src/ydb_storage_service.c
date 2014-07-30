/*
 * =====================================================================================
 *
 *       Filename:  ydb_storage_service.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/07/01 14时45分42秒
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

#include "ydb_storage_configurtion.h"

spx_private void ydb_storage_service(int fd,struct spx_nio_context *nio_context);

bool_t ydb_storage_nio_header_validator_handler(struct spx_nio_context *nio_context){
    return true;
}

void ydb_storage_nio_header_validator_fail_handler(struct spx_nio_context *nio_context){
    return;
}

void ydb_nio_reader_body_handler(int fd,struct spx_nio_context *nio_context){
    struct spx_msg_header *header = nio_context->reader_header;
    struct ydb_storage_configurtion *c = \
                                         (struct ydb_storage_configurtion *) nio_context->config;
    if(NULL == header || NULL == c){
        SpxLog1(nio_context->log,SpxLogError,\
                "request header or configurtion is null.");
        return;
    }

    //disable lazy recv or recv size less than lazy size
    size_t size = 0;
    if(!c->lazyrecv || 0 == c->lazysize \
            || c->lazysize >= header->bodylen - header->offset){
        size = header->bodylen;
        nio_context->is_lazy_recv = false;
    } else { //enable lazy recv
        size = header->bodylen - header->offset;
        nio_context->is_lazy_recv = true;
        nio_context->lazy_recv_offet = header->offset;
        nio_context->lazy_recv_size = size;
    }

    struct spx_msg *ctx = spx_msg_new(size ,&(nio_context->err));
    if(NULL == ctx){
        SpxLogFmt2(nio_context->log,SpxLogError,nio_context->err,\
                "reader body is fail.client:%s.",\
                nio_context->client_ip);
        return;
    }
    nio_context->reader_body_ctx = ctx;
    size_t len = 0;
    nio_context->err = spx_read_to_msg_nb(fd,ctx,size,&len);
    if(0 != nio_context->err){
        SpxLogFmt2(nio_context->log,SpxLogError,nio_context->err,\
                "reader body is fail.client:%s.",\
                nio_context->client_ip);
        return;
    }
    if(size != len){
        nio_context->err = ENOENT;
        SpxLogFmt2(nio_context->log,SpxLogError,nio_context->err,\
                "reader body is fail.bodylen:%lld,real body len:%lld.",
                size,len);
        return;
    }

    ydb_storage_service(fd,nio_context);
}

spx_private void ydb_storage_service(int fd,struct spx_nio_context *nio_context){
    err_t err = 0;
    switch (nio_context->reader_header->protocol){
        case YDB_STORAGE_UPLOAD:{
                                    break;
                                }
        case YDB_STORAGE_MODIFY:{
                                    break;
                                }
        case YDB_STORAGE_DELETE:{
                                    break;
                                }
        case YDB_STORAGE_FIND:{
                                    break;
                                }
        case YDB_STORAGE_SYNC:{
                                  break;
                              }
        default:{
                    err = EPERM;
                    break;
                }
    }
    if(0 != err){
        struct spx_msg_header *writer_header = NULL;
        if(NULL == nio_context->writer_header){
            writer_header = spx_alloc_alone(sizeof(*writer_header),&(nio_context->err));
            if(NULL == writer_header){
                return ;
            }
            nio_context->writer_header = writer_header;
            writer_header->protocol = nio_context->reader_header->protocol;
            writer_header->version = YDB_VERSION;
            writer_header->bodylen = 0;
            writer_header->err = err;
            writer_header->offset = 0;
        } else{
            writer_header = nio_context->writer_header ;
            writer_header->protocol = nio_context->reader_header->protocol;
            writer_header->version = YDB_VERSION;
            writer_header->bodylen = 0;
            writer_header->err = err;
            writer_header->offset = 0;
        }
        if(NULL != nio_context->reader_header){
            spx_msg_free(&(nio_context->writer_header_ctx));
        }
        nio_context->writer_header_ctx = spx_header_to_msg(writer_header,SpxMsgHeaderSize,&(nio_context->err));
        if(NULL == nio_context->writer_header_ctx){
            return ;
        }
    }
    spx_nio_regedit_writer(fd,nio_context);
}


void ydb_storage_nio_response_body_handler(int fd,struct spx_nio_context *nio_context){

    if(SpxNioLifeCycleBody != nio_context->lifecycle){
        return;
    }
    struct spx_msg *ctx = spx_header_to_msg(nio_context->writer_header,SpxMsgHeaderSize,&(nio_context->err));
    if(NULL == ctx){
        SpxLog2(nio_context->log,SpxLogError,nio_context->err,\
                "response serial to ctx is fail.");
        return;
    }
    size_t len = 0;
    nio_context->err =  spx_write_from_msg_nb(fd,nio_context->writer_body_ctx,nio_context->writer_header->bodylen,&len);
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
