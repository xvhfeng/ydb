/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_tracker_task_module.c
 *        Created:  2014/07/29 09时38分53秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "ydb_protocol.h"

#include "include/spx_task.h"
#include "include/spx_job.h"
#include "include/spx_message.h"
#include "include/spx_alloc.h"
#include "include/spx_network_module.h"
#include "include/spx_module.h"


#include "ydb_tracker_heartbeat.h"
#include "ydb_tracker_balance.h"

err_t ydb_tracker_task_module_handler(struct ev_loop *loop,struct spx_task_context *tcontext){
    if(NULL == loop || NULL == tcontext){
        return EINVAL;
    }

    //because the deal process handler is not noblacking
    //so we can deal the error in the end of the function
    err_t err = 0;
    struct spx_job_context *jcontext = tcontext->jcontext;
    switch (jcontext->reader_header->protocol){
        case YDB_REGEDIT_STORAGE :{
                                      err = ydb_tracker_regedit_from_storage(loop,tcontext);
                                      break;
                                  }
        case YDB_HEARTBEAT_STORAGE:{
                                       err =  ydb_tracker_heartbeat_from_storage(loop,tcontext);
                                       break;
                                   }
        case YDB_SHUTDOWN_STORAGE:{
                                      err =  ydb_tracker_shutdown_from_storage(loop,tcontext);
                                      break;
                                  }
        case YDB_TRACKER_QUERY_UPLOAD_STORAGE:{
                                                  err =  ydb_tracker_query_upload_storage(loop,tcontext);
                                                  break;
                                              }
        case YDB_TRACKER_QUERY_DELETE_STORAGE :{
                                                   err =  ydb_tracker_query_modify_storage(loop,tcontext);
                                                   break;
                                               }
        case YDB_TRACKER_QUERY_MODIFY_STORAGE:{
                                                  err =  ydb_tracker_query_delete_storage(loop,tcontext);
                                                  break;
                                              }
        case YDB_TRACKER_QUERY_SELECT_STORAGE:{
                                                  err =  ydb_tracker_query_select_storage(loop,tcontext);
                                                  break;
                                              }
        case YDB_QUERY_SYNC_STORAGES:{
                                         break;
                                     }
        case YDB_QUERY_STORAGE_STATUS:{
                                          break;
                                      }
        default:{
                    err = EPERM;
                    break;
                }
    }
    spx_task_pool_push(g_spx_task_pool,tcontext);
    if(0 != err){
        struct spx_msg_header *response_header = NULL;
        if(NULL == jcontext->writer_header){
            response_header = spx_alloc_alone(sizeof(*response_header),&(jcontext->err));
            if(NULL == response_header){
                SpxLog2(jcontext->log,SpxLogError,jcontext->err,\
                        "alloc the response header for error is fail."\
                        "then forced pushing the job context to pool.");
                spx_job_pool_push(g_spx_job_pool,jcontext);
                return err;
            }
            jcontext->writer_header = response_header;
            response_header->protocol = jcontext->reader_header->protocol;
            response_header->version = YDB_VERSION;
            response_header->bodylen = 0;
            response_header->err = err;
            response_header->offset = 0;
        } else{
            response_header = jcontext->writer_header ;
            response_header->protocol = jcontext->reader_header->protocol;
            response_header->version = YDB_VERSION;
            response_header->bodylen = 0;
            response_header->err = err;
            response_header->offset = 0;
        }
        jcontext->writer_header_ctx = spx_header_to_msg(response_header,SpxMsgHeaderSize,&(jcontext->err));
        if(NULL == jcontext->writer_header_ctx){
            SpxLog2(jcontext->log,SpxLogError,jcontext->err,\
                    "response header to msg ctx for error is fail."\
                    "then forced pushing the job context to pool.");
            spx_job_pool_push(g_spx_job_pool,jcontext);
            return err;
        }
    }

    jcontext->moore = SpxNioMooreResponse;
    size_t idx = jcontext->idx % g_spx_network_module->threadpool->curr_size;
    err = spx_module_dispatch(g_spx_network_module,idx,jcontext);
    if(0 != err){
            SpxLog2(jcontext->log,SpxLogError,jcontext->err,\
                    "dispath network module from task module is fail."\
                    "then forced pushing the job context to pool.");
            spx_job_pool_push(g_spx_job_pool,jcontext);
    }
    return err;
}

