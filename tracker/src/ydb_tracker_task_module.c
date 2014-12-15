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

#include "spx_task.h"
#include "spx_job.h"
#include "spx_message.h"
#include "spx_alloc.h"
#include "spx_network_module.h"
#include "spx_module.h"


#include "ydb_tracker_heartbeat.h"
#include "ydb_tracker_balance.h"
#include "ydb_tracker_sync.h"

err_t ydb_tracker_task_module_handler(struct ev_loop *loop,
        int idx,struct spx_task_context *tcontext){
    if(NULL == loop || NULL == tcontext){
        return EINVAL;
    }

    //because the deal process handler is not noblacking
    //so we can deal the error in the end of the function
    err_t err = 0;
    struct spx_job_context *jc = tcontext->jcontext;
    switch (jc->reader_header->protocol){
        case YDB_S2T_REGEDIT :
            {
                err = ydb_tracker_regedit_from_storage(loop,tcontext);
                break;
            }
        case YDB_S2T_HEARTBEAT:
            {
                err =  ydb_tracker_heartbeat_from_storage(loop,tcontext);
                break;
            }
        case YDB_S2T_SHUTDOWN:
            {
                err =  ydb_tracker_shutdown_from_storage(loop,tcontext);
                break;
            }
        case YDB_C2T_QUERY_UPLOAD_STORAGE:
            {
                err =  ydb_tracker_query_upload_storage(loop,tcontext);
                break;
            }
        case YDB_C2T_QUERY_MODIFY_STORAGE :
            {
                err =  ydb_tracker_query_modify_storage(loop,tcontext);
                break;
            }
        case YDB_C2T_QUERY_DELETE_STORAGE:
            {
                err =  ydb_tracker_query_delete_storage(loop,tcontext);
                break;
            }
        case YDB_C2T_QUERY_SELECT_STORAGE:
            {
                err =  ydb_tracker_query_select_storage(loop,tcontext);
                break;
            }
        case YDB_S2T_QUERY_SYNC_STORAGES:
            {
                err =  ydb_tracker_query_sync_storage(loop,tcontext);
                break;
            }
        case YDB_S2T_QUERY_BASE_STORAGE:
            {
                err = ydb_tracker_query_base_storage(loop,tcontext);
                break;
            }
        case YDB_S2T_QUERY_SYNC_BEGIN_TIMESPAN:
            {
                err = ydb_tracker_query_timespan_for_begining_sync(
                        loop,tcontext);
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
        if(NULL == jc->writer_header){
            response_header = spx_alloc_alone(sizeof(*response_header),&(jc->err));
            if(NULL == response_header){
                SpxLog2(jc->log,SpxLogError,jc->err,\
                        "alloc the response header for error is fail."\
                        "then forced pushing the job context to pool.");
                spx_job_pool_push(g_spx_job_pool,jc);
                return err;
            }
            jc->writer_header = response_header;
        } else {
            response_header = jc->writer_header;
        }
        response_header->protocol = jc->reader_header->protocol;
        response_header->version = YDB_VERSION;
        response_header->bodylen = 0;
        response_header->err = err;
        response_header->offset = 0;
    }

    jc->writer_header_ctx = spx_header_to_msg(jc->writer_header,SpxMsgHeaderSize,&(jc->err));
    if(NULL == jc->writer_header_ctx){
        SpxLog2(jc->log,SpxLogError,jc->err,\
                "response header to msg ctx for error is fail."\
                "then forced pushing the job context to pool.");
        spx_job_pool_push(g_spx_job_pool,jc);
        return err;
    }

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *tc = spx_get_thread(g_spx_network_module,i);
    jc->tc = tc;
    //    err = spx_module_dispatch(tc,spx_network_module_wakeup_handler,jc);
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return 0;
}


