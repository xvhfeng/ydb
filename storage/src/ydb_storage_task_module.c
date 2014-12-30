/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_task_module.c
 *        Created:  2014/07/31 11时15分59秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <ev.h>


#include "spx_types.h"
#include "spx_task.h"
#include "spx_job.h"
#include "spx_task_module.h"
#include "spx_network_module.h"
#include "spx_io.h"
#include "spx_alloc.h"

#include "ydb_protocol.h"

#include "ydb_storage_dio_context.h"
#include "ydb_storage_storefile.h"
#include "ydb_storage_dio.h"
#include "ydb_storage_upload.h"
#include "ydb_storage_find.h"
#include "ydb_storage_delete.h"
#include "ydb_storage_modify.h"
#include "ydb_storage_disksync.h"
#include "ydb_storage_sync.h"

err_t ydb_storage_task_module_handler(struct ev_loop *loop,\
        int tidx,struct spx_task_context *tc){

    if(NULL == loop || NULL == tc){
        return EINVAL;
    }

    err_t err = 0;
    struct spx_job_context *jc = tc->jcontext;
    struct ydb_storage_storefile *sf = ydb_storage_storefile_get(\
            g_ydb_storage_storefile_pool,tidx);
    struct ydb_storage_dio_context *dc = ydb_storage_dio_pool_pop(\
            g_ydb_storage_dio_pool,&err);
    if(NULL == dc){
        SpxLog2(jc->log,SpxLogError,err,\
                "pop node from dio context pool is fail.");
        spx_task_pool_push(g_spx_task_pool,tc);
        jc->err = err;
        jc->moore = SpxNioMooreResponse;
        size_t idx = spx_network_module_wakeup_idx(jc);
        struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
        jc->tc = threadcontext;
        SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
        return err;
    }
    dc->tc = tc;
    dc->jc = jc;
    dc->storefile = sf;
    SpxLogFmt1(tc->log,SpxLogDebug,"jc idx:%d sf idx:%d.tc idx:%d",jc->idx,sf->tidx,tc->idx);

    switch (jc->reader_header->protocol){
        case (YDB_C2S_UPLOAD):
            {
                if(0 != (err = ydb_storage_dio_upload(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "upload file is fail.");
                }
                break;
            }
        case (YDB_C2S_FIND):
            {
                if(0 != (err = ydb_storage_dio_find(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,
                            "find file is fail.");
                }
                break;
            }
        case (YDB_C2S_MODIFY):
            {
                if(0 != (err = ydb_storage_dio_modify(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,
                            "modify file is fail.");
                }
                break;
            }
        case (YDB_C2S_DELETE):
            {
                if(0 != (err = ydb_storage_dio_delete(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,
                            "delete file is fail.");
                }
                break;
            }

        case (YDB_S2S_DSYNC):
            {
                if(0 != (err = ydb_storage_dsync_sync_data(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "dsync file is fail.");
                }

                break;
            }
        case (YDB_S2S_SYNC_LOGFILE):
            {
                if(0 != (err = ydb_storage_dsync_logfile(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "sync log file is fail.");
                }
                break;
            }
        case (YDB_S2S_CSYNC_BEGIN):
            {
                if(0 != (err = ydb_storage_sync_reply_make_state_machine(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "csync begin is fail.");
                }
                break;
            }
        case (YDB_S2S_QUERY_CSYNC_BEGINPOINT):
            {
                if( 0!= (err = ydb_storage_sync_reply_sync_beginpoint(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "query csync beginpoint file is fail.");
                }
                break;
            }
        case (YDB_S2S_CSYNC_ADD):
            {
                if(0 != (err = ydb_storage_sync_upload(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "csync file is fail.");
                }
                break;
            }
        case (YDB_S2S_CSYNC_DELETE):
            {
                if(0 != (err = ydb_storage_sync_delete(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "csync delete file is fail.");
                }
                break;
            }
        case (YDB_S2S_CSYNC_MODIFY):
            {
                if(0 != (err = ydb_storage_sync_modify(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "csync modify file is fail.");
                }
                break;
            }
        case (YDB_S2S_RESTORE_CSYNC_OVER):
            {
                if(0 != (err = ydb_storage_sync_reply_consistency(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "restore sync file over is fail.");
                }
                break;
            }
        default:{
                    SpxLog1(jc->log,SpxLogWarn,
                            "no the protocol for operation.");
                    spx_task_pool_push(g_spx_task_pool,dc->tc);
                    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

                    jc->writer_header = (struct spx_msg_header *)
                        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
                    if(NULL == jc->writer_header){
                        SpxLog2(jc->log,SpxLogError,err,\
                                "dispatch network module is fail,"
                                "and push jcontext to pool force.");
                        spx_job_pool_push(g_spx_job_pool,jc);
                        return err;
                    }
                    jc->writer_header->protocol = jc->reader_header->protocol;
                    jc->writer_header->bodylen = 0;
                    jc->writer_header->version = YDB_VERSION;
                    jc->writer_header->err = EACCES;

                    jc->moore = SpxNioMooreResponse;
                    size_t idx = spx_network_module_wakeup_idx(jc);
                    struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
                    jc->tc = threadcontext;
                    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
                    break;
                }
    }
    return err;
}



