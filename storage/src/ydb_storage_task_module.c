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

#include "ydb_protocol.h"

#include "ydb_storage_dio_context.h"
#include "ydb_storage_storefile.h"
#include "ydb_storage_dio.h"
#include "ydb_storage_upload.h"

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
        err = spx_module_dispatch(g_spx_network_module,idx,jc);
        return err;
    }
    dc->tc = tc;
    dc->jc = jc;
    dc->storefile = sf;

    switch (jc->reader_header->protocol){
        case (YDB_STORAGE_UPLOAD):
            {
                if(0 != (err = ydb_storage_dio_upload(loop,dc))){
                    SpxLog2(jc->log,SpxLogError,err,\
                            "upload file is fail.");
                }
                break;
            }
        case (YDB_STORAGE_FIND):
            {
                break;
            }
        case (YDB_STORAGE_MODIFY):
            {
                break;
            }
        case (YDB_STORAGE_DELETE):
            {
                break;
            }
        default:{
                    break;
                }
    }
    return err;
}



