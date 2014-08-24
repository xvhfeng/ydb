/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_tracker_network_module.c
 *        Created:  2014/07/28 16时49分35秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <ev.h>
#include <errno.h>

#include "include/spx_types.h"
#include "include/spx_defs.h"
#include "include/spx_job.h"
#include "include/spx_module.h"
#include "include/spx_task_module.h"
#include "include/spx_io.h"
#include "include/spx_nio.h"
#include "include/spx_task.h"


bool_t ydb_tracker_network_module_header_validator_handler(struct spx_job_context *jcontext){
    return true;
}

void ydb_tracker_network_module_header_validator_fail_handler(struct spx_job_context *jcontext){
    return;
}

void ydb_tracker_network_module_request_body_handler(int fd,struct spx_job_context *jcontext){
    spx_nio_reader_body_handler(fd,jcontext,jcontext->reader_header->bodylen);
    if(0 != jcontext->err){
        SpxLog2(jcontext->log,SpxLogError,jcontext->err,\
                "read body is fail.");
        return;
    }

    size_t idx = 0;
    struct spx_task_context *tcontext = spx_task_pool_pop(g_spx_task_pool,&(jcontext->err));
    if(0 != jcontext->err){
        SpxLog2(jcontext->log,SpxLogError,jcontext->err,\
                "pop task context from task context pool is fail.");
        return;
    }
    tcontext->jcontext = jcontext;
    spx_module_dispatch(g_spx_task_module,idx,tcontext);
    return;
}

void ydb_tracker_network_module_response_body_handler(int fd,struct spx_job_context *jcontext){
    spx_nio_writer_body_handler(fd,jcontext,jcontext->writer_header->bodylen);
    if(0 != jcontext->err){
        SpxLog2(jcontext->log,SpxLogError,jcontext->err,\
                "write body buffer is fail.");
    }
    return;
}

