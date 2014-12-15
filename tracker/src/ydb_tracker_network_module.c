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

#include "spx_types.h"
#include "spx_defs.h"
#include "spx_job.h"
#include "spx_module.h"
#include "spx_task_module.h"
#include "spx_network_module.h"
#include "spx_io.h"
#include "spx_nio.h"
#include "spx_task.h"


bool_t ydb_tracker_network_module_header_validator_handler(struct spx_job_context *jcontext){
    return true;
}

void ydb_tracker_network_module_header_validator_fail_handler(struct spx_job_context *jcontext){
    return;
}

void ydb_tracker_network_module_request_body_handler(
        struct ev_loop *loop,int fd,struct spx_job_context *jcontext){
    spx_nio_reader_body_handler(loop,fd,jcontext);
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
    struct spx_thread_context *tc = spx_get_thread(g_spx_task_module,idx);
    jcontext->tc = tc;
//    spx_module_dispatch(tc,spx_task_module_wakeup_handler,tcontext);
    SpxModuleDispatch(spx_task_module_wakeup_handler,tcontext);
    return;
}

void ydb_tracker_network_module_response_body_handler(
        struct ev_loop *loop,int fd,struct spx_job_context *jcontext){
    spx_nio_writer_body_handler(loop,fd,jcontext);
    if(0 != jcontext->err){
        SpxLog2(jcontext->log,SpxLogError,jcontext->err,\
                "write body buffer is fail.");
    }
    ev_io_stop(loop,&(jcontext->watcher));
    if(jcontext->reader_header->is_keepalive){
        spx_job_context_reset(jcontext);
        size_t idx = spx_network_module_wakeup_idx(jcontext);
        SpxLogFmt1(jcontext->log,SpxLogDebug,\
                "recv the client:%s connection.sock:%d."\
                "and send to thread:%d to deal.",
                jcontext->client_ip,jcontext->fd,idx);
        struct spx_thread_context *tc = spx_get_thread(g_spx_network_module,idx);
        jcontext->tc = tc;
        jcontext->moore = SpxNioMooreRequest;
        SpxModuleDispatch(spx_network_module_wakeup_handler,jcontext);
    } else {
        spx_job_pool_push(g_spx_job_pool,jcontext);
    }
    return;
}

