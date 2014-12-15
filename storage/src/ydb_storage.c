/*
 * =====================================================================================
 *
 *       Filename:  ydb_storage.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/07/01 13时53分05秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#include <ev.h>


#include "spx_types.h"
#include "spx_string.h"
#include "spx_defs.h"
#include "spx_log.h"
#include "spx_socket.h"
#include "spx_env.h"
#include "spx_nio.h"
#include "spx_configurtion.h"
#include "spx_module.h"
#include "spx_job.h"
#include "spx_task.h"
#include "spx_notifier_module.h"
#include "spx_network_module.h"
#include "spx_task_module.h"
#include "spx_periodic.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_mainsocket.h"
#include "ydb_storage_network_module.h"
#include "ydb_storage_task_module.h"
#include "ydb_storage_heartbeat.h"
#include "ydb_storage_dio.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_lifecycle.h"
#include "ydb_storage_disksync.h"
#include "ydb_storage_sync.h"


spx_private void ydb_storage_regedit_signal();
spx_private void ydb_storage_sigaction_mark(int sig);
spx_private void ydb_storage_sigaction_exit(int sig);

int main(int argc,char **argv){
    umask(0);
    SpxLogDelegate *log = spx_log;
    if(2 != argc){
        SpxLog1(log,SpxLogError,"no the configurtion file in the argument.");
        return ENOENT;
    }
    err_t err = 0;
    string_t confname = spx_string_new(argv[1],&err);
    if(NULL == confname){
        SpxLog2(log,SpxLogError,err,"alloc the confname is fail.");
        abort();
    }

    struct ydb_storage_configurtion *c = (struct ydb_storage_configurtion *) \
                                         spx_configurtion_parser(log,\
                                                 ydb_storage_config_before_handle,\
                                                 NULL,\
                                                 confname,\
                                                 ydb_storage_config_line_parser,\
                                                 &err);
    if(NULL == c || 0 != err){
        SpxLogFmt2(log,SpxLogError,err,"parser the configurtion is fail.file name:%s.",confname);
        abort();
    }

    if(c->daemon){
        spx_env_daemon();
    }

    ydb_storage_regedit_signal();

    if(0 != ( err = spx_log_new(\
                    log,\
                    c->logpath,\
                    c->logprefix,\
                    c->logsize,\
                    c->loglevel))){
        SpxLog2(log,SpxLogError,err,"init the logger is fail.");
        abort();
    }

    g_ydb_storage_runtime = ydb_storage_runtime_init(log,c,&err);
    if(NULL == g_ydb_storage_runtime || 0 != err){
        SpxLog2(log,SpxLogError,err,
                "init storage runtime is fail.");
        abort();
    }

    g_ydb_storage_runtime->status = YDB_STORAGE_INITING;

    if(0 != (err = ydb_storage_dio_mountpoint_init(c))){
        SpxLog2(log,SpxLogError,err,
                "init mountpoint store is fail.");
        abort();
    }

    g_ydb_storage_storefile_pool =
        ydb_storage_storefile_pool_new(log,c->task_module_thread_size,&err);
    if(NULL == g_ydb_storage_storefile_pool || 0 != err){
        SpxLog2(log,SpxLogError,err,
                "new storefile pool is fail,");
        abort();
    }

    g_ydb_storage_dio_pool =
        ydb_storage_dio_pool_new(log,c,c->context_size,&err);
    if(NULL == g_ydb_storage_dio_pool || 0 != err){
        SpxLog2(log,SpxLogError,err,
                "new storage dio pool is fail.");
        abort();
    }

    g_ydb_storage_binlog = ydb_storage_binlog_new(log,c->dologpath,
            c->machineid,&err);
    if(NULL == g_ydb_storage_binlog || 0 != err){
        SpxLog2(log,SpxLogError,err,
                "new storage binlog is fail.");
        abort();
    }

    g_spx_job_pool = spx_job_pool_new(log,\
            c,\
            c->context_size,\
            c->timeout,\
            spx_nio_reader,\
            spx_nio_writer,\
            ydb_storage_network_module_header_validator_handler,\
            ydb_storage_network_module_header_validator_fail_handler,\
            ydb_storage_network_module_request_body_before_handler,\
            ydb_storage_network_module_request_body_handler,\
            ydb_storage_network_module_response_body_handler,\
            &err);
    if(NULL == g_spx_job_pool){
        SpxLog2(log,SpxLogError,err,\
                "alloc job pool is fail.");
        abort();
    }

    g_spx_task_pool = spx_task_pool_new(log,\
            c->context_size,\
            ydb_storage_task_module_handler,\
            &err);
    if(NULL == g_spx_task_pool){
        SpxLog2(log,SpxLogError,err,\
                "alloc task pool is fail.");
        abort();
    }

    g_spx_notifier_module = spx_module_new(log,\
            c->notifier_module_thread_size,\
            c->stacksize,\
            spx_notifier_module_receive_handler,\
            &err);
    if(NULL == g_spx_notifier_module){
        SpxLog2(log,SpxLogError,err,\
                "new notifier module is fail.");
        abort();
    }

    g_spx_network_module = spx_module_new(log,\
            c->network_module_thread_size,\
            c->stacksize,\
            spx_network_module_receive_handler,\
            &err);
    if(NULL == g_spx_network_module){
        SpxLog2(log,SpxLogError,err,\
                "new network module is fail.");
        abort();
    }

    g_spx_task_module = spx_module_new(log,\
            c->task_module_thread_size,\
            c->stacksize,\
            spx_task_module_receive_handler,\
            &err);
    if(NULL == g_spx_task_module){
        SpxLog2(log,SpxLogError,err,\
                "new task module is fail.");
        abort();
    }

    pthread_t heartbeat_tid = ydb_storage_heartbeat_service_init( log,c->timeout,c,&err);
    if(0 == heartbeat_tid && 0 != err){
        SpxLog2(log,SpxLogError,err,
                "new heartbeat thread is fail.");
        abort();
    }

    struct ydb_storage_mainsocket *socket=
        ydb_storage_mainsocket_thread_new(c,&err);
    if(NULL == socket){
        SpxLog2(log,SpxLogError,err,"create main socket thread is fail.");
        abort();
    }

    err = ydb_storage_sync_restore(c);

    struct spx_periodic *pdQueryRemoteStorages =  spx_periodic_exec_and_async_run(c->log,
            c->query_sync_timespan,0,
            ydb_storage_sync_query_remote_storages,
            (void *) c,
            c->stacksize,
            &err);

    err =  ydb_storage_dsync_startup(c,g_ydb_storage_runtime);

    g_sync_threadpool = spx_threadpool_new(c->log,c->sync_threads_count,
            c->stacksize,&err);

    g_ydb_storage_runtime->status = YDB_STORAGE_CSYNCING;

    struct spx_periodic *pdSyncHeartbeat = spx_periodic_exec_and_async_run(c->log,
            c->query_sync_timespan,0,ydb_storage_sync_heartbeat,
            g_ydb_storage_runtime,c->stacksize,&err);

    struct spx_periodic *pdRuntimeFlush = spx_periodic_exec_and_async_run(c->log,
            c->refreshtime,0,ydb_storage_startup_runtime_flush,
            g_ydb_storage_runtime,c->stacksize,&err);

    pthread_join(socket->tid,NULL);
    return 0;
}

spx_private void ydb_storage_regedit_signal(){
    spx_env_sigaction(SIGUSR1,NULL);
    spx_env_sigaction(SIGHUP,ydb_storage_sigaction_mark);
    spx_env_sigaction(SIGPIPE,ydb_storage_sigaction_mark);
    spx_env_sigaction(SIGINT,ydb_storage_sigaction_exit);
}

spx_private void ydb_storage_sigaction_mark(int sig){
    SpxLogDelegate *log = spx_log;
    SpxLogFmt1(log,SpxLogMark,
            "emerge sig:%d,info:%s.",
            sig,strsignal(sig));
}

spx_private void ydb_storage_sigaction_exit(int sig){
    SpxLogDelegate *log = spx_log;
    SpxLogFmt1(log,SpxLogMark,
            "emerge sig:%d,info:%s.",
            sig,strsignal(sig));
    exit(0);
}
