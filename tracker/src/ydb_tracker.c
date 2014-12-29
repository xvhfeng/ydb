/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/20 16时44分01秒
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


#include "ydb_tracker_configurtion.h"
#include "ydb_tracker_mainsocket.h"
#include "ydb_tracker_network_module.h"
#include "ydb_tracker_task_module.h"

spx_private void ydb_tracker_regedit_signal();
spx_private void ydb_tracker_sigaction_mark(int sig);
spx_private void ydb_tracker_sigaction_exit(int sig);

int main(int argc,char **argv){
    umask(0);
    SpxLogDelegate *log = spx_log;
    printf("%d.\n",getpid());
    if(2 != argc){
        SpxLog1(log,SpxLogError,"no the configurtion file in the argument.");
        return ENOENT;
    }
    err_t err = 0;
    string_t confname = spx_string_new(argv[1],&err);
    if(NULL == confname){
        SpxLog2(log,SpxLogError,err,
                "new the confname is fail."
                "and will exit...");
        exit(err);
    }

    struct ydb_tracker_configurtion *c = (struct ydb_tracker_configurtion *) \
                                         spx_configurtion_parser(log,\
                                                 ydb_tracker_config_before_handle,\
                                                 NULL,\
                                                 confname,\
                                                 ydb_tracker_config_line_parser_handle,\
                                                 &err);
    if(NULL == c){
        SpxLogFmt2(log,SpxLogError,err,
                "parser the configurtion is fail.file name:%s."
                "and will exit...",
                confname);
        exit(err);
    }

    if(c->daemon){
        spx_env_daemon();
    }

    if(0 != ( err = spx_log_new(\
                    log,\
                    c->logpath,\
                    c->logprefix,\
                    c->logsize,\
                    c->loglevel))){
        SpxLog2(log,SpxLogError,err,
                "init the logger is fail."
                "and will exit...");
        exit(err);
    }

    ydb_tracker_regedit_signal();

    g_spx_job_pool = spx_job_pool_new(log,
                     c,c->context_size,c->timeout,
                    spx_nio_reader,
                    spx_nio_writer,
            ydb_tracker_network_module_header_validator_handler,
            ydb_tracker_network_module_header_validator_fail_handler,
            NULL,
            ydb_tracker_network_module_request_body_handler,
            ydb_tracker_network_module_response_body_handler,
            &err);
    if(NULL == g_spx_job_pool){
        SpxLog2(log,SpxLogError,err,
                "alloc job pool is fail."
                "and will exit...");
        exit(err);
    }

    g_spx_task_pool = spx_task_pool_new(log,
            c->context_size,
            ydb_tracker_task_module_handler,
            &err);
    if(NULL == g_spx_task_pool){
        SpxLog2(log,SpxLogError,err,
                "alloc task pool is fail."
                "and will exit...");
        exit(err);
    }

    g_spx_notifier_module = spx_module_new(log,
            c->notifier_module_thread_size,
            c->stacksize,
            spx_notifier_module_receive_handler,
            &err);
    if(NULL == g_spx_notifier_module){
        SpxLog2(log,SpxLogError,err,\
                "new notifier module is fail."
                "and will exit...");
        exit(err);
    }

    g_spx_network_module = spx_module_new(log,
            c->network_module_thread_size,
            c->stacksize,
            spx_network_module_receive_handler,
            &err);
    if(NULL == g_spx_network_module){
        SpxLog2(log,SpxLogError,err,
                "new network module is fail."
                "and will exit...");
        exit(err);
    }

    g_spx_task_module = spx_module_new(log,\
            c->task_module_thread_size,\
            c->stacksize,\
            spx_task_module_receive_handler,\
            &err);
    if(NULL == g_spx_task_module){
        SpxLog2(log,SpxLogError,err,\
                "new task module is fail."
                "and will exit...");
        exit(err);
    }


    pthread_t tid =  ydb_tracker_mainsocket_thread_new(log,c,&err);
    if(0 != err){
        SpxLog2(log,SpxLogError,err,
                "create main socket thread is fail."
                "and will exit...");
        exit(err);
    }

    SpxLogFmt1(log,SpxLogMark,
            "tracker start over."
            "ip:%s,port:%d",
            c->ip,c->port);

    //if have maneger code please input here

    pthread_join(tid,NULL);
    //here code is for resource recycling
    //but now this is backspace

    return 0;
}

spx_private void ydb_tracker_regedit_signal(){
    spx_env_sigaction(SIGUSR1,NULL);
    spx_env_sigaction(SIGHUP,ydb_tracker_sigaction_mark);
    spx_env_sigaction(SIGPIPE,ydb_tracker_sigaction_mark);
    spx_env_sigaction(SIGINT,ydb_tracker_sigaction_exit);
}

spx_private void ydb_tracker_sigaction_mark(int sig){
    SpxLogDelegate *log = spx_log;
    SpxLogFmt1(log,SpxLogMark,
            "emerge sig:%d,info:%s.",
            sig,strsignal(sig));
}

spx_private void ydb_tracker_sigaction_exit(int sig){
    SpxLogDelegate *log = spx_log;
    SpxLogFmt1(log,SpxLogMark,
            "emerge sig:%d,info:%s.",
            sig,strsignal(sig));
    exit(0);
}


