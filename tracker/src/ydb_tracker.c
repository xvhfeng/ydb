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

spx_private void ydb_tracker_regedit_signal(struct ev_loop *loop);
spx_private void ydb_tracker_sig_empty (struct ev_loop *loop, ev_signal *w, int revents);
spx_private void ydb_tracker_sig_abort (struct ev_loop *loop, ev_signal *w, int revents);

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
        SpxLog2(log,SpxLogError,err,"alloc the confname is fail.");
        return err;
    }

    struct ydb_tracker_configurtion *c = (struct ydb_tracker_configurtion *) \
                                         spx_configurtion_parser(log,\
                                                 ydb_tracker_config_before_handle,\
                                                 NULL,\
                                                 confname,\
                                                 ydb_tracker_config_line_parser_handle,\
                                                 &err);
    if(NULL == c){
        SpxLogFmt2(log,SpxLogError,err,"parser the configurtion is fail.file name:%s.",confname);
        return err;
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
        SpxLog2(log,SpxLogError,err,"init the logger is fail.");
        return err;
    }

    struct ev_loop *mainloop = NULL;
    mainloop = ev_loop_new(0);
    if(NULL == mainloop){
        SpxLog1(log,SpxLogError,\
                "create main loop is fail.");
        abort();
    }
    ydb_tracker_regedit_signal(mainloop);
    ev_run(mainloop,EVRUN_NOWAIT);

    g_spx_job_pool = spx_job_pool_new(log,\
                     c,c->context_size,c->timeout,\
                    spx_nio_reader,\
                    spx_nio_writer,\
            ydb_tracker_network_module_header_validator_handler,\
            ydb_tracker_network_module_header_validator_fail_handler,\
            NULL,\
            ydb_tracker_network_module_request_body_handler,\
            ydb_tracker_network_module_response_body_handler,\
            &err);


    if(NULL == g_spx_job_pool){
        SpxLog2(log,SpxLogError,err,\
                "alloc job pool is fail.");
        return err;
    }

    g_spx_task_pool = spx_task_pool_new(log,\
            c->context_size,\
            ydb_tracker_task_module_handler,\
            &err);
    if(NULL == g_spx_task_pool){
        SpxLog2(log,SpxLogError,err,\
                "alloc task pool is fail.");
        return err;
    }

    g_spx_notifier_module = spx_module_new(log,\
            c->notifier_module_thread_size,\
            c->stacksize,\
//            spx_notifier_module_wakeup_handler,
            spx_notifier_module_receive_handler,\
            &err);
    if(NULL == g_spx_notifier_module){
        SpxLog2(log,SpxLogError,err,\
                "new notifier module is fail.");
        return err;
    }

    g_spx_network_module = spx_module_new(log,\
            c->network_module_thread_size,\
            c->stacksize,\
//            spx_network_module_wakeup_handler,
            spx_network_module_receive_handler,\
            &err);
    if(NULL == g_spx_network_module){
        SpxLog2(log,SpxLogError,err,\
                "new network module is fail.");
        return err;
    }

    g_spx_task_module = spx_module_new(log,\
            c->task_module_thread_size,\
            c->stacksize,\
//            spx_task_module_wakeup_handler,
            spx_task_module_receive_handler,\
            &err);
    if(NULL == g_spx_task_module){
        SpxLog2(log,SpxLogError,err,\
                "new task module is fail.");
        return err;
    }


    pthread_t tid =  ydb_tracker_mainsocket_thread_new(log,c,&err);
    if(0 != err){
        SpxLog2(log,SpxLogError,err,"create main socket thread is fail.");
        return err;
    }

    //if have maneger code please input here

    sleep(10);
    pthread_join(tid,NULL);

    return 0;
}

spx_private void ydb_tracker_regedit_signal(struct ev_loop *loop){
    ev_signal siguser1;
    ev_signal_init (&siguser1,ydb_tracker_sig_empty , SIGUSR1);
    ev_signal_start (loop, &siguser1);

    ev_signal sighup;
    ev_signal_init(&sighup,ydb_tracker_sig_empty,SIGHUP);
    ev_signal_start(loop,&sighup);

    ev_signal sigpipe;
    ev_signal_init(&sigpipe,ydb_tracker_sig_abort,SIGPIPE);
    ev_signal_start(loop,&sigpipe);

    ev_signal sigint;
    ev_signal_init(&sigint,ydb_tracker_sig_abort,SIGINT);
    ev_signal_start(loop,&sigint);
}

spx_private void ydb_tracker_sig_empty (struct ev_loop *loop, ev_signal *w, int revents){
    SpxLogDelegate *log = spx_log;
    switch(w->signum){
        case SIGUSR1:{
                         SpxLog1(log,SpxLogWarn,"catch the sigusr1 and ignore it.");
                         break;
                     }
        case SIGHUP:{
                        SpxLog1(log,SpxLogWarn,"catch the sighup and ignore it.");
                        break;
                    }
        default:{
                    SpxLogFmt1(log,SpxLogError,"catch the sig:%d,and ignore it,but no regedit the sig handler.",w->signum);
                    break;
                }
    }
}

spx_private void ydb_tracker_sig_abort (struct ev_loop *loop, ev_signal *w, int revents){
    SpxLogDelegate *log = spx_log;
    switch(w->signum){
        case SIGPIPE:{
                         SpxLog1(log,SpxLogError,"catch the sigpipe and abort the process.");
                         break;
                     }
        case SIGINT:{
                        SpxLog1(log,SpxLogError,"catch the sigint and abort the process.");
                        break;
                    }
        default:{
                    SpxLogFmt1(log,SpxLogError,"catch the sig:%d and abort it,but no regedit the sig handler.",w->signum);
                    break;
                }
    }
}
