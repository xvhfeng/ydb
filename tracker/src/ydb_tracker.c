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


#include "include/spx_types.h"
#include "include/spx_string.h"
#include "include/spx_defs.h"
#include "include/spx_properties.h"
#include "include/spx_log.h"
#include "include/spx_socket.h"
#include "include/spx_nio_context.h"
#include "include/spx_env.h"
#include "include/spx_nio.h"
#include "include/spx_sio.h"


#include "ydb_tracker_configurtion.h"
#include "ydb_tracker_service.h"
#include "ydb_tracker_mainsocket.h"

spx_private void ydb_tracker_regedit_signal(struct ev_loop *loop);
spx_private void ydb_tracker_sig_empty (struct ev_loop *loop, ev_signal *w, int revents);
spx_private void ydb_tracker_sig_abort (struct ev_loop *loop, ev_signal *w, int revents);

int main(int argc,char **argv){

    umask(0);
    SpxLogDelegate *log = spx_log;
    if(0 != argc){
        SpxLog1(log,SpxLogError,"no the configurtion file in the argument.");
        return ENOENT;
    }
    err_t err = 0;
    string_t confname = spx_string_new(argv[1],&err);
    if(NULL == confname){
        SpxLog2(log,SpxLogError,err,"alloc the confname is fail.");
        return err;
    }

    struct spx_properties *configurtion = spx_properties_new(log,
            ydb_tracker_config_line_deserialize,
            NULL,
            ydb_tracker_config_parser_before_handle,
            NULL,
            &err);
    if(NULL == configurtion){
        SpxLogFmt2(log,SpxLogError,err,"parser the configurtion is fail.file name:%s.",confname);
        return err;
    }

    bool_t *daemon = NULL;
    err =  spx_properties_get(configurtion,\
            ydb_tracker_config_daemon_key,\
            (void **) &daemon,NULL);
    if(NULL != daemon && *daemon){
        spx_env_daemon();
    }

    string_t logpath = NULL;
    string_t logprefix = NULL;
    u64_t *logsize = NULL;
    u8_t *loglevel = NULL;
    err =  spx_properties_get(configurtion,\
            ydb_tracker_config_logpath_key,\
            (void **) &logpath,NULL);
    if(NULL == logpath){
        SpxLog2(log,SpxLogError,err,"get logpath from config is fail.");
        return err;
    }
    err =  spx_properties_get(configurtion,\
            ydb_tracker_config_logprefix_key,\
            (void **) &logprefix,NULL);
    if(NULL == logprefix){
        SpxLog2(log,SpxLogError,err,"get logprefix from config is fail.");
        return err;
    }
    err =  spx_properties_get(configurtion,\
            ydb_tracker_config_logsize_key,\
            (void **) &logsize,NULL);
    if(NULL == logsize){
        SpxLog2(log,SpxLogError,err,"get logsize from config is fail.");
        return err;
    }
    err =  spx_properties_get(configurtion,\
            ydb_tracker_config_loglevel_key,\
            (void **) &loglevel,NULL);
    if(NULL == loglevel){
        SpxLog2(log,SpxLogError,err,"get loglevel from config is fail.");
        return err;
    }
    if(0 != ( err = spx_log_new(log,logpath,logprefix,*logsize,*loglevel))){
        SpxLog2(log,SpxLogError,err,"init the logger is fail.");
        return err;
    }

    struct ev_loop *mainloop = NULL;
    mainloop = ev_default_loop(0);
    ydb_tracker_regedit_signal(mainloop);


    int *timeout = NULL;
    err = spx_properties_get(configurtion,ydb_tracker_config_timeout_key,(void **)&timeout,NULL);
    if(0 != err){
        SpxLog2(log,SpxLogError,err,"get timeout config item is fail.");
        return err;
    }
    int *niosize = NULL;
    err = spx_properties_get(configurtion,ydb_tracker_config_niosize_key,(void **) &niosize,NULL);
    if(0 != err){
        SpxLog2(log,SpxLogError,err,"get niosize config item is fail.");
        return err;
    }

    struct spx_nio_context_pool *g_spx_nio_context_pool = \
                                                    spx_nio_context_pool_new(log,\
                                                            configurtion,\
                                                            *niosize,*timeout,\
                                                            spx_nio_reader,\
                                                            spx_nio_writer,\
                                                            ydb_tracker_nio_header_validator_handler,\
                                                            ydb_tracker_nio_header_validator_fail_handler,\
                                                            ydb_tracker_nio_request_body_handler,\
                                                            ydb_tracker_nio_response_body_handler,\
                                                            &err);
    if(NULL == g_spx_nio_context_pool){
        SpxLog2(log,SpxLogError,err,"new the nio context pool is fail.");
        return err;
    }


    int *siosize = NULL;
    err = spx_properties_get(configurtion,ydb_tracker_config_siosize_key,(void **)&siosize,NULL);
    if(0 != err){
        SpxLog2(log,SpxLogError,err,"get siosize config item is fail.");
        return err;
    }
    size_t *stacksize = NULL;
    err = spx_properties_get(configurtion,ydb_tracker_config_stacksize_key,(void **)&stacksize,NULL);
    if(0 != err){
        SpxLog2(log,SpxLogError,err,"get stacksize config item is fail.");
        return err;
    }

    err = spx_sio_init(log,*siosize,*stacksize,spx_sio_reader);
    err_t spx_sio_init(SpxLogDelegate *log,\
            size_t size,size_t stack_size,\
            SpxSioDelegate *sio_reader);

    pthread_t tid =  ydb_tracker_mainsocket_thread_new(log,configurtion,&err);
    if(NULL == tid){
        SpxLog2(log,SpxLogError,err,"create main socket thread is fail.");
        return err;
    }

    //if have maneger code please input here

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
