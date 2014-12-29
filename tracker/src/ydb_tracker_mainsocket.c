/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_mainsocket.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/30 18时06分04秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include <pthread.h>
#include <ev.h>

#include "spx_types.h"
#include "spx_properties.h"
#include "spx_defs.h"
#include "spx_string.h"
#include "spx_socket.h"
#include "spx_io.h"
#include "spx_log.h"
#include "spx_alloc.h"
#include "spx_socket_accept.h"

#include "ydb_tracker_configurtion.h"

struct mainsocket_thread_arg{
    SpxLogDelegate *log;
    struct ydb_tracker_configurtion *c;
};

spx_private struct ev_loop *main_socket_loop = NULL;

spx_private void *ydb_tracker_mainsocket_create(void *arg);

pthread_t ydb_tracker_mainsocket_thread_new(SpxLogDelegate *log,struct ydb_tracker_configurtion *c,err_t *err){
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t ostack_size = 0;
    pthread_attr_getstacksize(&attr, &ostack_size);
    if (ostack_size != c->stacksize
            && (0 != (*err = pthread_attr_setstacksize(&attr,c->stacksize)))){
        return 0;
    }
    struct mainsocket_thread_arg *arg =(struct mainsocket_thread_arg *) spx_alloc_alone(sizeof(*arg),err);
    if(NULL == arg){
        pthread_attr_destroy(&attr);
        return 0;
    }
    arg->log = log;
    arg->c = c;

    pthread_t tid = 0;
    if (0 !=(*err =  pthread_create(&tid, &attr, ydb_tracker_mainsocket_create,
                    arg))){
        pthread_attr_destroy(&attr);
        SpxFree(arg);
        return 0;
    }
    pthread_attr_destroy(&attr);
    return tid;
}

spx_private void *ydb_tracker_mainsocket_create(void *arg){
    struct mainsocket_thread_arg *mainsocket_arg = (struct mainsocket_thread_arg *) arg;
    SpxLogDelegate *log = mainsocket_arg->log;
    struct ydb_tracker_configurtion *c= mainsocket_arg->c;
    SpxFree(mainsocket_arg);
    err_t err = 0;
    main_socket_loop = ev_loop_new(0);
    if(NULL == main_socket_loop){
        SpxLog2(log,SpxLogError,err,"create main socket loop is fail.");
        return NULL;
    }
    int mainsocket =  spx_socket_new(&err);
    if(0 == mainsocket){
        SpxLog2(log,SpxLogError,err,"create main socket is fail.");
        return NULL;
    }

    if(0!= (err = spx_set_nb(mainsocket))){
        SpxLog2(log,SpxLogError,err,"set main socket nonblock is fail.");
        goto r1;
    }

    if(0 != (err =  spx_socket_start(mainsocket,c->ip,c->port,\
                    true,c->timeout,\
                    3,c->timeout,\
                    false,0,\
                    true,\
                    true,c->timeout,
                    1024))){
        SpxLog2(log,SpxLogError,err,"start main socket is fail.");
        goto r1;
    }

    SpxLogFmt1(log,SpxLogMark,
            "main socket fd:%d."
            "and accepting...",
            mainsocket);

    spx_socket_accept_nb(c->log,main_socket_loop,mainsocket);
r1:
    SpxClose(mainsocket);
    return NULL;
}

