/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_mainsocket.c
 *        Created:  2014/07/31 10时48分05秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <pthread.h>

#include "spx_types.h"
#include "spx_properties.h"
#include "spx_defs.h"
#include "spx_string.h"
#include "spx_socket.h"
#include "spx_io.h"
#include "spx_alloc.h"
#include "spx_socket_accept.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_mainsocket.h"


spx_private void *ydb_storage_mainsocket_create(void *arg);

struct ydb_storage_mainsocket *ydb_storage_mainsocket_thread_new(
        struct ydb_storage_configurtion *c,err_t *err){
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t ostack_size = 0;
    pthread_attr_getstacksize(&attr, &ostack_size);
    if (ostack_size != c->stacksize
            && (0 != (*err = pthread_attr_setstacksize(&attr,c->stacksize)))){
        pthread_attr_destroy(&attr);
        SpxLog2(c->log,SpxLogError,*err,
                "set stacksize is fail.");
        return NULL;
    }
    struct ydb_storage_mainsocket *mainsocket =
        (struct ydb_storage_mainsocket *) spx_alloc_alone(sizeof(*mainsocket),err);
    if(NULL == mainsocket){
        SpxLog2(c->log,SpxLogError,*err,
                "new mainsocket is fail.");
        pthread_attr_destroy(&attr);
        return NULL;
    }

    mainsocket->log = c->log;
    mainsocket->c = c;
    if (0 !=(*err =  pthread_create(&(mainsocket->tid), &attr, ydb_storage_mainsocket_create,
                    (void *) mainsocket))){
        SpxLog2(c->log,SpxLogError,*err,
                "create mainsocket thread is fail.");
        pthread_attr_destroy(&attr);
        SpxFree(mainsocket);
        return NULL;
    }
    return mainsocket;
}

spx_private void *ydb_storage_mainsocket_create(void *arg){
    SpxTypeConvert2(struct ydb_storage_mainsocket,mainsocket,arg);
    SpxTypeConvert2(struct ydb_storage_configurtion,c,mainsocket->c);
    err_t err = 0;
    mainsocket->loop = ev_loop_new(0);
    if(NULL == mainsocket->loop){
        SpxLog2(mainsocket->log,SpxLogError,err,"create main socket loop is fail.");
        return NULL;
    }
    int socket =  spx_socket_new(&err);
    if(0 >= socket){
        SpxLog2(mainsocket->log,SpxLogError,err,"create main socket is fail.");
        goto r1;
    }
    mainsocket->socket = socket;

    if(0!= (err = spx_set_nb(socket))){
        SpxLog2(mainsocket->log,SpxLogError,err,"set main socket nonblock is fail.");
        goto r1;
    }

    if(0 != (err =  spx_socket_start(socket,c->ip,c->port,\
                    true,c->timeout,\
                    3,c->timeout,\
                    false,0,\
                    true,\
                    true,c->timeout,
                    1024))){
        SpxLog2(c->log,SpxLogError,err,"start main socket is fail.");
        goto r1;
    }

    g_ydb_storage_runtime->status = YDB_STORAGE_ACCEPTING;
    spx_socket_accept_nb(c->log,mainsocket->loop,socket);
r1:
    if(NULL != mainsocket->loop){
        ev_loop_destroy(mainsocket->loop);
    }

    SpxClose(socket);
    SpxFree(mainsocket);
    return NULL;
}

