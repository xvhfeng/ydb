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

#include "ydb_storage_configurtion.h"

struct mainsocket_thread_arg{
    SpxLogDelegate *log;
    struct ydb_storage_configurtion *c;
};

spx_private void *ydb_storage_mainsocket_create(void *arg);

pthread_t ydb_storage_mainsocket_thread_new(SpxLogDelegate *log,struct ydb_storage_configurtion *c,err_t *err){
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t ostack_size = 0;
    pthread_attr_getstacksize(&attr, &ostack_size);
    if (ostack_size != c->stacksize
            && (0 != (*err = pthread_attr_setstacksize(&attr,c->stacksize)))){
        return NULL;
    }
    struct mainsocket_thread_arg arg;
    SpxZero(arg);
    arg.log = log;
    arg.c = c;

    pthread_t tid = 0;
    if (0 !=(*err =  pthread_create(&tid, &attr, ydb_storage_mainsocket_create,
                    &arg))){
        return NULL;
    }
    return tid;
}

spx_private void *ydb_storage_mainsocket_create(void *arg){
    struct mainsocket_thread_arg *mainsocket_arg = (struct mainsocket_thread_arg *) arg;
    SpxLogDelegate *log = mainsocket_arg->log;
    struct ydb_storage_configurtion *c= mainsocket_arg->c;
    err_t err = 0;
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

    spx_socket_accept_nb(mainsocket);
r1:
    SpxClose(mainsocket);
    return NULL;
}

