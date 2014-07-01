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

#include "include/spx_types.h"
#include "include/spx_properties.h"
#include "include/spx_defs.h"
#include "include/spx_string.h"
#include "include/spx_socket.h"
#include "include/spx_io.h"

#include "ydb_tracker_configurtion.h"

struct mainsocket_thread_arg{
    SpxLogDelegate *log;
    struct spx_properties *configurtion;
};

spx_private void *ydb_tracker_mainsocket_create(void *arg);

pthread_t ydb_tracker_mainsocket_thread_new(SpxLogDelegate *log,struct spx_properties *configurtion,err_t *err){
    size_t *stacksize = NULL;
    *err = spx_properties_get(configurtion,ydb_tracker_config_stacksize_key,(void **)&stacksize,NULL);
    if(0 != err){
        SpxLog2(log,SpxLogError,*err,"get stacksize config item is fail.");
        return NULL;
    }
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t ostack_size = 0;
    pthread_attr_getstacksize(&attr, &ostack_size);
    if (ostack_size != *stacksize
            && (0 != (*err = pthread_attr_setstacksize(&attr,*stacksize)))){
        return NULL;
    }
    struct mainsocket_thread_arg arg;
    SpxZero(arg);
    arg.log = log;
    arg.configurtion = configurtion;

    pthread_t tid = 0;
    if (0 !=(*err =  pthread_create(&tid, &attr, ydb_tracker_mainsocket_create,
                    &arg))){
        return NULL;
    }
    return tid;
}

spx_private void *ydb_tracker_mainsocket_create(void *arg){
    struct mainsocket_thread_arg *mainsocket_arg = (struct mainsocket_thread_arg *) arg;
    SpxLogDelegate *log = mainsocket_arg->log;
    struct spx_properties *configurtion = mainsocket_arg->configurtion;
    err_t err = 0;
    string_t ip = NULL;
    if(0 != (err = spx_properties_get(configurtion,ydb_tracker_config_ip_key,(void **) &ip,NULL))){
        SpxLog2(log,SpxLogError,err,"get ip config item is fail.");
        return NULL;
    }
    int *port = NULL;
    if(0 != (err = spx_properties_get(configurtion,ydb_tracker_config_port_key,(void **) &port,NULL))){
        SpxLog2(log,SpxLogError,err,"get port config item is fail.");
        return NULL;
    }

    int *timeout = NULL;
    err = spx_properties_get(configurtion,ydb_tracker_config_timeout_key,(void **)&timeout,NULL);
    if(0 != err){
        SpxLog2(log,SpxLogError,err,"get timeout config item is fail.");
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

    if(0 != (err =  spx_socket_start(mainsocket,ip,*port,\
                    true,*timeout,\
                    3,*timeout,\
                    false,0,\
                    true,\
                    true,*timeout,
                    1024))){
        SpxLog2(log,SpxLogError,err,"start main socket is fail.");
        goto r1;
    }

    spx_socket_accept_nb(mainsocket);
r1:
    SpxClose(mainsocket);
    return NULL;
}
