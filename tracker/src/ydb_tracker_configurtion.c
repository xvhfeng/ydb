/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_configurtion.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/20 17时13分16秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#include <stdlib.h>

#include "include/spx_defs.h"
#include "include/spx_types.h"
#include "include/spx_properties.h"
#include "include/spx_string.h"
#include "include/spx_alloc.h"
#include "include/spx_socket.h"

#include "ydb_tracker_configurtion.h"

spx_private string_t ip_key = NULL;
spx_private string_t port_key = NULL;
spx_private string_t timeout_key = NULL;
spx_private string_t basepath_key = NULL;
spx_private string_t logpath_key = NULL;
spx_private string_t logprefix_key = NULL;
spx_private string_t logsize_key = NULL;
spx_private string_t loglevel_key = NULL;
spx_private string_t balance_key = NULL;
spx_private string_t master_key = NULL;
spx_private string_t heartbeat_key = NULL;
spx_private string_t daemon_key  = NULL;
spx_private string_t stacksize_key = NULL;
spx_private string_t notifier_module_thread_size_key = NULL;
spx_private string_t network_module_thread_size_key = NULL;
spx_private string_t task_module_thread_size_key = NULL;
spx_private string_t context_size_key = NULL;

void *ydb_storage_config_before_handle(SpxLogDelegate *log,err_t *err){

    ip_key = spx_string_new("ip",err);
    port_key = spx_string_new("port",err);
    timeout_key = spx_string_new("timeout",err);
    basepath_key = spx_string_new("basepath",err);
    logpath_key = spx_string_new("logpath",err);
    logprefix_key = spx_string_new("logprefix",err);
    logsize_key = spx_string_new("logsize",err);
    loglevel_key = spx_string_new("loglevel",err);
    balance_key = spx_string_new("balance",err);
    master_key = spx_string_new("master",err);
    heartbeat_key = spx_string_new("heartbeat",err);
    daemon_key = spx_string_new("daemon",err);
    stacksize_key = spx_string_new("stacksize",err);
    notifier_module_thread_size_key = spx_string_new("notifier_module_thread_size",err);
    network_module_thread_size_key = spx_string_new("network_module_thread_size",err);
    task_module_thread_size_key = spx_string_new("task_module_thread_size",err);
    context_size_key = spx_string_new("context_size",err);


    struct ydb_tracker_configurtion *config = (struct ydb_tracker_configurtion *) \
                                              spx_alloc_alone(sizeof(*config),err);
    if(NULL == config){
        SpxLog2(log,SpxLogError,*err,\
                "alloc the tracker config is fail.");
        return NULL;
    }
    config->log = log;
    config->port = 1404;
    config->timeout = 30;
    config->logsize = 10 * SpxMB;
    config->loglevel = SpxLogInfo;
    config->balance = YDB_TRACKER_BALANCE_LOOP;
    config->heartbeat = 30;
    config->daemon = true;
    config->stacksize = 128 * SpxKB;
    config->network_module_thread_size = 8;
    config->notifier_module_thread_size = 4;
    config->task_module_thread_size = 4;
    config->context_size = 64;

    return config;
}


void ydb_storage_config_line_parser_handle(string_t line,void *config,err_t *err){
    struct ydb_tracker_configurtion *c = (struct ydb_tracker_configurtion *) config;
    int count = 0;
    string_t *kv = spx_string_splitlen(line,\
            spx_string_len(line),"=",sizeof("="),&count,err);
    if(NULL == kv){
        return;
    }

    spx_string_trim(*kv," ");
    if(2 == count){
        spx_string_trim(*(kv + 1)," ");
    }

    //ip
    if(0 == spx_string_casecmp_string(*kv,ip_key)){
        if(2 == count){
            if(spx_socket_is_ip(*(kv + 1))){
                c->ip =spx_string_dup(*(kv + 1),err);
                if(NULL == c->ip){
                    SpxLog2(c->log,SpxLogError,*err,\
                            "dup the ip from config operator is fail.");
                }
            } else {
                string_t ip = spx_socket_getipbyname(*(kv + 1),err);
                if(NULL == ip){
                    SpxLogFmt2(c->log,SpxLogError,*err,\
                            "get local ip by hostname:%s is fail.",*(kv + 1));
                    goto r1;
                }
                c->ip = ip;
            }
        } else{
            SpxLog1(c->log,SpxLogWarn,"use the default ip.");
            string_t ip = spx_socket_getipbyname(NULL,err);
            if(NULL == ip){
                SpxLog2(c->log,SpxLogError,*err,\
                        "get local ip by default hostname is fail.");
                goto r1;
            }
            c->ip = ip;
        }
        goto r1;
    }

    //port
    if(0 == spx_string_casecmp_string(*kv,port_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"the port is use default:%d.",c->port);
            goto r1;
        }
        i32_t port = strtol(*(kv + 1),NULL,10);
        if(ERANGE == port) {
            SpxLog1(c->log,SpxLogError,"bad the configurtion item of port.");
            goto r1;
        }
        c->port = port;
        goto r1;
    }

    //timeout
    if(0 == spx_string_casecmp_string(*kv,timeout_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"use default timeout:%d.",c->timeout);
        } else {
            u32_t timeout = strtol(*(kv + 1),NULL,10);
            if(ERANGE == timeout) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of timeout.");
            }
            c->timeout = timeout;
        }
        goto r1;
    }

    //daemon
    if(0 == spx_string_casecmp_string(*kv,daemon_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"instance use default daemon:%d.",c->daemon);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,spx_bool_desc[false])){
                c->daemon = false;
            } else if(0 == spx_string_casecmp(s,spx_bool_desc[true])){
                c->daemon = true;
            } else {
                c->daemon = true;
            }
        }
        goto r1;
    }

    //stacksize
    if(0 == spx_string_casecmp_string(*kv,stacksize_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"stacksize use default:%lld.",c->stacksize);
        } else {
            u64_t size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == c->stacksize) {
                SpxLog1(c->log,SpxLogError,\
                        "convect stasksize is fail.");
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),\
                    -2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
               c->stacksize =  size * SpxKB;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                size *= SpxKB;
            }else {
                size *= SpxKB;
            }
            spx_string_free(unit);
            c->stacksize = size;
        }
        goto r1;
    }

    //network_module_thread_size
    if(0 == spx_string_casecmp_string(*kv,network_module_thread_size_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"network module thread size use default:%d.",c->network_module_thread_size);
        } else {
            u32_t network_module_thread_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == network_module_thread_size) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of network_module_thread_size.");
                goto r1;
            }
            c->network_module_thread_size = network_module_thread_size;
        }
        goto r1;
    }

    //notifier_module_thread_size
    if(0 == spx_string_casecmp_string(*kv,notifier_module_thread_size_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "notifier module thread size use default:%d.",c->notifier_module_thread_size);
        } else {
            u32_t notifier_module_thread_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == notifier_module_thread_size) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of notifier_module_thread_size.");
                goto r1;
            }
            c->notifier_module_thread_size = notifier_module_thread_size;
        }
        goto r1;
    }

    //task_module_thread_size
    if(0 == spx_string_casecmp_string(*kv,task_module_thread_size_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "task module thread size use default:%d.",c->task_module_thread_size);
        } else {
            u32_t task_module_thread_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == task_module_thread_size) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of task_module_thread_size.");
                goto r1;
            }
            c->task_module_thread_size = task_module_thread_size;
        }
        goto r1;
    }

    //context size
    if(0 == spx_string_casecmp_string(*kv,context_size_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "context size use default:%d.",c->context_size);
        } else {
            u32_t context_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == context_size) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of context_size.");
                goto r1;
            }
            c->context_size = context_size;
        }
        goto r1;
    }
    //heartbeat
    if(0 == spx_string_casecmp_string(*kv,heartbeat_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "heartbeat use default:%d.",c->heartbeat);
        } else {
            u32_t heartbeat = strtol(*(kv + 1),NULL,10);
            if(ERANGE == heartbeat) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of heartbeat.");
                goto r1;
            }
            c->heartbeat = heartbeat;
        }
        goto r1;
    }
    //basepath
    if(0 == spx_string_casecmp_string(*kv,basepath_key)){
        if(1 == count){
            SpxLog1(c->log,SpxLogError,\
                    "bad the configurtion item of basepath.and basepath is empty.");
            goto r1;
        }
        return;
    }

    //logpath
    if(0 == spx_string_casecmp_string(*kv,logpath_key)){
        if(1 == count){
            c->logpath = spx_string_new("/opt/ydb/log/storage/",err);
            if(NULL == c->logpath){
                SpxLog2(c->log,SpxLogError,*err,\
                        "alloc default logpath is fail.");
                goto r1;
            }
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "logpath use default:%s.",c->logpath);
        }else {
            c->logpath = spx_string_dup(*(kv + 1),err);
            if(NULL == c->logpath){
                SpxLog2(c->log,SpxLogError,*err,\
                        "dup the string for logpath is fail.");
            }
        }
        goto r1;
    }

    //logprefix
    if(0 == spx_string_casecmp_string(*kv,logprefix_key)){
        if( 1 == count){
            c->logprefix = spx_string_new("ydb-storage",err);
            if(NULL == c->logprefix){
                SpxLog2(c->log,SpxLogError,*err,\
                        "alloc default logprefix is fail.");
                goto r1;
            }
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "logprefix use default:%s.",c->logprefix);
        } else {
            c->logprefix = spx_string_dup(*(kv + 1),err);
            if(NULL == c->logprefix){
                SpxLog2(c->log,SpxLogError,*err,\
                        "dup the string for logprefix is fail.");
            }
        }
        goto r1;
    }

    //logsize
    if(0 == spx_string_casecmp_string(*kv,logsize_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "logsize use default:%lld.",c->logsize);
        } else {
            u64_t size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == size) {
                SpxLog1(c->log,SpxLogError,\
                        "convect logsize is fail.");
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),-2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                c->logsize = size * SpxMB;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                size *= SpxKB;
            }else {
                size *= SpxMB;
            }
            spx_string_free(unit);
            c->logsize = size;
        }
        goto r1;
    }

    //loglevel
    if(0 == spx_string_casecmp_string(*kv,loglevel_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "loglevel use default:%s",SpxLogDesc[c->loglevel]);
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"debug")){
                c->loglevel = SpxLogDebug;
            } else if(0 == spx_string_casecmp(s,"info")){
                c->loglevel = SpxLogInfo;
            }else if(0 == spx_string_casecmp(s,"warn")){
                c->loglevel = SpxLogWarn;
            }else if(0 == spx_string_casecmp(s,"error")){
                c->loglevel = SpxLogError;
            } else {
                c->loglevel = SpxLogInfo;
            }
        }
        goto r1;
    }

    //balance
    if(0 == spx_string_casecmp_string(*kv,balance_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "mountpoint balance use default:%s",\
                    tracker_balance_mode_desc[c->balance]);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,\
                        tracker_balance_mode_desc[YDB_TRACKER_BALANCE_LOOP])){
                c->balance = YDB_TRACKER_BALANCE_LOOP;
            } else if(0 == spx_string_casecmp(s,\
                        tracker_balance_mode_desc[YDB_TRACKER_BALANCE_TURN])){
                c->balance = YDB_TRACKER_BALANCE_TURN;
            }else if(0 == spx_string_casecmp(s,\
                        tracker_balance_mode_desc[YDB_TRACKER_BALANCE_MAXDISK])){
                c->balance = YDB_TRACKER_BALANCE_MAXDISK;
            }else if(0 == spx_string_casecmp(s,\
                        tracker_balance_mode_desc[YDB_TRACKER_BALANCE_MASTER])){
                c->balance = YDB_TRACKER_BALANCE_MASTER;
            } else {
                c->balance = YDB_TRACKER_BALANCE_LOOP;
            }
        }
        goto r1;
    }

    //master
    if(0 == spx_string_casecmp_string(*kv,master_key)){
        if(1 == count){
            goto r1;
        }
        c->master = spx_string_dup(*(kv + 1),err);
        if(NULL == c->master){
            SpxLog2(c->log,SpxLogError,*err,\
                    "dup master is fail.");
        }
        goto r1;
    }

r1:
    spx_string_free_splitres(kv,count);
    return;
}

