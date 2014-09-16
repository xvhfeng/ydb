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

#include "spx_defs.h"
#include "spx_types.h"
#include "spx_properties.h"
#include "spx_string.h"
#include "spx_alloc.h"
#include "spx_socket.h"

#include "ydb_tracker_configurtion.h"


void *ydb_tracker_config_before_handle(SpxLogDelegate *log,err_t *err){
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


void ydb_tracker_config_line_parser_handle(string_t line,void *config,err_t *err){
    struct ydb_tracker_configurtion *c = (struct ydb_tracker_configurtion *) config;
    int count = 0;
    string_t *kv = spx_string_splitlen(line,\
            spx_string_len(line),"=",strlen("="),&count,err);
    if(NULL == kv){
        return;
    }

    spx_string_rtrim(*kv," ");
    if(2 == count){
        spx_string_ltrim(*(kv + 1)," ");
    }

    //ip
    if(0 == spx_string_casecmp(*kv,"ip")){
        if(2 == count && !SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"port")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"timeout")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"daemon")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"stacksize")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"network_module_thread_size")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"notifier_module_thread_size")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"task_module_thread_size")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"context_size")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"heartbeat")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"basepath")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
            SpxLog1(c->log,SpxLogError,\
                    "bad the configurtion item of basepath.and basepath is empty.");
            goto r1;
        }
        c->basepath = spx_string_dup(*(kv + 1),err);
        if(NULL == c->basepath){
            SpxLog2(c->log,SpxLogError,*err,\
                    "dup the string for basepath is fail.");
        }
        goto r1;
    }

    //logpath
    if(0 == spx_string_casecmp(*kv,"logpath")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
            c->logpath = spx_string_new("/opt/ydb/log/tracker/",err);
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
    if(0 == spx_string_casecmp(*kv,"logprefix")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"logsize")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
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
    if(0 == spx_string_casecmp(*kv,"loglevel")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "loglevel use default:%s",SpxLogDesc[c->loglevel]);
        } else {
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
    if(0 == spx_string_casecmp(*kv,"balance")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "mountpoint balance use default:%s",\
                    tracker_balance_mode_desc[c->balance]);
            goto r1;
        }
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
        goto r1;
    }

    //master
    if(0 == spx_string_casecmp(*kv,"master")){
        if(1 == count || SpxStringIsEmpty(*(kv + 1))){
            SpxLog1(c->log,SpxLogWarn,
                    "disable the master.");
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

