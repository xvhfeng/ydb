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


string_t ydb_tracker_config_ip_key = NULL;
string_t ydb_tracker_config_port_key = NULL;
string_t ydb_tracker_config_timeout_key = NULL;
string_t ydb_tracker_config_basepath_key = NULL;
string_t ydb_tracker_config_logpath_key = NULL;
string_t ydb_tracker_config_logprefix_key = NULL;
string_t ydb_tracker_config_logsize_key = NULL;
string_t ydb_tracker_config_loglevel_key = NULL;
string_t ydb_tracker_config_balance_key = NULL;
string_t ydb_tracker_config_master_key = NULL;
string_t ydb_tracker_config_heartbeat_key = NULL;
string_t ydb_tracker_config_daemon_key = NULL;
string_t ydb_tracker_config_niosize_key = NULL;
string_t ydb_tracker_config_siosize_key =NULL ;
string_t ydb_tracker_config_stacksize_key = NULL;


spx_private string_t tracker_default_ip = NULL;
spx_private int tracker_default_timeout = 30;
spx_private string_t tracker_default_logpath = NULL;
spx_private string_t tracker_default_logprefix = NULL;
spx_private int tracker_default_loglevel = SpxLogInfo;
spx_private u64_t tracker_default_logsize = 10;
spx_private int tracker_default_balance = YDB_TRACKER_BALANCE_LOOP;
spx_private int tracker_default_heartbeat = 30;
spx_private bool_t tracker_default_daemon = false;
spx_private int tracker_default_siosize = 16;
spx_private int tracker_default_niosize = 16;
spx_private int tracker_default_stacksize = 128;

err_t ydb_tracker_config_parser_before_handle(){
    err_t err = 0;
    tracker_default_ip = spx_string_empty(&err);
    tracker_default_timeout = 30;
    tracker_default_heartbeat = 30;
    tracker_default_logpath = spx_string_new("/opt/ydb/log/tracker/",&err);
    tracker_default_loglevel = SpxLogInfo;
    tracker_default_logprefix = spx_string_new("log",&err);
    tracker_default_logsize = 10;
    tracker_default_balance = YDB_TRACKER_BALANCE_LOOP;
    tracker_default_daemon = false;
    tracker_default_siosize = 16;
    tracker_default_niosize = 16;
    tracker_default_stacksize = 128;


    ydb_tracker_config_ip_key = spx_string_new("ip",&err);
    ydb_tracker_config_port_key = spx_string_new("port",&err);
    ydb_tracker_config_timeout_key = spx_string_new("timeout",&err);
    ydb_tracker_config_basepath_key = spx_string_new("basepath",&err);
    ydb_tracker_config_logpath_key = spx_string_new("logpath",&err);
    ydb_tracker_config_logprefix_key = spx_string_new("logprefix",&err);
    ydb_tracker_config_logsize_key = spx_string_new("logsize",&err);
    ydb_tracker_config_loglevel_key = spx_string_new("loglevel",&err);
    ydb_tracker_config_balance_key = spx_string_new("balance",&err);
    ydb_tracker_config_master_key = spx_string_new("master",&err);
    ydb_tracker_config_heartbeat_key = spx_string_new("heartbeat",&err);
    ydb_tracker_config_daemon_key = spx_string_new("daemon",&err);
    ydb_tracker_config_siosize_key = spx_string_new("siosize",&err);
    ydb_tracker_config_niosize_key = spx_string_new("niosize",&err);
    ydb_tracker_config_stacksize_key = spx_string_new("stacksize",&err);

    return err;
}


void ydb_tracker_config_line_deserialize(string_t line,struct spx_properties *p,err_t *err){
  int count = 0;
    string_t *kv = spx_string_splitlen(line,spx_string_len(line),"=",sizeof("="),&count,err);
    if(NULL == kv){
        return;
    }

    spx_string_trim(*kv," ");
    if(2 == count){
        spx_string_trim(*(kv + 1)," ");
    }

    //ip
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_ip_key)){
        if(2 == count){
            *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        } else{
            SpxLog1(p->log,SpxLogWarn,"use the default ip.");
            spx_string_free_splitres(kv,count);
        }
        return;
    }

    //port
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_port_key)){
        if(2 != count){
            *err = EINVAL;
            SpxLog1(p->log,SpxLogError,"bad the configurtion item of port.the port is empty.");
            goto r1;
        }
        int *port = spx_alloc_alone(sizeof(int),err);
        if(NULL == port) goto r1;
        *port = strtol(*(kv + 1),NULL,10);
        if(ERANGE == *port) {
            SpxLog1(p->log,SpxLogError,"bad the configurtion item of port.");
            SpxFree(port);
            goto r1;
        }
        *err = spx_properties_set(p,ydb_tracker_config_port_key, port,sizeof(int));
        if(0 != *err){
            SpxFree(port);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //timeout
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_timeout_key)){
        int *timeout = spx_alloc_alone(sizeof(int),err);
        if(NULL == timeout) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the timeout default is 30s.");
            *timeout = tracker_default_timeout;
        } else {
            *timeout = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *timeout) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of timeout.");
                SpxFree(timeout);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_tracker_config_timeout_key, timeout,sizeof(int));
        if(0 != *err){
            SpxFree(timeout);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //daemon
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_daemon_key)){
        bool_t *is_daemon = spx_alloc_alone(sizeof(bool_t),err);
        if(NULL == is_daemon) goto r1;
        if(1 == count){
            *is_daemon = tracker_default_daemon;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"false")){
                *is_daemon = false;
            } else if(0 == spx_string_casecmp(s,"true")){
                *is_daemon = true;
            } else {
                *is_daemon = false;
            }
        }
        *err = spx_properties_set(p,ydb_tracker_config_daemon_key, is_daemon,sizeof(bool_t));
        if(0 != *err){
            SpxFree(is_daemon);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //stacksize
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_stacksize_key)){
        u64_t *size = spx_alloc_alone(sizeof(u64_t),err);
        if(NULL == size) goto r1;
        if(1 == count){
            *size = tracker_default_stacksize * SpxKB;
        } else {
            *size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == *size) {
                SpxFree(size);
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),-2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                SpxFree(size);
                goto r1;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                *size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                *size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                *size *= SpxKB;
            }else {
                *size *= SpxMB;
            }
            spx_string_free(unit);
        }
        *err = spx_properties_set(p,ydb_tracker_config_stacksize_key, size,sizeof(u64_t));
        if(0 != *err){
            SpxFree(size);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //niosize
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_niosize_key)){
        int *niosize = spx_alloc_alone(sizeof(int),err);
        if(NULL == niosize) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the niosize default is 16.");
            *niosize = tracker_default_niosize;
        } else {
            *niosize = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *niosize) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of niosize.");
                SpxFree(niosize);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_tracker_config_niosize_key, niosize,sizeof(int));
        if(0 != *err){
            SpxFree(niosize);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //siosize
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_siosize_key)){
        int *siosize = spx_alloc_alone(sizeof(int),err);
        if(NULL == siosize) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the siosize default is 16.");
            *siosize = tracker_default_siosize;
        } else {
            *siosize = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *siosize) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of siosize.");
                SpxFree(siosize);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_tracker_config_siosize_key, siosize,sizeof(int));
        if(0 != *err){
            SpxFree(siosize);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //heartbeat
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_heartbeat_key)){
        int *heartbeat = spx_alloc_alone(sizeof(int),err);
        if(NULL == heartbeat) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the heartbeat default is 30s.");
            *heartbeat = tracker_default_heartbeat;
        } else {
            *heartbeat = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *heartbeat) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of heartbeat.");
                SpxFree(heartbeat);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_tracker_config_heartbeat_key, heartbeat,sizeof(int));
        if(0 != *err){
            SpxFree(heartbeat);
        }
        spx_string_free_splitres(kv,count);
        return;
    }
    //basepath
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_basepath_key)){
        if(1 == count){
            *err = EINVAL;
            SpxLog1(p->log,SpxLogError,"bad the configurtion item of basepath.and basepath is empty.");
            goto r1;
        }
        *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        return;
    }

    //logpath
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_logpath_key)){
        if(1 == count){
            *err = spx_properties_set(p,*kv, tracker_default_logpath,spx_string_len(tracker_default_logpath));
            spx_string_free_splitres(kv,count);
        }else {
            *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        }
        return;
    }

    //logprefix
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_logprefix_key)){
        if( 1 == count){
            *err = spx_properties_set(p,*kv,tracker_default_logprefix,spx_string_len(tracker_default_logprefix));
            spx_string_free_splitres(kv,count);
        } else {
            *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        }
        return;
    }

    //logsize
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_logsize_key)){
        u64_t *size = spx_alloc_alone(sizeof(u64_t),err);
        if(NULL == size) goto r1;
        if(1 == count){
            *size = tracker_default_logsize * SpxMB;
        } else {
            *size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == *size) {
                SpxFree(size);
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),-2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                SpxFree(size);
                goto r1;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                *size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                *size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                *size *= SpxKB;
            }else {
                *size *= SpxMB;
            }
            spx_string_free(unit);
        }
        *err = spx_properties_set(p,ydb_tracker_config_logsize_key, size,sizeof(u64_t));
        if(0 != *err){
            SpxFree(size);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //loglevel
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_loglevel_key)){
        int *level = spx_alloc_alone(sizeof(int),err);
        if(NULL == level) goto r1;
        if(1 == count){
            *level = tracker_default_loglevel;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"debug")){
                *level = SpxLogDebug;
            } else if(0 == spx_string_casecmp(s,"info")){
                *level = SpxLogInfo;
            }else if(0 == spx_string_casecmp(s,"warn")){
                *level = SpxLogWarn;
            }else if(0 == spx_string_casecmp(s,"error")){
                *level = SpxLogError;
            } else {
                *level = tracker_default_loglevel;
            }
        }
        *err = spx_properties_set(p,ydb_tracker_config_loglevel_key, level,sizeof(int));
        if(0 != *err){
            SpxFree(level);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //balance
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_balance_key)){
        int *balance = spx_alloc_alone(sizeof(int),err);
        if(NULL == balance) goto r1;
        if(1 == count){
            *balance = YDB_TRACKER_BALANCE_LOOP;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"loop")){
                *balance = YDB_TRACKER_BALANCE_LOOP;
            } else if(0 == spx_string_casecmp(s,"maxdisk")){
                *balance = YDB_TRACKER_BALANCE_MAXDISK;
            }else if(0 == spx_string_casecmp(s,"turn")){
                *balance = YDB_TRACKER_BALANCE_TURN;
            }else if(0 == spx_string_casecmp(s,"master")){
                *balance = YDB_TRACKER_BALANCE_MASTER;
            } else {
                *balance = YDB_TRACKER_BALANCE_LOOP;
            }
        }
        *err = spx_properties_set(p,ydb_tracker_config_balance_key, balance,sizeof(int));
        if(0 != *err){
            SpxFree(balance);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //master
    if(0 == spx_string_casecmp_string(*kv,ydb_tracker_config_master_key)){
        if(1 == count){
            goto r1;
        }
        *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        return;
    }
r1:
    spx_string_free_splitres(kv,count);
    return;
}

