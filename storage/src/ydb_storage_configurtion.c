/*
 * =====================================================================================
 *
 *       Filename:  ydb_storage_configurtion.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/07/01 15时40分40秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include <string.h>

#include "include/spx_types.h"
#include "include/spx_defs.h"
#include "include/spx_string.h"
#include "include/spx_alloc.h"
#include "include/spx_list.h"
#include "include/spx_path.h"
#include "include/spx_socket.h"
#include "include/spx_time.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"

spx_private string_t ydb_storage_config_ip_key = NULL;
spx_private string_t ydb_storage_config_port_key = NULL;
spx_private string_t ydb_storage_config_timeout_key = NULL;
spx_private string_t ydb_storage_config_basepath_key = NULL;
spx_private string_t ydb_storage_config_logpath_key = NULL;
spx_private string_t ydb_storage_config_logprefix_key = NULL;
spx_private string_t ydb_storage_config_logsize_key = NULL;
spx_private string_t ydb_storage_config_loglevel_key = NULL;
spx_private string_t ydb_storage_config_balance_key = NULL;
spx_private string_t ydb_storage_config_master_key = NULL;
spx_private string_t ydb_storage_config_heartbeat_key = NULL;
spx_private string_t ydb_storage_config_daemon_key  = NULL;
spx_private string_t ydb_storage_config_nio_thread_size_key = NULL;
spx_private string_t ydb_storage_config_nio_context_size_key = NULL;
spx_private string_t ydb_storage_config_sio_thread_size_key = NULL;
spx_private string_t ydb_storage_config_stacksize_key = NULL;
spx_private string_t ydb_storage_config_groupname_key = NULL;
spx_private string_t ydb_storage_config_machineid_key = NULL;
spx_private string_t ydb_storage_config_dio_thread_size_key = NULL;
spx_private string_t ydb_storage_config_dio_context_size_key = NULL;
spx_private string_t ydb_storage_config_freedisk_key = NULL;
spx_private string_t ydb_storage_config_mountpoint_key = NULL;
spx_private string_t ydb_storage_config_tracker_key = NULL;
spx_private string_t ydb_storage_config_storerooms_key = NULL;
spx_private string_t ydb_storage_config_storemode_key = NULL;
spx_private string_t ydb_storage_config_storecount_key = NULL;
spx_private string_t ydb_storage_config_fillchunk_key = NULL;
spx_private string_t ydb_storage_config_compress_key = NULL;
spx_private string_t ydb_storage_config_chunkfile_key = NULL;
spx_private string_t ydb_storage_config_chunksize_key = NULL;
spx_private string_t ydb_storage_config_overload_key = NULL;
spx_private string_t ydb_storage_config_oversize_key = NULL;
    //this operator is not config and from the oversize
spx_private string_t ydb_storage_config_overmode_key = NULL;
spx_private string_t ydb_storage_config_singlemin_key = NULL;
spx_private string_t ydb_storage_config_lazyrecv_key = NULL;
spx_private string_t ydb_storage_config_lazysize_key = NULL;
spx_private string_t ydb_storage_config_sendfile_key = NULL;
spx_private string_t ydb_storage_config_holerefresh_key = NULL;
spx_private string_t ydb_storage_config_refreshtime_key = NULL;

int ydb_storage_status = YDB_STORAGE_NORMAL;
u64_t ydb_storage_first_start = 0;

spx_private void *ydb_mountpoint_new(size_t i,void *arg,err_t *err);
spx_private err_t ydb_mountpoint_free(void **arg);
spx_private err_t ydb_tracker_free(void **arg);
spx_private u64_t ydb_storage_hole_idx_refresh_timeout(\
        int hour,int min,int sec,err_t *err);

spx_private void *ydb_mountpoint_new(size_t i,void *arg,err_t *err){
    struct ydb_storage_mountpoint *mp = spx_alloc_alone(sizeof(*mp),err);
    if(NULL == mp){
        return NULL;
    }
    string_t path = (string_t) arg;
    mp->path = spx_string_dup(path,err);
    if(NULL == mp->path){
        SpxFree(mp);
        return NULL;
    }
    mp->freesize = spx_mountpoint_freesize(path,err);
    if(0 != *err){
        spx_string_free(mp->path);
        SpxFree(mp);
        return NULL;
    }
    mp->disksize = spx_mountpoint_size(path,err);
    if(0 != *err){
        spx_string_free(mp->path);
        SpxFree(mp);
        return NULL;
    }
    mp->idx = i;
    return mp;
}

spx_private err_t ydb_mountpoint_free(void **arg){
    if(NULL == arg || NULL == *arg){
        return 0;
    }
    struct ydb_storage_mountpoint **mp = (struct ydb_storage_mountpoint **)arg;

    if(NULL != (*mp)->path){
        spx_string_free((*mp)->path);
    }
    SpxFree(*mp);
    return 0;
}

spx_private err_t ydb_tracker_free(void **arg){
    struct spx_host **host = (struct spx_host **) arg;
    SpxFree(*host);
    return 0;
}


spx_private u64_t ydb_storage_hole_idx_refresh_timeout(\
        int hour,int min,int sec,err_t *err){
    struct spx_datetime dt;
    SpxZero(dt);
    spx_get_curr_datetime(&dt);
    struct spx_datetime *new_dt = spx_datetime_dup(&dt,err);
    if(NULL == new_dt){
        return 0;
    }
    SpxHour(new_dt) = hour;
    SpxMinute(new_dt) = min;
    SpxSecond(new_dt) = sec;
    time_t fixed_time = spx_mktime(new_dt);
    i64_t secs = fixed_time - spx_now();
    if(0 > secs){// the current time is run over than the fixed time point
        new_dt = spx_datetime_add_days(new_dt,1);
        secs = spx_mktime(new_dt) - spx_now();
    }
    SpxFree(new_dt);
    return secs;
}

void *ydb_storage_config_before_handle(SpxLogDelegate *log,err_t *err){

    ydb_storage_config_ip_key = spx_string_new("ip",err);
    ydb_storage_config_port_key = spx_string_new("port",err);
    ydb_storage_config_timeout_key = spx_string_new("timeout",err);
    ydb_storage_config_basepath_key = spx_string_new("basepath",err);
    ydb_storage_config_logpath_key = spx_string_new("logpath",err);
    ydb_storage_config_logprefix_key = spx_string_new("logprefix",err);
    ydb_storage_config_logsize_key = spx_string_new("logsize",err);
    ydb_storage_config_loglevel_key = spx_string_new("loglevel",err);
    ydb_storage_config_balance_key = spx_string_new("balance",err);
    ydb_storage_config_master_key = spx_string_new("master",err);
    ydb_storage_config_heartbeat_key = spx_string_new("heartbeat",err);
    ydb_storage_config_daemon_key = spx_string_new("daemon",err);
    ydb_storage_config_stacksize_key = spx_string_new("stacksize",err);
    ydb_storage_config_groupname_key = spx_string_new("groupname",err);
    ydb_storage_config_machineid_key = spx_string_new("machineid",err);
    ydb_storage_config_nio_thread_size_key = spx_string_new("nio_thread_size",err);
    ydb_storage_config_nio_context_size_key = spx_string_new("nio_context_size",err);
    ydb_storage_config_sio_thread_size_key = spx_string_new("sio_thread_size",err);
    ydb_storage_config_dio_thread_size_key = spx_string_new("dio_thread_size",err);
    ydb_storage_config_dio_context_size_key = spx_string_new("dio_context_size",err);
    ydb_storage_config_freedisk_key = spx_string_new("freedisk",err);
    ydb_storage_config_mountpoint_key = spx_string_new("mp",err);
    ydb_storage_config_tracker_key = spx_string_new("tracker",err);
    ydb_storage_config_storerooms_key = spx_string_new("storerooms",err);
    ydb_storage_config_storemode_key = spx_string_new("storemode",err);
    ydb_storage_config_storecount_key = spx_string_new("storecount",err);
    ydb_storage_config_fillchunk_key = spx_string_new("fillchunk",err);
    ydb_storage_config_compress_key = spx_string_new("compress",err);
    ydb_storage_config_chunkfile_key = spx_string_new("chunkfile",err);
    ydb_storage_config_chunksize_key = spx_string_new("chunksize",err);
    ydb_storage_config_overload_key = spx_string_new("overload",err);
    ydb_storage_config_oversize_key = spx_string_new("oversize",err);
    ydb_storage_config_overmode_key = spx_string_new("overmode",err);
    ydb_storage_config_singlemin_key = spx_string_new("singlemin",err);
    ydb_storage_config_lazyrecv_key = spx_string_new("lazyrecv",err);
    ydb_storage_config_lazysize_key = spx_string_new("lazysize",err);
    ydb_storage_config_sendfile_key = spx_string_new("sendfile",err);
    ydb_storage_config_holerefresh_key = spx_string_new("holerefresh",err);
    ydb_storage_config_refreshtime_key = spx_string_new("refreshtime",err);


    struct ydb_storage_configurtion *config = (struct ydb_storage_configurtion *) \
                                              spx_alloc_alone(sizeof(*config),err);
    if(NULL == config){
        SpxLog2(log,SpxLogError,*err,\
                "alloc the storage config is fail.");
        return NULL;
    }
    config->log = log;
    config->port = 8175;
    config->timeout = 30;
    config->logsize = 10 * SpxMB;
    config->loglevel = SpxLogInfo;
    config->balance = YDB_STORAGE_MOUNTPOINT_LOOP;
    config->heartbeat = 30;
    config->daemon = true;
    config->nio_thread_size = 16;
    config->nio_context_size = 256;
    config->sio_thread_size = 8;
    config->stacksize = 128 * SpxKB;
    config->dio_thread_size = 8;
    config->dio_context_size = 256;
    config->freedisk = 4 * SpxGB;
    config->mountpoints = spx_list_new(log,\
            YDB_STORAGE_MOUNTPOINT_COUNT,ydb_mountpoint_free,err);
    config->trackers = spx_vector_init(log,\
            ydb_tracker_free,err);
    config->storerooms = 256;
    config->storemode = YDB_STORAGE_STOREMODE_TURN;
    config->storecount = 4096;
    config->fillchunk = true;
    config->holerefresh = YDB_STORAGE_HOLEREFRESH_FIXEDTIME;
    config->refreshtime = ydb_storage_hole_idx_refresh_timeout(6,0,0,err);

    config->compress = false;
    config->chunkfile = true;
    config->chunksize = 64 * SpxMB;
    config->overload = true;
    config->oversize = 10;
    config->overmode = YDB_STORAGE_OVERMODE_RELATIVE;
    config->singlemin = 10 * SpxKB;
    config->lazyrecv = true;
    config->lazysize = 1 * SpxMB;
    config->sendfile = true;

    return config;
}


void ydb_storage_config_line_parser(string_t line,void *config,err_t *err){
    struct ydb_storage_configurtion *c = (struct ydb_storage_configurtion *) config;
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_ip_key)){
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_port_key)){
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_timeout_key)){
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_daemon_key)){
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_stacksize_key)){
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

    //nio_thread_size
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_nio_thread_size_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"nio_thread_size use default:%d.",c->nio_thread_size);
        } else {
            u32_t nio_thread_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == nio_thread_size) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of nio_thread_size.");
                goto r1;
            }
            c->nio_thread_size = nio_thread_size;
        }
        goto r1;
    }

    //nio_context_size
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_nio_context_size_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"nio_context_size use default:%d.",c->nio_context_size);
        } else {
            u32_t nio_context_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == nio_context_size) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of nio_context_size.");
                goto r1;
            }
            c->nio_context_size = nio_context_size;
        }
        goto r1;
    }

    //siosize
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_sio_thread_size_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "sio_thread_size use default:%d.",c->sio_thread_size);
        } else {
            u32_t sio_thread_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == sio_thread_size) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of sio_thread_size.");
                goto r1;
            }
            c->sio_thread_size = sio_thread_size;
        }
        goto r1;
    }

    //heartbeat
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_heartbeat_key)){
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_basepath_key)){
        if(1 == count){
            SpxLog1(c->log,SpxLogError,"bad the configurtion item of basepath.and basepath is empty.");
            goto r1;
        }
        return;
    }

    //logpath
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_logpath_key)){
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_logprefix_key)){
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_logsize_key)){
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_loglevel_key)){
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
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_balance_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "mountpoint balance use default:%s",\
                    mountpoint_balance_mode_desc[c->balance]);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"loop")){
                c->balance = YDB_STORAGE_MOUNTPOINT_LOOP;
            } else if(0 == spx_string_casecmp(s,"maxdisk")){
                c->balance = YDB_STORAGE_MOUNTPOINT_MAXSIZE;
            }else if(0 == spx_string_casecmp(s,"turn")){
                c->balance = YDB_STORAGE_MOUNTPOINT_TURN;
            }else if(0 == spx_string_casecmp(s,"master")){
                c->balance = YDB_STORAGE_MOUNTPOINT_MASTER;
            } else {
                c->balance = YDB_STORAGE_MOUNTPOINT_LOOP;
            }
        }
        goto r1;
    }

    //master
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_master_key)){
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

    //groupname
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_groupname_key)){
        if(1 == count){
            *err = EINVAL;
            SpxLog1(c->log,SpxLogError,\
                    "bad the configurtion item of groupname.and groupname is empty.");
            goto r1;
        }
        c->groupname = spx_string_dup(*(kv + 1),err);
        if(NULL == c->groupname){
            SpxLog2(c->log,SpxLogError,*err,\
                    "dup the groupname is fail.");
        }
        goto r1;
    }

    //machineid
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_machineid_key)){
        if(1 == count){
            *err = EINVAL;
            SpxLog1(c->log,SpxLogError,\
                    "bad the configurtion item of machineid.and machineid is empty.");
            goto r1;
        }
        c->machineid = spx_string_dup(*(kv + 1),err);
        if(NULL == c->machineid){
            SpxLog2(c->log,SpxLogError,*err,\
                    "dup the machineid is fail.");
        }
        goto r1;
    }

    //dio_thread_size
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_dio_thread_size_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"dio_thread_size use default:%d.",c->dio_thread_size);
        } else {
            u32_t dio_thread_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == dio_thread_size) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of dio_thread_size.");
                goto r1;
            }
            c->dio_thread_size = dio_thread_size;
        }
        goto r1;
    }

    //dio_context_size
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_dio_context_size_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"dio_context_size use default:%d.",c->dio_context_size);
        } else {
            u32_t dio_context_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == dio_context_size) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of dio_context_size.");
                goto r1;
            }
            c->dio_context_size = dio_context_size;
        }
        goto r1;
    }

    //freedisk
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_freedisk_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "freedisk use default:%lld.",c->freedisk);
        } else {
            u64_t size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == size) {
                SpxLog2(c->log,SpxLogError,*err,\
                        "concevt freedisk is fail.");
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),\
                    -2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                c->freedisk = size * SpxMB;
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
            c->freedisk = size;
        }
        goto r1;
    }

    //mount point
    if(spx_string_begin_casewith_string(*kv,ydb_storage_config_mountpoint_key)){
        int idx = strtol((*kv) + spx_string_len(ydb_storage_config_mountpoint_key),\
                NULL,16);

        struct ydb_storage_mountpoint *mp = (struct ydb_storage_mountpoint *) \
                                             ydb_mountpoint_new(idx,*(kv+1),err);
        if(NULL == mp){
            return;
        }
        spx_list_insert(c->mountpoints,idx,mp);
        goto r1;
    }

    //tracker
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_tracker_key)){
        int sum = 0;
        string_t *kv1 = spx_string_splitlen(*(kv + 1),\
                spx_string_len(*(kv + 1)),":",sizeof(":"),&sum,err);
        struct spx_host *tracker = spx_alloc_alone(sizeof(*tracker),err);
        if(NULL == tracker){
            spx_string_free_splitres(kv1,sum);
            goto r1;
        }
        if(spx_socket_is_ip(*kv)){
            tracker->ip = spx_string_dup(*kv,err);
            if(NULL == tracker->ip){
                SpxLog2(c->log,SpxLogError,*err,\
                        "dup tracker ip is fail.");
            }
        }else {
            tracker->ip = spx_socket_getipbyname(*kv,err);
            if(NULL == tracker->ip){
                SpxLogFmt2(c->log,SpxLogError,*err,\
                        "get tracker ip by hostname:%s is fail.",\
                        *kv);
            }
        }
        tracker->port = strtol(*(kv1 + 1),NULL,10);
        spx_vector_add(c->trackers,tracker);
        spx_string_free_splitres(kv1,sum);
        goto r1;
    }

    //storepaths
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_storerooms_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"storerooms use default:%d.",c->storerooms);
        } else {
            i32_t storerooms = strtol(*(kv + 1),NULL,10);
            if(ERANGE == storerooms) {
                SpxLog1(c->log,SpxLogError,"bad the configurtion item of storerooms.");
                goto r1;
            }
            c->storerooms = storerooms;
        }
        goto r1;
    }

    //storemode
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_storemode_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "storemode use default:%s.",\
                    storemode_desc[c->storemode]);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"loop")){
                c->storemode = YDB_STORAGE_STOREMODE_LOOP;
            }else if(0 == spx_string_casecmp(s,"turn")){
                c->storemode = YDB_STORAGE_STOREMODE_TURN;
            } else {
                c->storemode = YDB_STORAGE_STOREMODE_LOOP;
            }
        }
        goto r1;
    }

    //storecount
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_storecount_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "storecount use default:%d.",c->storecount);
        } else {
            u32_t storecount = strtol(*(kv + 1),NULL,10);
            if(ERANGE == storecount) {
                SpxLog2(c->log,SpxLogError,*err,\
                        "convect storecount is fail.");
                goto r1;
            }
            c->storecount = storecount;
        }
        goto r1;
    }

    //fillchunk
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_fillchunk_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "fillchunk use default:%s.",spx_bool_desc[c->fillchunk]);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,spx_bool_desc[false])){
                c->fillchunk = false;
            } else if(0 == spx_string_casecmp(s,spx_bool_desc[true])){
                c->fillchunk = true;
            } else {
                c->fillchunk = true;
            }
        }
        goto r1;
    }

    //compress
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_compress_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "compress use default:%s.",spx_bool_desc[c->compress]);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,spx_bool_desc[false])){
                c->compress = false;
            } else if(0 == spx_string_casecmp(s,spx_bool_desc[true])){
                c->compress = true;
            } else {
                c->compress = false;
            }
        }
        if(c->compress){
#ifndef YDB_STORAGE_CONTEXT_COMPRESS
#define YDB_STORAGE_CONTEXT_COMPRESS
#endif
        }else {
#ifdef YDB_STORAGE_CONTEXT_COMPRESS
#undef YDB_STORAGE_CONTEXT_CONPRESS
#endif
        }
        goto r1;
    }

    //chunkfile
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_chunkfile_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "chunkfile use default:%s.",spx_bool_desc[c->chunkfile]);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,spx_bool_desc[false])){
                c->chunkfile = false;
            } else if(0 == spx_string_casecmp(s,spx_bool_desc[true])){
                c->chunkfile = true;
            } else {
                c->chunkfile = true;
            }
        }
        goto r1;
    }

    //chunksize
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_chunksize_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "chunksize use default:%lld.",c->chunksize);
        } else {
            u64_t size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == size) {
                SpxLog2(c->log,SpxLogError,*err,\
                        "convect chunksize is fail.");
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),\
                    -2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                c->chunksize = size * SpxMB;
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
            c->chunksize = size;
        }
        goto r1;
    }

    //overload
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_overload_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "overload use default:%s/",\
                    spx_bool_desc[c->overload]);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,spx_bool_desc[false])){
                c->overload = false;
            } else if(0 == spx_string_casecmp(s,spx_bool_desc[true])){
                c->overload = true;
            } else {
                c->overload = true;
            }
        }
        goto r1;
    }

    //oversize & overmode
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_oversize_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "oversize use default:%s,and ovvermode use default:%s too",\
                    c->oversize,overmode_desc[c->overmode]);
        } else {
            if(SpxStringEndWith(*(kv + 1),'%')){
                c->overmode = YDB_STORAGE_OVERMODE_RELATIVE;
            } else {
                c->overmode = YDB_STORAGE_OVERMODE_ABSSOLUTE;
            }

            u64_t size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == size) {
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),\
                    -2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                c->oversize = size * SpxKB;
                goto r1;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                size *= SpxKB;
            }
            spx_string_free(unit);
            c->oversize = size;
        }
        goto r1;
    }

    //singlemin
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_singlemin_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "single min use default:%lld.",c->singlemin);
        } else {
            u64_t size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == size) {
                SpxLog1(c->log,SpxLogError,\
                        "convect singlemin is fail.");
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),\
                    -2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                c->singlemin = size * SpxMB;
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
            c->singlemin = size;
        }
        goto r1;
    }

    //lazyrecv
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_lazyrecv_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "lazyrecv use default:%s.",spx_bool_desc[c->lazyrecv]);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,spx_bool_desc[false])){
                c->lazyrecv = false;
            } else if(0 == spx_string_casecmp(s,spx_bool_desc[true])){
                c->lazyrecv = true;
            } else {
                c->lazyrecv = false;
            }
        }
        goto r1;
    }


    //lazysize
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_lazysize_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "single min use default:%lld.",c->lazysize);
        } else {
            u64_t size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == size) {
                SpxLog1(c->log,SpxLogError,\
                        "convect singlemin is fail.");
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),\
                    -2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                c->lazysize = size * SpxMB;
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
            c->lazysize = size;
        }
        goto r1;
    }

    //sendfile
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_sendfile_key)){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "sendfile use default:%s.",spx_bool_desc[c->sendfile]);
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,spx_bool_desc[false])){
                c->sendfile = false;
            } else if(0 == spx_string_casecmp(s,spx_bool_desc[true])){
                c->sendfile = true;
            } else {
                c->sendfile = false;
            }
        }
        goto r1;
    }

    //holerefresh
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_holerefresh_key)){
        if(1 ==  count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "holerefresh use default:%s.",\
                    holerefresh_mode_desc[c->holerefresh]);
        }
        string_t s = *(kv + 1);
        if(0 == spx_string_casecmp(s,\
                    holerefresh_mode_desc[YDB_STORAGE_HOLEREFRESH_FIXEDTIME])){
            c->holerefresh = YDB_STORAGE_HOLEREFRESH_FIXEDTIME;
        } else if(0 == spx_string_casecmp(s,\
                    holerefresh_mode_desc[YDB_STORAGE_HOLEREFRESH_TIMESTAMPS])){
            c->holerefresh = YDB_STORAGE_HOLEREFRESH_TIMESTAMPS;
        } else {
            c->holerefresh = YDB_STORAGE_HOLEREFRESH_FIXEDTIME;
        }
        goto r1;
    }

    //refreshtime
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_refreshtime_key)){
        string_t s = *(kv + 1);
        int sum = 0;
        if(spx_string_exist(s,':')){
            string_t *kv1 = spx_string_splitlen(s,
                    spx_string_len(s),":",sizeof(":"),&sum,err);
            if(sum <= 0){
                SpxLog1(c->log,SpxLogError,\
                        "parser fixed time is fail.");
                goto r1;
            }
            int hour = 0;
            int min = 0;
            hour = strtol(*kv1,NULL,10);
            if(ERANGE == hour){
                SpxLog1(c->log,SpxLogError,\
                        "convect hour is fail.");
                spx_string_free_splitres(kv1,sum);
                goto r1;
            }

            if(sum >= 2){
                min = strtol(*(kv + 1),NULL,10);
                if(ERANGE == min){
                    SpxLog1(c->log,SpxLogError,\
                            "convect minute is fail.");
                }
                spx_string_free_splitres(kv1,sum);
                goto r1;
            }
            u64_t time = ydb_storage_hole_idx_refresh_timeout(hour,min,0,err);
            if(0 != err){
                SpxLog2(c->log,SpxLogError,*err,\
                        "convect fixed time to timestamp is fail.");
                spx_string_free_splitres(kv1,sum);
                goto r1;
            }
            c->refreshtime = time;
            spx_string_free_splitres(kv1,sum);
            goto r1;
        }else{
            u64_t timestamp  = strtoul(s,NULL,10);
            if(ERANGE == timestamp){
                SpxLog1(c->log,SpxLogError,\
                        "convect refresh time is fail.");
            }
            c->refreshtime = timestamp;
        }
        goto r1;
    }

r1:
    spx_string_free_splitres(kv,count);
}






