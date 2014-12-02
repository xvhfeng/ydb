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
#include <unistd.h>

#include "spx_types.h"
#include "spx_defs.h"
#include "spx_string.h"
#include "spx_alloc.h"
#include "spx_list.h"
#include "spx_path.h"
#include "spx_socket.h"
#include "spx_time.h"
#include "spx_path.h"
#include "spx_job.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"


spx_private err_t ydb_mountpoint_free(void **arg);
spx_private err_t ydb_tracker_free(void **arg);
spx_private u64_t ydb_storage_hole_idx_refresh_timeout(\
        int hour,int min,int sec,err_t *err);
spx_private u64_t ydb_storage_configurtion_iosize_convert(
        SpxLogDelegate *log,string_t v,u32_t default_unitsize,
        char *errinfo,err_t *err);
spx_private u32_t ydb_storage_configurtion_timespan_convert(
        SpxLogDelegate *log,string_t v,u32_t default_unitsize,
        char *errinfo,err_t *err);

spx_private err_t ydb_mountpoint_free(void **arg){/*{{{*/
    if(NULL == arg || NULL == *arg){
        return 0;
    }
    struct ydb_storage_mountpoint **mp = (struct ydb_storage_mountpoint **)arg;

    if(NULL != (*mp)->path){
        SpxStringFree((*mp)->path);
    }
    SpxFree(*mp);
    return 0;
}/*}}}*/

spx_private err_t ydb_tracker_free(void **arg){/*{{{*/
    struct ydb_tracker ** t = (struct ydb_tracker **) arg;
    if(NULL != (*t)->ystc){
        struct ydb_storage_transport_context *ystc = (*t)->ystc;
        if(NULL != ystc->request){
            if(NULL != ystc->request->header){
                SpxFree(ystc->request->header);
            }
            if(NULL != ystc->request->body){
                SpxMsgFree(ystc->request->body);
            }
            SpxFree(ystc->request);
        }

        if(NULL != ystc->response){
            if(NULL != ystc->response->header){
                SpxFree(ystc->response->header);
            }
            if(NULL != ystc->response->body){
                SpxMsgFree(ystc->response->body);
            }
            SpxFree(ystc->response);
        }

        if( 0!= ystc->fd){
            SpxClose(ystc->fd);
        }
        SpxFree((*t)->ystc);
    }
    if(NULL != (*t)->hjc){
        spx_job_context_free((void **) &((*t)->hjc));
    }
    SpxFree(*t);
    return 0;
}/*}}}*/

spx_private u64_t ydb_storage_hole_idx_refresh_timeout(\
        int hour,int min,int sec,err_t *err){/*{{{*/
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
}/*}}}*/

void *ydb_storage_config_before_handle(SpxLogDelegate *log,err_t *err){/*{{{*/

    struct ydb_storage_configurtion *c = (struct ydb_storage_configurtion *) \
                                              spx_alloc_alone(sizeof(*c),err);
    if(NULL == c){
        SpxLog2(log,SpxLogError,*err,\
                "alloc the storage config is fail.");
        return NULL;
    }
    c->log = log;
    c->port = 8175;
    c->timeout = 5;
    c->waitting = 30;
    c->logsize = 10 * SpxMB;
    c->loglevel = SpxLogInfo;
    c->balance = YDB_STORAGE_MOUNTPOINT_LOOP;
    c->heartbeat = 30;
    c->daemon = true;
    c->stacksize = 128 * SpxKB;
    c->notifier_module_thread_size = 4;
    c->network_module_thread_size = 8;
    c->task_module_thread_size = 4;
    c->context_size = 256;
    c->freedisk =(u64_t) 4 * SpxGB;
    c->mountpoints = spx_list_new(log,\
            YDB_STORAGE_MOUNTPOINT_COUNT,ydb_mountpoint_free,err);
    c->trackers = spx_vector_init(log,\
            ydb_tracker_free,err);
    c->storerooms = 256;
    c->storemode = YDB_STORAGE_STOREMODE_TURN;
    c->storecount = 4096;
    c->fillchunk = true;
    c->holerefresh = YDB_STORAGE_HOLEREFRESH_FIXEDTIME;
    c->refreshtime = ydb_storage_hole_idx_refresh_timeout(6,0,0,err);

    c->compress = false;
    c->chunkfile = true;
    c->chunksize = 64 * SpxMB;
    c->overload = true;
    c->oversize = 10;
    c->overmode = YDB_STORAGE_OVERMODE_RELATIVE;
    c->singlemin = 10 * SpxKB;
    c->lazyrecv = true;
    c->lazysize = 1 * SpxMB;
    c->sendfile = true;
    c->binlog_size =(u64_t) 2 * SpxGB;
    c->runtime_flush_timespan = 60;
    c->pagesize = getpagesize();
    c->query_sync_timespan = 30;
    c->sync = YDB_STORAGE_SYNC_REALTIME;
    c->sync_wait = 1;
    c->sync_begin.hour =4;
    c->sync_begin.min = 0;
    c->sync_begin.sec = 0;
    c->sync_end.hour = 6;
    c->sync_end.min = 0;
    c->sync_end.sec = 0;
    c->disksync_timespan = SpxDayTick;
    c->disksync_busysize = 512 * SpxMB;
    c->start_timespan = spx_now();
    c->sync_threads_count = 3;

    return c;
}/*}}}*/

spx_private u64_t ydb_storage_configurtion_iosize_convert(
        SpxLogDelegate *log,string_t v,u32_t default_unitsize,
        char *errinfo,err_t *err){/*{{{*/
    u64_t size = strtoul(v,NULL,10);
    if(ERANGE == size) {
        SpxLog1(log,SpxLogError,\
                errinfo);
        *err = size;
        return 0;
    }
    string_t unit = spx_string_range_new(v,\
            -2,spx_string_len(v),err);
    if(NULL == unit){
        size *= default_unitsize;
    }
    if(0 == spx_string_casecmp(unit,"GB")){
        size *= SpxGB;
    }
    else if( 0 == spx_string_casecmp(unit,"MB")){
        size *= SpxMB;
    }else if(0 == spx_string_casecmp(unit,"KB")){
        size *= SpxKB;
    }else {
        size *= default_unitsize;
    }
    spx_string_free(unit);
    return size;
}/*}}}*/

spx_private u32_t ydb_storage_configurtion_timespan_convert(
        SpxLogDelegate *log,string_t v,u32_t default_unitsize,
        char *errinfo,err_t *err){/*{{{*/
    u64_t size = strtoul(v,NULL,10);
    if(ERANGE == size) {
        SpxLog1(log,SpxLogError,\
                errinfo);
        *err = size;
        return 0;
    }
    string_t unit = spx_string_range_new(v,\
            -2,spx_string_len(v),err);
    if(NULL == unit){
        size *= default_unitsize;
    }
    if(0 == spx_string_casecmp(unit,"D")){
        size *= SpxDayTick;
    }
    else if( 0 == spx_string_casecmp(unit,"H")){
        size *= SpxHourTick;
    }else if(0 == spx_string_casecmp(unit,"M")){
        size *= SpxMinuteTick;
    }else if(0 == spx_string_casecmp(unit,"S")){
        size *= SpxSecondTick;
    }else {
        size *= default_unitsize;
    }
    spx_string_free(unit);
    return size;
}/*}}}*/


void ydb_storage_config_line_parser(string_t line,void *config,err_t *err){
    struct ydb_storage_configurtion *c = (struct ydb_storage_configurtion *) config;
    int count = 0;
    string_t *kv = spx_string_splitlen(line,\
            spx_string_len(line),"=",strlen("="),&count,err);
    if(NULL == kv){
        return;
    }

    spx_string_trim(*kv," ");
    if(2 == count){
        spx_string_trim(*(kv + 1)," ");
    }

    //ip
    if(0 == spx_string_casecmp(*kv,"ip")){
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
    if(0 == spx_string_casecmp(*kv,"port")){
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
    if(0 == spx_string_casecmp(*kv,"timeout")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"use default timeout:%d.",c->timeout);
        } else {
            u32_t timeout = ydb_storage_configurtion_timespan_convert(c->log,*(kv + 1),
                    SpxSecondTick,
                "bad the configurtion item of timeout.",err);
            if(0 != *err) {
                goto r1;
            }
            c->timeout = timeout;
        }
        goto r1;
    }


    //waitting
    if(0 == spx_string_casecmp(*kv,"waitting")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"use default waitting:%d.",c->waitting);
        } else {
            u32_t waitting = ydb_storage_configurtion_timespan_convert(c->log,*(kv + 1),
                    SpxSecondTick,
                "bad the configurtion item of waitting.",err);
            if(0 != *err) {
                goto r1;
            }
            c->waitting = waitting;
        }
        goto r1;
    }

    //runtime flush timespan
    if(0 == spx_string_casecmp(*kv,"runtime_flush_timespan")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"use default runtime flush timespan:%d.",c->timeout);
        } else {
            u32_t timeout = ydb_storage_configurtion_timespan_convert(c->log,*(kv + 1),SpxSecondTick,
                    "bad the configurtion item of runtime flush timespan.",err);
            if(0 != *err) {
                goto r1;
            }
            c->runtime_flush_timespan = timeout;
        }
        goto r1;
    }



    //daemon
    if(0 == spx_string_casecmp(*kv,"daemon")){
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
    if(0 == spx_string_casecmp(*kv,"stacksize")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"stacksize use default:%lld.",c->stacksize);
        } else {
            u64_t size = ydb_storage_configurtion_iosize_convert(
                    c->log,*(kv + 1),SpxKB,
                    "convert stacksize is fail.",err);
            if(0 != *err){
                goto r1;
            }
            c->stacksize = size;
        }
        goto r1;
    }

    //notifier_module_thread_size
    if(0 == spx_string_casecmp(*kv,"notifier_module_thread_size")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "notifier module_thread_size use default:%d.",\
                    c->notifier_module_thread_size);
        } else {
            u32_t notifier_module_thread_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == notifier_module_thread_size) {
                SpxLog1(c->log,SpxLogError,\
                        "bad the configurtion item of notifier module thread size.");
                goto r1;
            }
            c->notifier_module_thread_size = notifier_module_thread_size;
        }
        goto r1;
    }

    //network_module_thread_size
    if(0 == spx_string_casecmp(*kv,"network_module_thread_size")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "network module thread size use default:%d.",\
                    c->network_module_thread_size);
        } else {
            u32_t network_module_thread_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == network_module_thread_size) {
                SpxLog1(c->log,SpxLogError,\
                        "bad the configurtion item of network_module_thread_size.");
                goto r1;
            }
            c->network_module_thread_size = network_module_thread_size;
        }
        goto r1;
    }

    //task_module_thread_size
    if(0 == spx_string_casecmp(*kv,"task_module_thread_size")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "task_module_thread_size use default:%d.",c->task_module_thread_size);
        } else {
            u32_t task_module_thread_size = strtol(*(kv + 1),NULL,10);
            if(ERANGE == task_module_thread_size) {
                SpxLog1(c->log,SpxLogError,\
                        "bad the configurtion item of task_module_thread_size.");
                goto r1;
            }
            c->task_module_thread_size = task_module_thread_size;
        }
        goto r1;
    }

    //heartbeat
    if(0 == spx_string_casecmp(*kv,"heartbeat")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "heartbeat use default:%d.",c->heartbeat);
        } else {
            u32_t heartbeat = ydb_storage_configurtion_timespan_convert(
                    c->log,*(kv + 1),SpxSecondTick,
                    "bad configurtion item of heartheat.",err);
            if(0 != *err) {
                goto r1;
            }
            c->heartbeat = heartbeat;
        }
        goto r1;
    }
    //basepath
    if(0 == spx_string_casecmp(*kv,"basepath")){
        if(1 == count){
            SpxLog1(c->log,SpxLogError,"bad the configurtion item of basepath.and basepath is empty.");
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

    //dologpath
    if(0 == spx_string_casecmp(*kv,"dologpath")){
        if(1 == count){
            c->dologpath = spx_string_new("/opt/ydb/dolog/storage/",err);
            if(NULL == c->dologpath){
                SpxLog2(c->log,SpxLogError,*err,\
                        "alloc default dologpath is fail.");
                goto r1;
            }
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "dologpath use default:%s.",c->dologpath);
        }else {
            c->dologpath = spx_string_dup(*(kv + 1),err);
            if(NULL == c->dologpath){
                SpxLog2(c->log,SpxLogError,*err,\
                        "dup the string for dologpath is fail.");
            }
        }
        goto r1;
    }


    //logprefix
    if(0 == spx_string_casecmp(*kv,"logprefix")){
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
    if(0 == spx_string_casecmp(*kv,"logsize")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "logsize use default:%lld.",c->logsize);
        } else {
            u64_t size = ydb_storage_configurtion_iosize_convert(
                    c->log,*(kv + 1),SpxMB,
                    "convert logsize is fail.",err);
            if(0 != *err){
                goto r1;
            }
            c->logsize = size;
        }
        goto r1;
    }

    //loglevel
    if(0 == spx_string_casecmp(*kv,"loglevel")){
        if(1 == count){
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

    //binlog size
    if(0 == spx_string_casecmp(*kv,"binlog_size")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "logsize use default:%lld.",c->binlog_size);
        } else {
            u64_t size = ydb_storage_configurtion_iosize_convert(
                    c->log,*(kv + 1),SpxGB,
                    "convert binlog size is fail.",err);
            if(0 != *err){
                goto r1;
            }
            c->binlog_size = size;
        }
        goto r1;
    }

    //balance
    if(0 == spx_string_casecmp(*kv,"balance")){
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
    if(0 == spx_string_casecmp(*kv,"master")){
        if(spx_string_begin_casewith(*kv,"mp")){
            int idx = strtol((*kv) + strlen("mp"),\
                    NULL,16);
            c->master = idx;
            goto r1;
        }
    }

    //groupname
    if(0 == spx_string_casecmp(*kv,"groupname")){
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
    if(0 == spx_string_casecmp(*kv,"machineid")){
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


    //context_size
    if(0 == spx_string_casecmp(*kv,"context_size")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "context_size use default:%d.",\
                    c->context_size);
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

    //freedisk
    if(0 == spx_string_casecmp(*kv,"freedisk")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "freedisk use default:%lld.",c->freedisk);
        } else {
            u64_t size = ydb_storage_configurtion_iosize_convert(
                    c->log,*(kv + 1),SpxMB,
                    "convert freedisk is fail.",err);
            if(0 != *err){
                goto r1;
            }
            c->freedisk = size;
        }
        goto r1;
    }

    //mount point
    if(spx_string_begin_casewith(*kv,"mp")){
        int idx = strtol((*kv) + strlen("mp"),\
                NULL,16);

        struct ydb_storage_mountpoint *mp = spx_alloc_alone(sizeof(*mp),err);
        if(NULL == mp){
            goto r1;
        }
        mp->path = spx_string_dup(*(kv + 1),err);
        if(NULL == mp->path){
            SpxFree(mp);
            ydb_mountpoint_free((void **) &mp);
            goto r1;
        }
        mp->idx = idx;
        mp->isusing = true;
        spx_list_set(c->mountpoints,idx,mp);
        goto r1;
    }

    //tracker
    if(0 == spx_string_casecmp(*kv,"tracker")){
        int sum = 0;
        string_t *kv1 = spx_string_splitlen(*(kv + 1),\
                spx_string_len(*(kv + 1)),":",strlen(":"),&sum,err);
        struct ydb_tracker *t = spx_alloc_alone(sizeof(*t),err);
        if(NULL == t){
            spx_string_free_splitres(kv1,sum);
            goto r1;
        }
        if(spx_socket_is_ip(*kv1)){
            t->host.ip = spx_string_dup(*kv1,err);
            if(NULL == t->host.ip){
                SpxLog2(c->log,SpxLogError,*err,\
                        "dup t ip is fail.");
            }
        }else {
            t->host.ip = spx_socket_getipbyname(*kv1,err);
            if(NULL == t->host.ip){
                SpxLogFmt2(c->log,SpxLogError,*err,\
                        "get t ip by hostname:%s is fail.",\
                        *kv1);
            }
        }
        t->host.port = strtol(*(kv1 + 1),NULL,10);
        spx_vector_add(c->trackers,t);
        spx_string_free_splitres(kv1,sum);
        goto r1;
    }

    //storerooms
    if(0 == spx_string_casecmp(*kv,"storerooms")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "storerooms use default:%d.",\
                    c->storerooms);
        } else {
            u32_t storerooms = strtol(*(kv + 1),NULL,10);
            if(ERANGE == storerooms) {
                SpxLog1(c->log,SpxLogError,\
                        "bad the configurtion item of storerooms.");
                goto r1;
            }
            c->storerooms = storerooms;
        }
        goto r1;
    }

    //storemode
    if(0 == spx_string_casecmp(*kv,"storemode")){
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
    if(0 == spx_string_casecmp(*kv,"storecount")){
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
    if(0 == spx_string_casecmp(*kv,"fillchunk")){
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
    if(0 == spx_string_casecmp(*kv,"compress")){
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
    if(0 == spx_string_casecmp(*kv,"chunkfile")){
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
    if(0 == spx_string_casecmp(*kv,"chunksize")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "chunksize use default:%lld.",c->chunksize);
        } else {
            u64_t size = ydb_storage_configurtion_iosize_convert(
                    c->log,*(kv + 1),SpxMB,
                    "convert chunksize is fail.",err);
            if(0 != *err){
                goto r1;
            }
            c->chunksize = size;
        }
        goto r1;
    }

    //overload
    if(0 == spx_string_casecmp(*kv,"overload")){
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
    if(0 == spx_string_casecmp(*kv,"oversize")){
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
            u64_t size = ydb_storage_configurtion_iosize_convert(
                    c->log,*(kv + 1),SpxKB,
                    "convert oversize is fail.",err);
            if(0 != *err){
                goto r1;
            }
            c->oversize = size;
        }
        goto r1;
    }

    //singlemin
    if(0 == spx_string_casecmp(*kv,"singlemin")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "single min use default:%lld.",c->singlemin);
        } else {
            u64_t size = ydb_storage_configurtion_iosize_convert(
                    c->log,*(kv + 1),SpxMB,
                    "convert singlemin is fail.",err);
            if(0 != *err){
                goto r1;
            }
            c->singlemin = size;
        }
        goto r1;
    }

    //lazyrecv
    if(0 == spx_string_casecmp(*kv,"lazyrecv")){
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
    if(0 == spx_string_casecmp(*kv,"lazysize")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "lazysize use default:%lld.",c->lazysize);
        } else {
            u64_t size = ydb_storage_configurtion_iosize_convert(
                    c->log,*(kv + 1),SpxMB,
                    "comvert lazysize is fail.",err);
            if(0 != *err){
                goto r1;
            }
            c->lazysize = size;
        }
        goto r1;
    }

    //sendfile
    if(0 == spx_string_casecmp(*kv,"sendfile")){
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
    if(0 == spx_string_casecmp(*kv,"holerefresh")){
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
    if(0 == spx_string_casecmp(*kv,"refreshtime")){
        string_t s = *(kv + 1);
        int sum = 0;
        if(spx_string_exist(s,':')){
            string_t *kv1 = spx_string_splitlen(s,
                    spx_string_len(s),":",strlen(":"),&sum,err);
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


    //sync
    if(0 == spx_string_casecmp(*kv,"sync")){
        if(1 ==  count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "sync use default:%s.",\
                    sync_mode_desc[c->sync]);
        }
        string_t s = *(kv + 1);
        if(0 == spx_string_casecmp(s,\
                    sync_mode_desc[YDB_STORAGE_SYNC_REALTIME])){
            c->sync = YDB_STORAGE_SYNC_REALTIME;
        } else if(0 == spx_string_casecmp(s,\
                    sync_mode_desc[YDB_STORAGE_SYNC_REALTIME])){
            c->sync = YDB_STORAGE_SYNC_REALTIME;
        } else {
            c->sync = YDB_STORAGE_SYNC_FIXEDTIME;
        }
        goto r1;
    }


    //syncgroup
    if(0 == spx_string_casecmp(*kv,"syncgroup")){
        if(1 == count){
            *err = EINVAL;
            SpxLog1(c->log,SpxLogError,\
                    "bad the configurtion item of syncgroup.and syncgroup is empty.");
            goto r1;
        }
        c->syncgroup = spx_string_dup(*(kv + 1),err);
        if(NULL == c->syncgroup){
            SpxLog2(c->log,SpxLogError,*err,\
                    "dup the syncgroup is fail.");
        }
        goto r1;
    }


    //query_sync_timespan
    if(0 == spx_string_casecmp(*kv,"query_sync_timespan")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"use default query_sync_timespan:%d.",c->query_sync_timespan);
        } else {
            u32_t timespan = ydb_storage_configurtion_timespan_convert(
                    c->log,*(kv + 1),SpxSecondTick,
                    "bad configurtion item of sync timespan.",err);
            if(0 != *err){
                goto r1;
            }
            c->query_sync_timespan = timespan;
        }
        goto r1;
    }

    //sync_wait
    if(0 == spx_string_casecmp(*kv,"sync_wait")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"use default sync_wait:%d.",c->sync_wait);
        } else {
            u32_t timespan = ydb_storage_configurtion_timespan_convert(
                    c->log,*(kv + 1),SpxSecondTick,
                    "bad configurtion item of sync wait.",err);
            if(0 != *err){
                goto r1;
            }
            c->sync_wait = timespan;
        }
        goto r1;
    }

    //disksync timespan
    if(0 == spx_string_casecmp(*kv,"disksync_timespan")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,"use default disksync timespan:%lld.",c->disksync_timespan);
        } else {
            u32_t timespan = ydb_storage_configurtion_timespan_convert(
                    c->log,*(kv + 1),SpxSecondTick,
                    "bad configurtion item of sync wait.",err);
            if(0 != *err){
                goto r1;
            }
            c->disksync_timespan = timespan;
        }
        goto r1;
    }

    //disksync busysize
    if(0 == spx_string_casecmp(*kv,"disksync_busysize")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "disksync busysize use default:%lld.",c->disksync_busysize);
        } else {
            u64_t size = ydb_storage_configurtion_iosize_convert(
                    c->log,*(kv + 1),SpxMB,
                    "comvert disksync_busysize is fail.",err);
            if(0 != *err){
                goto r1;
            }
            c->disksync_busysize = size;
        }
        goto r1;
    }

    //sync_threads_count
    if(0 == spx_string_casecmp(*kv,"sync_threads_count")){
        if(1 == count){
            SpxLogFmt1(c->log,SpxLogWarn,\
                    "sync_threads_count use default:%d.",c->sync_threads_count);
        } else {
            u32_t sync_threads_count = strtol(*(kv + 1),NULL,10);
            if(ERANGE == sync_threads_count) {
                SpxLog2(c->log,SpxLogError,*err,\
                        "convect sync_threads_count is fail.");
                goto r1;
            }
            c->sync_threads_count = sync_threads_count;
        }
        goto r1;
    }
r1:
    spx_string_free_splitres(kv,count);
    //the size is the smallest with the same syncgroup
}







