#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "spx_types.h"
#include "spx_properties.h"
#include "spx_defs.h"
#include "spx_io.h"
#include "spx_time.h"
#include "spx_path.h"
#include "spx_list.h"
#include "spx_string.h"
#include "spx_limits.h"
#include "spx_periodic.h"
#include "spx_alloc.h"
#include "spx_vector.h"
#include "spx_thread.h"
#include "spx_nio.h"
#include "spx_module.h"
#include "spx_network_module.h"
#include "spx_message.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_mountpoint.h"
#include "ydb_storage_sync.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_synclog.h"
#include "ydb_storage_dio.h"
#include "ydb_storage_dio_context.h"

#define YdbStorageSyncLogFileRecvSize (1 * SpxMB)

struct ydb_storage_dsync_mp{
    int idx;
    bool_t begin_dsync;
    bool_t dsync_over;
    bool_t dsync_fail;
    bool_t need_dsync;
};

#define YDB_STORAGE_DSYNC_NORMAL 0
#define YDB_STORAGE_DSYNC_LOGFILE 1
#define YDB_STORAGE_DSYNC_DATA 2

struct ydb_storage_dsync_runtime{
    struct spx_periodic *timer;
    struct ydb_storage_configurtion *c;
    int step;//0:no start;1:binlog;2:synclog;3:data
    struct spx_date sync_date_curr;
    u64_t offset;
    u32_t last_linelen;
    struct spx_date dsync_over_of_mps;
    struct ydb_storage_dsync_mp
        ydb_storage_dsync_mps[YDB_STORAGE_MOUNTPOINT_MAXSIZE];
    u64_t begin_dsync_timespan;
    //do the dsync last time
};

spx_private struct ydb_storage_remote *base_storage = NULL;

spx_private bool_t ydb_storage_dsync_confim(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt);

spx_private struct ydb_storage_remote *ydb_storage_dsync_query_base(
        struct ydb_storage_configurtion * c,
        err_t *err);

spx_private struct ydb_storage_remote *ydb_storage_dsync_query_base_from_tracker(
        struct ydb_storage_configurtion *c,
        struct  ydb_tracker *t,
        err_t *err);

spx_private u64_t ydb_storage_dsync_query_begin_timespan(
        struct ydb_storage_configurtion * c,
        err_t *err);

spx_private u64_t ydb_storage_dsync_query_begin_timespan_from_tracker(
        struct ydb_storage_configurtion *c,
        struct  ydb_tracker *t,
        err_t *err);

spx_private err_t ydb_storage_dsync_sync_synclogs(
        struct ydb_storage_configurtion *c,
        struct spx_date *dt_sync_curr);

spx_private err_t ydb_storage_dsync_sync_logfile(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        int protocol,
        string_t machineid,
        struct spx_date *dtlogf,
        string_t logfname);

spx_private err_t ydb_storage_dsync_binlog_data(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt);

spx_private err_t ydb_storage_dsync_synclog_data(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt);

spx_private err_t ydb_storage_dsync_from_remote_storage(
        struct ydb_storage_configurtion *c,
        string_t logfname,
        struct ydb_storage_remote *s,
        struct ydb_storage_remote *base,
        struct ydb_storage_dsync_runtime *dsrt);

spx_private err_t ydb_storage_dsync_delete_request(
        struct ydb_storage_configurtion *c,
        string_t fid);

spx_private err_t ydb_storage_dsync_upload_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        string_t fid);

spx_private err_t ydb_storage_dsync_modify_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        string_t fid,
        string_t ofid);

spx_private err_t ydb_storage_dsync_data_to_remote_storage(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        string_t fid);

spx_private void ydb_storage_dsync_over(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt);

spx_private struct ydb_storage_dsync_runtime *ydb_storage_dsync_runtime_reader(
        struct ydb_storage_configurtion *c,
        err_t *err);

spx_private void *ydb_storage_dsync_runtime_writer(
        void *arg);

spx_private string_t ydb_storage_dsync_make_runtime_filename(
        struct ydb_storage_configurtion *c,
        err_t *err);

spx_private void ydb_storage_dsync_runtime_file_remove(
        struct ydb_storage_configurtion *c);

spx_private void ydb_storage_dsync_mountpoint_over(
        struct ydb_storage_dsync_runtime  *dsrt);

spx_private err_t ydb_storage_dsync_total_mounpoint_sync_timespan(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt);

spx_private void ydb_storage_dsync_data_from_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents);

spx_private void ydb_storage_dsync_data_from_singlefile(
        struct ev_loop *loop,ev_async *w,int revents);

err_t ydb_storage_dsync_logfile(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);

spx_private void ydb_storage_dsync_logfile_context(
        struct ev_loop *loop,ev_async *w,int revents);


/*
 * note:please make sure the mountpoint of storages in the same syncgroup
 * are the same,must is the disksize ,freesize,and blocksize of disk.
 */

err_t ydb_storage_dsync_startup(struct ydb_storage_configurtion *c,
        struct ydb_storage_runtime *srt
        ){/*{{{*/
    err_t err = 0;
    srt->status = YDB_STORAGE_DSYNCING;

    struct spx_date dt_sync_begin;
    struct spx_date dt_sync_end;

    struct ydb_storage_dsync_runtime *dsrt = NULL;
    dsrt = ydb_storage_dsync_runtime_reader(c,&err);
    bool_t is_send_dsync_over = false;
    while(true){// for fail twice
        bool_t is_need_dsync = false;
        if(NULL == dsrt){
            dsrt = spx_alloc_alone(sizeof(*dsrt),&err);
            if(NULL == dsrt){
                SpxLog2(c->log,SpxLogError,err,
                        "new disksync runtime is fail.");
                return err;
            }
            dsrt->c = c;
            is_need_dsync = ydb_storage_dsync_confim(c,dsrt);
            if(!is_need_dsync){
                if(is_send_dsync_over){
                    break;
                } else {
                    SpxFree(dsrt);
                    return 0;
                }
            }
            is_send_dsync_over = true;
            dsrt->step = YDB_STORAGE_DSYNC_LOGFILE;
        } else {
            //do the dsync last time
            is_need_dsync = true;
            is_send_dsync_over = true;
            SpxLog1(c->log,SpxLogMark,
                    "do dsync last time goon.");
        }

        err = ydb_storage_dsync_total_mounpoint_sync_timespan(c,dsrt);
        if(0 != err){
            SpxFree(dsrt);
            SpxLog2(c->log,SpxLogError,err,
                    "get mps dsync over date is fail.");
            break;
        }

        //get base storage
        //and the base is the full data
        //the base storage must be exist
        if(NULL == base_storage) {
            base_storage = ydb_storage_dsync_query_base(c,&err);
            if(NULL == base_storage){
                SpxLogFmt1(c->log,SpxLogMark,
                        "no the base storage in the all syncgroup:%s,"
                        "and then,this storage:%s is upto base storage.",
                        c->syncgroup,c->machineid);
                //set all mountpoint is not disksync
                ydb_storage_dsync_mountpoint_over(dsrt);
                SpxFree(dsrt);
                return 0;
            }
        }

        //get begin timespan of begining
        if(0 == dsrt->begin_dsync_timespan){
            dsrt->begin_dsync_timespan = ydb_storage_dsync_query_begin_timespan(c,&err);
            if(0 == dsrt->begin_dsync_timespan){
                SpxLogFmt1(c->log,SpxLogMark,
                        "no the timespan of begin sync,"
                        "so use default this startup time:%lld.",
                        g_ydb_storage_runtime->this_startup_time);
            } else {
                if(dsrt->begin_dsync_timespan < g_ydb_storage_runtime->first_statrup_time){
                    g_ydb_storage_runtime->first_statrup_time = dsrt->begin_dsync_timespan;
                }
            }
        }

        SpxZero(dt_sync_begin);
        SpxZero(dt_sync_end);
        SpxZero(dsrt->sync_date_curr);

        spx_get_date((time_t *) &(dsrt->begin_dsync_timespan),&dt_sync_begin);
        spx_get_date((time_t *) &(dsrt->begin_dsync_timespan),&(dsrt->sync_date_curr));
        spx_get_date((time_t *) &(g_ydb_storage_runtime->this_startup_time),
                &dt_sync_end);

        SpxLogFmt1(c->log,SpxLogInfo,
                "check and sync binlog and synclog from %d-%d-%d to %d-%d-%d.",
                dt_sync_begin.year,dt_sync_begin.month,dt_sync_begin.day,
                dt_sync_end.year,dt_sync_end.month,dt_sync_end.day);

        dsrt->timer = spx_periodic_exec_and_async_run(c->log,
                c->heartbeat,0,
                ydb_storage_dsync_runtime_writer,dsrt,
                c->stacksize,
                &err);
        if(0 != err){
            SpxLog2(c->log,SpxLogError,err,
                    "create new thread for dsync runtime writer is fail.");
            break;
        }

        if(YDB_STORAGE_DSYNC_LOGFILE == dsrt->step) {/*{{{*/
            while(true) {
                int cmp = spx_date_cmp(&(dsrt->sync_date_curr),&dt_sync_end);
                if(0 >= cmp ){ // do dsync
                    SpxLogFmt1(c->log,SpxLogInfo,
                            "check and sync binlog of date: %d-%d-%d.",
                            dsrt->sync_date_curr.year,
                            dsrt->sync_date_curr.month,
                            dsrt->sync_date_curr.day);

                    //sync binlog
                    string_t binlog_filename = ydb_storage_binlog_make_filename(c->log,
                            c->dologpath,c->machineid,(dsrt->sync_date_curr).year,
                            (dsrt->sync_date_curr).month,(dsrt->sync_date_curr).day,&err);

                    if(!SpxFileExist(binlog_filename)){
                        err = ydb_storage_dsync_sync_logfile(c,base_storage,
                                YDB_S2S_SYNC_LOGFILE,
                                c->machineid,
                                &(dsrt->sync_date_curr),
                                binlog_filename);
                        if(0 != err){
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "sync binlog of  %d-%d-%d is fail.",
                                    dsrt->sync_date_curr.year,
                                    dsrt->sync_date_curr.month,
                                    dsrt->sync_date_curr.day);
                        }
                    }
                    SpxStringFree(binlog_filename);
                    SpxErrReset;
                    err = 0;
                    //sync synclog
                    err = ydb_storage_dsync_sync_synclogs(c,&(dsrt->sync_date_curr));
                } else { //no dsync
                    SpxLogFmt1(c->log,SpxLogInfo,
                            "check and sync binlog and synclog from %d-%d-%d to %d-%d-%d is over.",
                            dt_sync_begin.year,dt_sync_begin.month,dt_sync_begin.day,
                            dt_sync_end.year,dt_sync_end.month,dt_sync_end.day);
                    break;
                }
                spx_date_add(&(dsrt->sync_date_curr),1);

                if(0 < c->sync_wait) {
                    spx_periodic_sleep(c->sync_wait,0);
                }
            }
            dsrt->step = YDB_STORAGE_DSYNC_DATA;
        }/*}}}*/

        /*
           SpxZero(dsrt->sync_date_curr);
           spx_get_date((time_t *) &(dsrt->begin_dsync_timespan),&(dsrt->sync_date_curr));
           SpxLogFmt1(c->log,SpxLogInfo,
           "sync data from binlog and synclog from %d-%d-%d to %d-%d-%d.",
           dt_sync_begin.year,dt_sync_begin.month,dt_sync_begin.day,
           dt_sync_end.year,dt_sync_end.month,dt_sync_end.day);

           if(YDB_STORAGE_DSYNC_SYNCLOGFILE == dsrt->step) {
           while(true) {
           int cmp = spx_date_cmp(&(dsrt->sync_date_curr),&dt_sync_end);
           if(0 > cmp ){ // do dsync

           SpxLogFmt1(c->log,SpxLogInfo,
           "sync data by binlog of date: %d-%d-%d.",
           dt_sync_curr.year,dt_sync_curr.month,dt_sync_curr.day);

           string_t synclog_fname = ydb_storage_binlog_make_filename(c->log,
           c->dologpath,c->machineid,(dsrt->sync_date_curr).year,
           (dsrt->sync_date_curr).month,(dsrt->sync_date_curr).day,&err);

           if(!SpxFileExist(synclog_fname)){
           SpxLogFmt1(c->log,SpxLogInfo,
           "no the operator at %d-%d-%d,and then no binlog.",
           dt_sync_curr.year,dt_sync_curr.month,dt_sync_curr.day);
           } else {
           err = ydb_storage_dsync_sync_synclogs(c,&(dsrt->sync_date_curr));
        //                        err = ydb_storage_dsync_sync_logfile(c,base_storage,
        //                                YDB_S2S_SYNC_LOGFILE,
        //                                c->machineid,
        //                                &(dsrt->sync_date_curr),
        //                                synclog_fname);
        if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
        "sync binlog of  %d-%d-%d is fail.",
        dt_sync_curr.year,dt_sync_curr.month,
        dt_sync_curr,day);
        }
        }
        SpxStringFree(synclog_fname);
        SpxErrReset;
        err = 0;
        //sync synclog
        //                    err = ydb_storage_dsync_sync_synclogs(c,&(dsrt->sync_date_curr));
        } else { //no dsync
        SpxLogFmt1(c->log,SpxLogInfo,
        "check and sync binlog and synclog from %d-%d-%d to %d-%d-%d is over.",
        dt_sync_begin.year,dt_sync_begin.month,dt_sync_begin.day,
        dt_sync_end.year,dt_sync_end.month,dt_sync_end.day);
        break;
        }
        spx_date_add(&(dsrt->sync_date_curr),1);
        }
        dsrt->step = YDB_STORAGE_DSYNC_DATA;
        }
        */

        SpxZero((dsrt->sync_date_curr));
        spx_get_date((time_t *) &(dsrt->begin_dsync_timespan),&(dsrt->sync_date_curr));
        SpxLogFmt1(c->log,SpxLogInfo,
                "sync data from binlog and synclog from %d-%d-%d to %d-%d-%d.",
                dt_sync_begin.year,dt_sync_begin.month,dt_sync_begin.day,
                dt_sync_end.year,dt_sync_end.month,dt_sync_end.day);

        if(YDB_STORAGE_DSYNC_DATA  == dsrt->step){/*{{{*/
            while(true) {
                int cmp = spx_date_cmp(&(dsrt->sync_date_curr),&dt_sync_end);
                if(0 >= cmp ){ // do dsync

                    SpxLogFmt1(c->log,SpxLogInfo,
                            "sync data by binlog of date: %d-%d-%d.",
                            dsrt->sync_date_curr.year,
                            dsrt->sync_date_curr.month,
                            dsrt->sync_date_curr.day);
                    err = ydb_storage_dsync_binlog_data(c,dsrt);

                    SpxLogFmt1(c->log,SpxLogInfo,
                            "sync data by synclog of date: %d-%d-%d.",
                            dsrt->sync_date_curr.year,
                            dsrt->sync_date_curr.month,
                            dsrt->sync_date_curr.day);
                    err =  ydb_storage_dsync_synclog_data(c,dsrt);
                } else { //no dsync
                    SpxLogFmt1(c->log,SpxLogInfo,
                            "check and sync binlog and synclog from %d-%d-%d to %d-%d-%d is over.",
                            dt_sync_begin.year,dt_sync_begin.month,dt_sync_begin.day,
                            dt_sync_end.year,dt_sync_end.month,dt_sync_end.day);
                    break;
                }
                spx_date_add(&(dsrt->sync_date_curr),1);

            }
        }/*}}}*/

        //channel the dsync runtime file timer
        //delete the dsync runtime file
        //set last-freesize by freesize to mp
        SpxLog1(c->log,SpxLogMark,
                "channel the therad of dsync runtime writer,and the operator is balocking...");
        spx_periodic_stop(&(dsrt->timer),true);//must blocking to cannel the thread
        SpxLog1(c->log,SpxLogMark,
                "the thread if dsync runtime writer is channeled,delete the runtime file.");
        ydb_storage_dsync_runtime_file_remove(c);

        ydb_storage_dsync_mountpoint_over(dsrt);
        SpxFree(dsrt);
    }

    ydb_storage_dsync_over(c, dsrt);
    SpxFree(dsrt);
    g_ydb_storage_runtime->status = YDB_STORAGE_DSYNCED;
    return err;
}/*}}}*/

spx_private bool_t ydb_storage_dsync_confim(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt
        ){/*{{{*/
    bool_t rc = false;
    ydb_storage_mprtf_reader(c);
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_COUNT; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path) && mp->isusing){
            if(mp->dsync_force){
                mp->need_dsync = true;
            } else {
                if(0 != mp->last_freesize &&
                        (mp->last_freesize + c->disksync_busysize >= mp->freesize)) {
                    mp->need_dsync = false;
                } else {
                    struct ydb_storage_dsync_mp *dmp = dsrt->ydb_storage_dsync_mps + i;
                    dmp->idx = i;
                    dmp->dsync_fail = false;
                    dmp->dsync_over = false;
                    dmp->begin_dsync = false;
                    dmp->need_dsync = true;
                    rc = true;
                }
            }
        }
    }

    return rc;
}/*}}}*/

spx_private struct ydb_storage_remote *ydb_storage_dsync_query_base(
        struct ydb_storage_configurtion * c,
        err_t *err
        ){/*{{{*/
    struct ydb_storage_remote *base = NULL;
    struct spx_vector_iter *iter = spx_vector_iter_new(c->trackers ,err);
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,*err,\
                "init the trackers iter is fail.");
        return NULL;
    }

    int trys = 3;
    do{
        trys--;
        struct ydb_tracker *t = NULL;
        while(NULL != (t = spx_vector_iter_next(iter))){
            base = ydb_storage_dsync_query_base_from_tracker(
                    c,t,err);
            if(NULL == base){
                SpxLogFmt2(c->log,SpxLogError,*err,
                        "no get  base storage from tracker :%s:%d.",
                        t->host.ip,t->host.port);
            } else {
                break;
            }
        }
        if(NULL == base){
            if(0 < trys){
                SpxLogFmt1(c->log,SpxLogWarn,
                        "no the base storage from all tracker,"
                        "sleep %d seconds again,this time is %d.",
                        c->query_basestorage_timespan,trys);
                spx_vector_iter_reset(iter);
                spx_periodic_sleep(c->query_basestorage_timespan,0);
            } else {
                SpxLog1(c->log,SpxLogWarn,
                        "no the base storage from all tracker,"
                        "try 3 times and out of trying.");
            }
        }
    }while(NULL == base && 0 <= trys);
    spx_vector_iter_free(&iter);
    return base;
}/*}}}*/

spx_private struct ydb_storage_remote *ydb_storage_dsync_query_base_from_tracker(
        struct ydb_storage_configurtion *c,
        struct  ydb_tracker *t,
        err_t *err
        ){/*{{{*/
    struct ydb_storage_remote *base = NULL;
    string_t machineid = NULL;
    struct ydb_storage_transport_context *ystc = NULL;
    ystc = spx_alloc_alone(sizeof(*ystc),err);
    if(NULL == ystc){
        SpxLog2(c->log,SpxLogError,*err,
                "new transport context for query base storage is fail");
        return NULL;
    }
    if(0 == ystc->fd) {
        ystc->fd  = spx_socket_new(err);
        if(0 >= ystc->fd){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "new socket to remote tracker "
                    "net address %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }

        if(0 != (*err = spx_socket_set(ystc->fd,SpxKeepAlive,SpxAliveTimeout,\
                        SpxDetectTimes,SpxDetectTimeout,\
                        SpxLinger,SpxLingerTimeout,\
                        SpxNodelay,\
                        true,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "set socket to remote tracker "
                    "net address %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }
        if(0 != (*err = spx_socket_connect_nb(ystc->fd,
                        t->host.ip,t->host.port,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "connect to remote tracker,"
                    "address %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request){
        ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),err);
        if(NULL == ystc->request){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "new request for query base storage "
                    "to remote tracker address %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request->header){
        struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),err);
        if(NULL == header){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "new request header for query base storage "
                    "to remote tracker,ip:%s,port:%d.",
                    t->host.ip,t->host.port);
            goto r1;
        }
        ystc->request->header = header;
        header->protocol = YDB_S2T_QUERY_BASE_STORAGE;
        header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN + YDB_SYNCGROUP_LEN;
        header->is_keepalive = false;//persistent connection
    }

    if(NULL == ystc->request->body){
        struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "new request body for query base storage "
                    "to remote tracker,ip:%s,port:%d.",
                    t->host.ip,t->host.port);
            goto r1;
        }
        ystc->request->body = body;
        spx_msg_pack_fixed_string(body,c->groupname,YDB_GROUPNAME_LEN);
        spx_msg_pack_fixed_chars(body,c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_fixed_string(body,c->syncgroup,YDB_SYNCGROUP_LEN);
    }


    *err = spx_write_context_nb(c->log,ystc->fd,ystc->request);
    if(0 != *err){
        SpxLogFmt2(c->log,SpxLogError,*err,
                "send request for query base storage "
                "to remote tracker,ip:%s,port:%d is fail.",
                t->host.ip,t->host.port);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->fd,c->timeout)){
        //timeout
        *err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "recving data for query base storage "
                "from remote tracker,ip:%s,port:%d is timeout.",
                t->host.ip,t->host.port);
        goto r1;
    }

    if(NULL == ystc->response){
        ystc->response = spx_alloc_alone(sizeof(struct spx_msg_context),err);
        if(NULL == ystc->response){
            SpxLog2(c->log,SpxLogError,*err,
                    "new response for query sync storage is fail.");
            goto r1;
        }
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->fd,err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,*err,
                "recving data for query base storage "
                "from remote tracker,ip:%s,port:%d is fail.",
                t->host.ip,t->host.port);
        goto r1;
    }

    *err = ystc->response->header->err;
    if(0 != *err){
        if(0 == ystc->response->header->bodylen
                && ENOENT == *err) {
            SpxLogFmt1(c->log,SpxLogWarn,
                    "no base storage from tracker:%s:%d.",
                    t->host.ip,t->host.port);
            goto r1;
        } else {
            SpxLogFmt2(c->log,SpxLogWarn,*err,
                    "no base storage from tracker:%s:%d.",
                    t->host.ip,t->host.port);
            goto r1;
        }
    }

    ystc->response->body = spx_read_body_nb(c->log,
            ystc->fd,ystc->response->header->bodylen,err);
    if(NULL == ystc->response->body){
        SpxLogFmt2(c->log,SpxLogError,*err,
                "recving data for query base storage "
                "from remote tracker,ip:%s,port:%d is fail.",
                t->host.ip,t->host.port);
        goto r1;
    }

    struct spx_msg *body = ystc->response->body;
    spx_msg_seek(body,0,SpxMsgSeekSet);
    machineid = spx_msg_unpack_string(body,YDB_MACHINEID_LEN,err);
    if(NULL == machineid){
        *err = EIO;
        SpxLogFmt2(c->log,SpxLogError,EIO,
                "recving data for query base storage "
                "from remote tracker,ip:%s,port:%d is fail.",
                t->host.ip,t->host.port);
        goto r1;
    }
    base = spx_map_get(g_ydb_storage_remote,machineid,spx_string_len(machineid),NULL);
    if(NULL == base){
        *err = ENOENT;
        SpxLogFmt2(c->log,SpxLogError,ENOENT,
                "recving data for query base storage:%s"
                "from remote tracker,ip:%s,port:%d is fail.",
                machineid,
                t->host.ip,t->host.port);
        goto r1;
    }
r1:
    SpxClose(ystc->fd);
    if(NULL != machineid){
        SpxStringFree(machineid);
    }
    if(NULL != ystc && NULL != ystc->request){
        if(NULL !=ystc->request->header){
            SpxFree(ystc->request->header);
        }
        if(NULL != ystc->request->body){
            SpxFree(ystc->request->body);
        }
    }
    if(NULL != ystc && NULL != ystc->response){
        if(NULL != ystc->response->header){
            SpxFree(ystc->response->header);
        }
        if(NULL != ystc->response->body){
            SpxMsgFree(ystc->response->body);
        }
        SpxFree(ystc->response);
    }
    return base;
}/*}}}*/


spx_private u64_t ydb_storage_dsync_query_begin_timespan(
        struct ydb_storage_configurtion * c,
        err_t *err
        ){/*{{{*/
    u64_t times = 0;
    struct spx_vector_iter *iter = spx_vector_iter_new(c->trackers ,err);
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,*err,\
                "init the trackers iter is fail.");
        return 0;
    }

    int trys = 3;
    do{
        trys--;
        struct ydb_tracker *t = NULL;
        while(NULL != (t = spx_vector_iter_next(iter))){
            times = ydb_storage_dsync_query_begin_timespan_from_tracker(
                    c,t,err);
            if(0 == times && 0 != *err){
                SpxLogFmt2(c->log,SpxLogError,*err,
                        "no get timespan of begin sync from tracker :%s:%d.",
                        t->host.ip,t->host.port);
            } else {
                break;
            }
        }
        if(0 == times){
            if(0 < trys){
                SpxLogFmt1(c->log,SpxLogWarn,
                        "no the timespan of begin sync from all tracker,"
                        "sleep %d seconds again,this time is %d.",
                        c->query_basestorage_timespan,trys);
                spx_vector_iter_reset(iter);
                spx_periodic_sleep(c->query_basestorage_timespan,0);
            } else {
                SpxLog1(c->log,SpxLogWarn,
                        "no the timespan of begin sync from all tracker,"
                        "try 3 times and out of trying.");
            }
        }
    }while(0 == times && 0 <= trys);
    spx_vector_iter_free(&iter);
    return times;
}/*}}}*/

spx_private u64_t ydb_storage_dsync_query_begin_timespan_from_tracker(
        struct ydb_storage_configurtion *c,
        struct  ydb_tracker *t,
        err_t *err
        ){/*{{{*/
    u64_t times = 0;
    struct ydb_storage_transport_context *ystc = NULL;
    ystc = spx_alloc_alone(sizeof(*ystc),err);
    if(NULL == ystc){
        SpxLog2(c->log,SpxLogError,*err,
                "new transport context for query timespan of begining sync is fail");
        return 0;
    }
    if(0 == ystc->fd) {
        ystc->fd  = spx_socket_new(err);
        if(0 >= ystc->fd){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "new socket to remote tracker "
                    "net address %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }

        if(0 != (*err = spx_socket_set(ystc->fd,SpxKeepAlive,SpxAliveTimeout,\
                        SpxDetectTimes,SpxDetectTimeout,\
                        SpxLinger,SpxLingerTimeout,\
                        SpxNodelay,\
                        true,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "set socket to remote tracker "
                    "net address %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }
        if(0 != (*err = spx_socket_connect_nb(ystc->fd,
                        t->host.ip,t->host.port,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "connect to remote tracker,"
                    "address %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request){
        ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),err);
        if(NULL == ystc->request){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "new request for query timespan of begin sync "
                    "to remote tracker address %s:%d is fail.",
                    t->host.ip,t->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request->header){
        struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),err);
        if(NULL == header){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "new request header for query timespan of begin sync "
                    "to remote tracker,ip:%s,port:%d.",
                    t->host.ip,t->host.port);
            goto r1;
        }
        ystc->request->header = header;
        header->protocol = YDB_S2T_QUERY_SYNC_BEGIN_TIMESPAN;
        header->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN + YDB_SYNCGROUP_LEN;
        header->is_keepalive = false;//persistent connection
    }

    if(NULL == ystc->request->body){
        struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,*err,
                    "new request body for query timespan of begin sync "
                    "to remote tracker,ip:%s,port:%d.",
                    t->host.ip,t->host.port);
            goto r1;
        }
        ystc->request->body = body;
        spx_msg_pack_fixed_string(body,c->groupname,YDB_GROUPNAME_LEN);
        spx_msg_pack_fixed_chars(body,c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_fixed_string(body,c->syncgroup,YDB_SYNCGROUP_LEN);
    }


    *err = spx_write_context_nb(c->log,ystc->fd,ystc->request);
    if(0 != *err){
        SpxLogFmt2(c->log,SpxLogError,*err,
                "send request for query timespan of begin sync "
                "to remote tracker,ip:%s,port:%d is fail.",
                t->host.ip,t->host.port);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->fd,c->timeout)){
        //timeout
        *err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "recving data for query timespan of begin sync "
                "from remote tracker,ip:%s,port:%d is timeout.",
                t->host.ip,t->host.port);
        goto r1;
    }

    if(NULL == ystc->response){
        ystc->response = spx_alloc_alone(sizeof(struct spx_msg_context),err);
        if(NULL == ystc->response){
            SpxLog2(c->log,SpxLogError,*err,
                    "new response for query sync storage is fail.");
            goto r1;
        }
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->fd,err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,*err,
                "recving data for query timespan of begin sync "
                "from remote tracker,ip:%s,port:%d is fail.",
                t->host.ip,t->host.port);
        goto r1;
    }

    *err = ystc->response->header->err;
    if(0 != *err){
        SpxLogFmt2(c->log,SpxLogWarn,*err,
                "query timespan of begin sync from tracker:%s:%d.",
                t->host.ip,t->host.port);
        goto r1;
    }

    ystc->response->body = spx_read_body_nb(c->log,
            ystc->fd,ystc->response->header->bodylen,err);
    if(NULL == ystc->response->body){
        SpxLogFmt2(c->log,SpxLogError,*err,
                "recving data for query timespan of begin sync "
                "from remote tracker,ip:%s,port:%d is fail.",
                t->host.ip,t->host.port);
        goto r1;
    }

    struct spx_msg *body = ystc->response->body;
    spx_msg_seek(body,0,SpxMsgSeekSet);
    times = spx_msg_unpack_u64(body);
r1:
    SpxClose(ystc->fd);
    if(NULL != ystc && NULL != ystc->request){
        if(NULL !=ystc->request->header){
            SpxFree(ystc->request->header);
        }
        if(NULL != ystc->request->body){
            SpxFree(ystc->request->body);
        }
    }
    if(NULL != ystc && NULL != ystc->response){
        if(NULL != ystc->response->header){
            SpxFree(ystc->response->header);
        }
        if(NULL != ystc->response->body){
            SpxMsgFree(ystc->response->body);
        }
        SpxFree(ystc->response);
    }
    return times;
}/*}}}*/

spx_private err_t ydb_storage_dsync_sync_synclogs(
        struct ydb_storage_configurtion *c,
        struct spx_date *dt_sync_curr
        ){/*{{{*/
    err_t err = 0;
    struct spx_map_iter *iter = spx_map_iter_new(g_ydb_storage_remote,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init iter of remote storage is fail.");
        return err;
    }
    struct spx_map_node *n = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&err))){
        if(NULL != n && NULL != n->v){
            SpxTypeConvert2(struct ydb_storage_remote,s,n->v);
            string_t synclog_filename =
                ydb_storage_synclog_make_filename(c->log,
                        c->dologpath,s->machineid,
                        dt_sync_curr->year,
                        dt_sync_curr->month,
                        dt_sync_curr->day,
                        &err);

            SpxLogFmt1(c->log,SpxLogInfo,
                    "check and sync synclog of storage:%s and date: %d-%d-%d.",
                    s->machineid,
                    dt_sync_curr->year,dt_sync_curr->month,dt_sync_curr->day);

            if(!SpxFileExist(synclog_filename)){
                err = ydb_storage_dsync_sync_logfile(c,s,
                        YDB_S2S_SYNC_LOGFILE,
                        s->machineid,
                        dt_sync_curr,
                        synclog_filename);

                // if fail in the source storage,then try base storage again
                if(0 != err) {

                    SpxLogFmt2(c->log,SpxLogError,err,
                            "check and sync synclog of storage:%s and date: %d-%d-%d is fail.",
                            s->machineid,
                            dt_sync_curr->year,dt_sync_curr->month,dt_sync_curr->day);
                    if (0 == spx_string_casecmp(
                                s->machineid,
                                base_storage->machineid)){

                        SpxLogFmt2(c->log,SpxLogInfo,err,
                                "try with base storage:%s "
                                "to sync synclog for storage:%s "
                                "of date:%d-%d-%d.",
                                base_storage->machineid,s->machineid,
                                dt_sync_curr->year,dt_sync_curr->month,dt_sync_curr->day);
                        err = ydb_storage_dsync_sync_logfile(c,base_storage,
                                YDB_S2S_SYNC_LOGFILE,
                                s->machineid,
                                dt_sync_curr,
                                synclog_filename);
                        if(0 != err){
                            SpxLogFmt2(c->log,SpxLogInfo,err,
                                    "try with base storage:%s "
                                    "to sync synclog for storage:%s of date:%d-%d-%d. is fail.",
                                    base_storage->machineid,s->machineid,
                                    dt_sync_curr->year,dt_sync_curr->month,dt_sync_curr->day);
                        }
                    }
                }
            }
            SpxStringFree(synclog_filename);
            SpxLogFmt1(c->log,SpxLogInfo,
                    "check and sync over synclog of storage:%s date: %d-%d-%d.",
                    s->machineid,
                    dt_sync_curr->year,dt_sync_curr->month,dt_sync_curr->day);
        }
    }
    spx_map_iter_free(&iter);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_dsync_sync_logfile(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        int protocol,
        string_t machineid,
        struct spx_date *dtlogf,
        string_t logfname
        ){/*{{{*/
    err_t err  = 0;
    struct ydb_storage_transport_context *ystc = NULL;
    ystc = spx_alloc_alone(sizeof(*ystc),&err);
    if(NULL == ystc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new transport context for sync logfile,"
                "in the remote storage:%s is fail.",
                s->machineid);
        return err;
    }
    int binlog_fd = 0;
    char *ptr = NULL;
    u64_t filesize = 0;
    if(0 == ystc->fd) {
        ystc->fd  = spx_socket_new(&err);
        if(0 >= ystc->fd){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new socket to remote storage:%s,"
                    "net address %s:%d is fail.",
                    s->machineid,
                    s->host.ip,s->host.port);
            goto r1;
        }

        if(0 != (err = spx_socket_set(ystc->fd,SpxKeepAlive,SpxAliveTimeout,\
                        SpxDetectTimes,SpxDetectTimeout,\
                        SpxLinger,SpxLingerTimeout,\
                        SpxNodelay,\
                        true,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "set socket to remote storage:%s,"
                    "net address %s:%d is fail.",
                    s->machineid,
                    s->host.ip,s->host.port);
            goto r1;
        }
        if(0 != (err = spx_socket_connect_nb(ystc->fd,
                        s->host.ip,s->host.port,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "connect to remote storage:%s,"
                    "address %s:%d is fail.",
                    s->machineid,
                    s->host.ip,s->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request){
        ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->request){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request for sync logfile "
                    "to remote storage %s address %s:%d is fail.",
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
    }

    if(NULL == ystc->request->header){
        struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
        if(NULL == header){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request header for sync logfile "
                    "to remote storage:%s,ip:%s,port:%d.",
                    s->machineid,s->host.ip,s->host.port);
            goto r1;
        }
        header->protocol = protocol;
        header->bodylen = YDB_MACHINEID_LEN + 3 * sizeof(u32_t);
        header->is_keepalive = false;//persistent connection
        ystc->request->header = header;
    }

    if(NULL == ystc->request->body) {
        struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,&err);
        if(NULL == body){
            SpxLog2(c->log,SpxLogError,err,
                    "new body of request for sync logfile is fail.");
            goto r1;
        }
        ystc->request->body = body;
        spx_msg_pack_fixed_string(body,c->machineid,YDB_MACHINEID_LEN);
        spx_msg_pack_u32(body,dtlogf->year);
        spx_msg_pack_u32(body,dtlogf->month);
        spx_msg_pack_u32(body,dtlogf->day);
    }

    err = spx_write_context_nb(c->log,ystc->fd,ystc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "send request for sync logfile "
                "to remote storage:%s,ip:%s,port:%d is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;

    }

    if(!spx_socket_read_timeout(ystc->fd,c->timeout)){
        //timeout
        err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "recving data for query mountpoint state "
                "from remote storage:%s,ip:%s,port:%d is timeout.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    if(NULL == ystc->response){
        ystc->response = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ystc->response){
            SpxLog2(c->log,SpxLogError,err,
                    "new response for query sync storage is fail.");
            goto r1;
        }
    }

    ystc->response->header =  spx_read_header_nb(c->log,ystc->fd,&err);
    if(NULL == ystc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recving data for query mountpoint state "
                "from remote storage:%s,ip:%s,port:%d is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = ystc->response->header->err;
    if(0 != err){
        if(ENOENT == err){
            SpxLogFmt1(c->log,SpxLogWarn,
                    "logfile of machineid:%s date:%d-%d-%d is not exist.",
                    machineid,dtlogf->year,dtlogf->month,
                    dtlogf->day);
        } else {
            SpxLogFmt1(c->log,SpxLogWarn,
                    "get logfile of machineid:%s date:%d-%d-%d is fail.",
                    machineid,dtlogf->year,dtlogf->month,
                    dtlogf->day);
        }
        goto r1;
    }

    binlog_fd = SpxWriteOpen(logfname,true);
    if(0 >= binlog_fd){
        err = 0 == errno ? EACCES : errno;
        SpxLogFmt2(c->log,SpxLogWarn,err,
                "create logfile of machineid:%s date:%d-%d-%d is fail.",
                machineid,dtlogf->year,dtlogf->month,
                dtlogf->day);
        goto r1;
    }
    filesize = ystc->response->header->bodylen - ystc->response->header->offset;
    if(0 > ftruncate(binlog_fd,filesize)){
        err = 0 == errno ? EACCES : errno;
        SpxLogFmt2(c->log,SpxLogWarn,err,
                "change logfile filesize of machineid:%s date:%d-%d-%d is fail.",
                machineid,dtlogf->year,dtlogf->month,
                dtlogf->day);
        goto r1;
    }
    ptr = mmap(NULL,\
            filesize,PROT_READ | PROT_WRITE ,\
            MAP_SHARED,binlog_fd,0);
    if(MAP_FAILED == ptr){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,\
                "mmap the logfile file machineid:%s "
                "date:%d-%d-%d to memory is fail.",
                machineid,dtlogf->year,dtlogf->month,
                dtlogf->day);
        goto r1;
    }

    err = spx_lazy_mmap_nb(c->log,ptr,ystc->fd,filesize,0);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,\
                "write the logfile file machineid:%s "
                "date:%d-%d-%d to memory is fail.",
                machineid,dtlogf->year,dtlogf->month,
                dtlogf->day);
    }

r1:
    SpxClose(ystc->fd);
    SpxClose(binlog_fd);
    if(NULL != ptr){
        munmap(ptr,filesize);
    }
    if(0 != err){
        if(SpxFileExist(logfname)){
            SpxLogFmt2(c->log,SpxLogError,err,\
                    "write the logfile file machineid:%s "
                    "date:%d-%d-%d to memory is fail."
                    " and remove it.",
                    machineid,dtlogf->year,dtlogf->month,
                    dtlogf->day);
            remove(logfname);
        }
    }
    if(NULL != ystc && NULL != ystc->request){
        if(NULL !=ystc->request->header){
            SpxFree(ystc->request->header);
        }
        if(NULL != ystc->request->body){
            SpxFree(ystc->request->body);
        }
    }
    if(NULL != ystc && NULL != ystc->response){
        if(NULL != ystc->response->header){
            SpxFree(ystc->response->header);
        }
        if(NULL != ystc->response->body){
            SpxMsgFree(ystc->response->body);
        }
        SpxFree(ystc->response);
    }
    return err;
}/*}}}*/

spx_private err_t ydb_storage_dsync_binlog_data(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt
        ){/*{{{*/
    err_t err = 0;
    string_t blog_fname = ydb_storage_binlog_make_filename(c->log,
            c->dologpath,c->machineid,
            dsrt->sync_date_curr.year,
            dsrt->sync_date_curr.month,
            dsrt->sync_date_curr.day,
            &err);
    if(NULL == blog_fname) {
        SpxLog2(c->log,SpxLogError,err,
                "make binlog filename is fail.");
        return err;
    }

    if(SpxFileExist(blog_fname)){
        err =  ydb_storage_dsync_from_remote_storage(c,
                blog_fname,base_storage,NULL,dsrt);
    }
    SpxStringFree(blog_fname);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_dsync_synclog_data(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt
        ){/*{{{*/
    err_t err = 0;
    struct spx_map_iter *iter = spx_map_iter_new(g_ydb_storage_remote,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init iter of remote storage is fail.");
        return err;
    }
    struct spx_map_node *n = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&err))){
        if(NULL != n && NULL != n->v){
            SpxTypeConvert2(struct ydb_storage_remote,s,n->v);
            string_t synclog_filename =
                ydb_storage_synclog_make_filename(c->log,
                        c->dologpath,s->machineid,
                        dsrt->sync_date_curr.year,
                        dsrt->sync_date_curr.month,
                        dsrt->sync_date_curr.day,
                        &err);

            SpxLogFmt1(c->log,SpxLogInfo,
                    "check and sync synclog of storage:%s and date: %d-%d-%d.",
                    s->machineid,
                    dsrt->sync_date_curr.year,
                    dsrt->sync_date_curr.month,
                    dsrt->sync_date_curr.day);

            if(SpxFileExist(synclog_filename)){
                err = ydb_storage_dsync_from_remote_storage(c,
                        synclog_filename,s,base_storage,dsrt);
            }
            SpxStringFree(synclog_filename);
            SpxLogFmt1(c->log,SpxLogInfo,
                    "check and sync over synclog of storage:%s date: %d-%d-%d.",
                    s->machineid,
                    dsrt->sync_date_curr.year,
                    dsrt->sync_date_curr.month,
                    dsrt->sync_date_curr.day);
        }
    }
    spx_map_iter_free(&iter);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_dsync_from_remote_storage(
        struct ydb_storage_configurtion *c,
        string_t logfname,
        struct ydb_storage_remote *s,
        struct ydb_storage_remote *base,
        struct ydb_storage_dsync_runtime *dsrt
        ){/*{{{*/
    err_t err = 0;
    if(!SpxFileExist(logfname)){
        SpxLogFmt1(c->log,SpxLogError,
                "logfile:%s is not exist.",
                logfname);
        return 0;
    }
    FILE *fp = SpxFReadOpen(logfname);
    if(NULL == fp){
        err = 0 == errno ? EINVAL : errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "open logfile:%s is fail.",
                logfname);
        return err;
    }
    string_t line = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),&err);
    if(NULL == line){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new a line of dsync in the logfile:%s is fail.",
                logfname);
        goto r1;
    }
    if(0 != dsrt->offset){
        err = errno;
        if(0 != fseek(fp,dsrt->offset - dsrt->last_linelen,SEEK_SET)){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "seek file pointer to :%lld is fail."
                    "then begin from file-beginer.",
                    dsrt->offset - dsrt->last_linelen);
            dsrt->offset = 0;
        }
    }
    int size = strlen("\t");
    int count = 0;
    while(NULL != fgets(line,SpxLineSize,fp)){
        spx_string_updatelen(line);
        dsrt->last_linelen = spx_string_len(line);
        dsrt->offset += dsrt->last_linelen;
        spx_string_strip_linefeed(line);
        string_t *strs = spx_string_split(line,"\t",size,&count,&err);
        if(NULL == strs){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "split line:%s in the logfile:%s is fail.",
                    line,logfname);
            continue;
        }
        switch (**strs){
            case (YDB_STORAGE_LOG_UPLOAD) :
                {
                    err = ydb_storage_dsync_upload_request(c,s,(*strs + 1));
                    if(0 != err ) {
                        if(NULL == base){
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "upload of dsync file:%s "
                                    "in the file:%s from remote:%s is fail,"
                                    "and base storage:%s is not support.",
                                    line,
                                    logfname,
                                    s->machineid,
                                    base->machineid);
                        } else {
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "upload of dsync file:%s "
                                    "in the file:%s from remote:%s is fail,"
                                    "and try base storage:%s again.",
                                    line,
                                    logfname,
                                    s->machineid,
                                    base->machineid);
                            err = ydb_storage_dsync_upload_request(c,
                                    base,(*strs + 1));
                            if(0 != err){
                                SpxLogFmt2(c->log,SpxLogError,err,
                                        "upload of dsync file:%s "
                                        "in the file:%s "
                                        "from base storage:%s is fail,",
                                        line,
                                        logfname,
                                        s->machineid,
                                        base->machineid);
                            }
                        }
                    }
                    break;
                }
            case (YDB_STORAGE_LOG_MODIFY) :
                {
                    err = ydb_storage_dsync_modify_request(c,
                            s,(*strs + 1),(*strs + 2));
                    if(0 != err ) {
                        if(NULL == base){
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "modify of dsync file:%s "
                                    "in the file:%s from remote:%s is fail,"
                                    "and base storage:%s is not support.",
                                    line,
                                    logfname,
                                    s->machineid,
                                    base->machineid);
                        } else {
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "modify of dsync file:%s "
                                    "in the file:%s from remote:%s is fail,"
                                    "and try base storage:%s again.",
                                    line,
                                    logfname,
                                    s->machineid,
                                    base->machineid);
                            err = ydb_storage_dsync_modify_request(c,
                                    base,(*strs + 1),(*strs + 2));
                            if(0 != err){
                                SpxLogFmt2(c->log,SpxLogError,err,
                                        "modify of dsync file:%s "
                                        "in the file:%s "
                                        "from base storage:%s is fail,",
                                        line,
                                        logfname,
                                        s->machineid,
                                        base->machineid);
                            }
                        }
                    }
                    break;
                }
            case (YDB_STORAGE_LOG_DELETE):
                {
                    err = ydb_storage_dsync_delete_request(c,(*strs + 1));
                    if(0 != err){
                        SpxLogFmt2(c->log,SpxLogError,err,
                                "delete of dsync file:%s in the file:%s is fail,"
                                "and delete in the local,so not base storage.",
                                line,logfname);
                    }
                    break;
                }
            default:{
                        SpxLogFmt1(c->log,SpxLogError,
                                "no the operator:%c in  the ydb.",
                                **strs);
                        break;
                    }

        }
        spx_string_free_splitres(strs,count);
        if(0 < c->sync_wait) {
            spx_periodic_sleep(c->sync_wait,0);
        }

    }
r1:
    if(NULL != fp){
        fclose(fp);
        fp = NULL;
    }
    if(NULL != line){
        SpxStringFree(line);
    }
    return err;
}/*}}}*/

spx_private err_t ydb_storage_dsync_delete_request(
        struct ydb_storage_configurtion *c,
        string_t fid
        ){/*{{{*/
    err_t err  = 0;
    struct ydb_storage_sync_context *ysdc = NULL;
    ysdc = spx_alloc_alone(sizeof(*ysdc),&err);
    if(NULL == ysdc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "alloc dsync context for delete file:%s is fail.",
                fid);
        return err;
    }
    struct ydb_storage_fid *fidbuf =
        spx_alloc_alone(sizeof(struct ydb_storage_fid),&err);
    if(NULL == fidbuf){
        SpxLogFmt2(c->log,SpxLogError,err,
                "alloc fid struct for delete dsync deleting of file:%s is fail.",
                fid);
        goto r1;
    }
    ysdc->fid = fidbuf;
    err = ydb_storage_dio_parser_fileid(c->log,fid,
            &(fidbuf->groupname),&(fidbuf->machineid),
            &(fidbuf->syncgroup),&(fidbuf->issinglefile),
            &(fidbuf->mpidx),&(fidbuf->p1),&(fidbuf->p2),
            &(fidbuf->tidx),&(fidbuf->fcreatetime),
            &(fidbuf->rand),&(fidbuf->begin),&(fidbuf->realsize),
            &(fidbuf->totalsize),&(fidbuf->ver),&(fidbuf->opver),
            &(fidbuf->lastmodifytime),&(fidbuf->hashcode),
            &(fidbuf->has_suffix),&(fidbuf->suffix));
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "parser fid:%s is for dsync deleting file is fail.",
                fid);
        goto r1;
    }

    string_t fname = ydb_storage_dio_make_filename(c->log,
            fidbuf->issinglefile,c->mountpoints,fidbuf->mpidx,
            fidbuf->p1,fidbuf->p2,fidbuf->machineid,
            fidbuf->tidx,fidbuf->fcreatetime,fidbuf->rand,
            fidbuf->suffix,&err);
    if(NULL == fname){
        SpxLogFmt2(c->log,SpxLogError,err,
                "make filename from fid:%s for dsync of deleting file is fail.",
                fid);
        goto r1;
    }

    ysdc->fname = fname;
    u32_t unit = 0;
    u64_t begin = 0;
    u64_t offset = 0;
    u64_t len = 0;
    if(!SpxFileExist(fname)){
        SpxLogFmt1(c->log,SpxLogDebug,
                "delete fname:%s by dsyncing is not exist.",
                fname);
        goto r1;
    }
    if(fidbuf->issinglefile){
        remove(fname);
        goto r1;
    } else {
        int fd = SpxWriteOpen(fname,false);
        if(0 >= fd){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open chunkfile:%s for dsync of deleting file is fail.",
                    fname);
            goto r1;
        }
        ysdc->fd = fd;
        unit = (int) fidbuf->begin / c->pagesize;
        begin = unit * c->pagesize;
        offset = fidbuf->begin - begin;
        len = offset + fidbuf->totalsize;

        char *ptr = SpxMmap(fd,begin,len);
        if(NULL == ptr){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "mmap chunkfile:%s for dsync of  deleting file is fail.",
                    fname);
            goto r1;
        }
        ysdc->ptr = ptr;

        struct spx_msg *ioctx = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);
        if(NULL == ioctx){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "alloc metadata for fid:%s to dsync of deleting bu file:%s is fail.",
                    fid,fname);
            goto r1;
        }
        ysdc->md = ioctx;

        if(0 != (err = spx_msg_pack_ubytes(ioctx,
                        ((ubyte_t *) (ptr+ offset)),
                        YDB_CHUNKFILE_MEMADATA_SIZE))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "cp metadata to ioctx is fail,fid:%s,fname:%s.",
                    fid,fname);
            goto r1;
        }
        spx_msg_seek(ioctx,0,SpxMsgSeekSet);

        bool_t io_isdelete = false;
        u32_t io_opver = 0;
        u32_t io_ver = 0;
        u64_t io_createtime = 0;
        u64_t io_lastmodifytime = 0;
        u64_t io_totalsize = 0;
        u64_t io_realsize = 0;
        string_t io_suffix = NULL;
        string_t io_hashcode = NULL;

        err = ydb_storage_dio_parser_metadata(c->log,ioctx,
                &io_isdelete,&io_opver,
                &io_ver,&io_createtime,
                &io_lastmodifytime,&io_totalsize,&io_realsize,
                &io_suffix,&io_hashcode);
        if(fidbuf->opver > io_opver){
            spx_msg_seek(ioctx,0,SpxMsgSeekSet);
            spx_msg_pack_true(ioctx);//isdelete
            spx_msg_pack_u32(ioctx,fidbuf->opver);
            spx_msg_pack_u32(ioctx,YDB_VERSION);
            spx_msg_seek(ioctx,sizeof(u64_t),SpxMsgSeekCurrent);//jump createtime
            spx_msg_pack_u64(ioctx,fidbuf->lastmodifytime);

            memcpy(ptr + offset,ioctx->buf,YDB_CHUNKFILE_MEMADATA_SIZE);
        }
        if(NULL != io_suffix){
            SpxStringFree(io_suffix);
        }
        if(NULL != io_hashcode){
            SpxStringFree(io_hashcode);
        }
    }
r1:
    ydb_storage_sync_context_free(&ysdc);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_dsync_upload_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        string_t fid
        ){/*{{{*/
    err_t err  = 0;
    struct ydb_storage_sync_context *ysdc = NULL;
    ysdc = spx_alloc_alone(sizeof(*ysdc),&err);
    if(NULL == ysdc){
        SpxLogFmt2(c->log,SpxLogError,err,
                "alloc dsync remote object is fail.",
                "fid:%s.",fid);
        return err;
    }
    struct ydb_storage_fid *fidbuf =
        spx_alloc_alone(sizeof(struct ydb_storage_fid),&err);
    if(NULL == fidbuf){
        SpxLogFmt2(c->log,SpxLogError,err,
                "alloc fid object is fail.",
                "fid:%s.",fid);
        goto r1;
    }

    ysdc->fid = fidbuf;
    err = ydb_storage_dio_parser_fileid(c->log,fid,
            &(fidbuf->groupname),&(fidbuf->machineid),
            &(fidbuf->syncgroup),&(fidbuf->issinglefile),
            &(fidbuf->mpidx),&(fidbuf->p1),&(fidbuf->p2),
            &(fidbuf->tidx),&(fidbuf->fcreatetime),
            &(fidbuf->rand),&(fidbuf->begin),&(fidbuf->realsize),
            &(fidbuf->totalsize),&(fidbuf->ver),&(fidbuf->opver),
            &(fidbuf->lastmodifytime),&(fidbuf->hashcode),
            &(fidbuf->has_suffix),&(fidbuf->suffix));
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "parser fid:%s is fail.",
                fid);
        goto r1;
    }

    string_t fname = ydb_storage_dio_make_filename(c->log,
            fidbuf->issinglefile,c->mountpoints,fidbuf->mpidx,
            fidbuf->p1,fidbuf->p2,fidbuf->machineid,
            fidbuf->tidx,fidbuf->fcreatetime,fidbuf->rand,
            fidbuf->suffix,&err);
    if(NULL == fname){
        SpxLogFmt2(c->log,SpxLogError,err,
                "make fname by fid:%s  is fail.",
                fid);
        goto r1;
    }
    ysdc->fname = fname;
    u32_t unit = 0;
    u64_t begin = 0;
    u64_t offset = 0;
    u64_t len = 0;


    //check localhost
    if(SpxFileExist(fname)){
        if(fidbuf->issinglefile){
            SpxLogFmt1(c->log,SpxLogDebug,
                    "fname:%s is exist and no do dsync.",
                    fname);
            goto r1;
        } else {

            int fd = SpxWriteOpen(fname,false);
            if(0 >= fd){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "open chunkfile:%s for fid:%s is fail.",
                        fname,fid);
                goto r1;
            }
            ysdc->fd = fd;
            unit = (int) fidbuf->begin / c->pagesize;
            begin = unit * c->pagesize;
            offset = fidbuf->begin - begin;
            len = offset + fidbuf->totalsize;

            char *ptr = SpxMmap(fd,begin,len);
            if(MAP_FAILED == ptr){
                err = errno;
                SpxLogFmt2(c->log,SpxLogError,err,
                        "mmap fid:%s to file:%s is fail.",
                        fid,fname);
                goto r1;
            }
            ysdc->ptr = ptr;

            struct spx_msg *ioctx = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);
            if(NULL == ioctx){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "new metadata for fid:%s with fname:%s is fail.",
                        fid,fname);
                goto r1;
            }
            ysdc->md = ioctx;

            if(0 != (err = spx_msg_pack_ubytes(ioctx,
                            ((ubyte_t *) (ptr+ offset)),
                            YDB_CHUNKFILE_MEMADATA_SIZE))){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "pack metadata for fid:%s with fname:%s is fail.",
                        fid,fname);
                goto r1;
            }
            spx_msg_seek(ioctx,0,SpxMsgSeekSet);

            bool_t io_isdelete = false;
            u32_t io_opver = 0;
            u32_t io_ver = 0;
            u64_t io_createtime = 0;
            u64_t io_lastmodifytime = 0;
            u64_t io_totalsize = 0;
            u64_t io_realsize = 0;
            string_t io_suffix = NULL;
            string_t io_hashcode = NULL;

            err = ydb_storage_dio_parser_metadata(c->log,ioctx,
                    &io_isdelete,&io_opver,
                    &io_ver,&io_createtime,
                    &io_lastmodifytime,&io_totalsize,&io_realsize,
                    &io_suffix,&io_hashcode);
            if(fidbuf->opver <= io_opver){
                SpxLogFmt1(c->log,SpxLogInfo,
                        "the fid:%s is not last in the fname:%s,so not do dsync.",
                        fid,fname);
                if(NULL != io_suffix){
                    SpxStringFree(io_suffix);
                }
                if(NULL != io_hashcode){
                    SpxStringFree(io_hashcode);
                }
                goto r1;
            }

            if(NULL != io_suffix){
                SpxStringFree(io_suffix);
            }
            if(NULL != io_hashcode){
                SpxStringFree(io_hashcode);
            }
        }
    }

    //need disksync
    if(0 == ysdc->sock) {
        ysdc->sock  = spx_socket_new(&err);
        if(0 >= ysdc->sock){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new socket to remote storage:%s,"
                    "host:%s:%d for dsync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }

        if(0 != (err = spx_socket_set(ysdc->sock,SpxKeepAlive,SpxAliveTimeout,\
                        SpxDetectTimes,SpxDetectTimeout,\
                        SpxLinger,SpxLingerTimeout,\
                        SpxNodelay,\
                        true,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "set socket to remote storage:%s,"
                    "host:%s:%d for dsync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }
        if(0 != (err = spx_socket_connect_nb(ysdc->sock,
                        s->host.ip,s->host.port,c->timeout))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "connect to remote storage:%s,"
                    "host:%s:%d for dsync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }
    }

    if(NULL == ysdc->request){
        ysdc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ysdc->request){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request to remote storage:%s,"
                    "host:%s:%d for dsync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }
    }

    if(NULL == ysdc->request->header){
        struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
        if(NULL == header){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request's header to remote storage:%s,"
                    "host:%s:%d for dsync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }
        header->protocol = YDB_S2S_DSYNC;
        header->bodylen = spx_string_len(fid);
        header->is_keepalive = false;//persistent connection
        ysdc->request->header = header;
    }

    if(NULL == ysdc->request->body) {
        struct spx_msg *body = spx_msg_new(ysdc->request->header->bodylen,&err);
        if(NULL == body){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new request's body to remote storage:%s,"
                    "host:%s:%d for dsync file:%s is fail.",
                    s->machineid,s->host.ip,s->host.port,
                    fid);
            goto r1;
        }
        ysdc->request->body = body;
        spx_msg_pack_string(body,fid);
    }

    err = spx_write_context_nb(c->log,ysdc->sock,ysdc->request);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "send request to remote storage:%s,"
                "host:%s:%d for dsync file:%s is fail.",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;

    }

    if(!spx_socket_read_timeout(ysdc->sock,c->timeout)){
        //timeout
        err = EAGAIN;
        SpxLogFmt1(c->log,SpxLogError,
                "response it timeout from  remote storage:%s,"
                "host:%s:%d for dsync file:%s .",
                s->machineid,s->host.ip,s->host.port,
                fid);
        goto r1;
    }

    if(NULL == ysdc->response){
        ysdc->response = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
        if(NULL == ysdc->response){
            SpxLog2(c->log,SpxLogError,err,
                    "new response for query sync storage is fail.");
            goto r1;
        }
    }

    ysdc->response->header =  spx_read_header_nb(c->log,ysdc->sock,&err);
    if(NULL == ysdc->response->header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "recving data for dsync fid:%s,"
                "from remote storage:%s,ip:%s,port:%d is fail.",
                fid,s->machineid,s->host.ip,s->host.port);
        goto r1;
    }

    err = ysdc->response->header->err;
    if(0 != err){
        if(ENOENT == err){
            SpxLogFmt1(c->log,SpxLogWarn,
                    "file:%s is not in the remote storage:%s,host:%s:%d.",
                    fid,s->machineid,s->host.ip,s->host.port);
        } else {
            SpxLogFmt2(c->log,SpxLogError,err,
                    "request file:%s from remote storage:%s,host:%s:%d is fail.",
                    fid,s->machineid,s->host.ip,s->host.port);
        }
        goto r1;
    }

    //store file
    if(0 == ysdc->fd){
        int fd = SpxWriteOpen(fname,false);
        if(0 >= fd){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open file:%s is fail.",
                    fname);
            goto r1;
        }
        ysdc->fd = fd;
        if(fidbuf->issinglefile) {
            if(0 > ftruncate(ysdc->fd,fidbuf->totalsize)){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "truncate file:%s is fail.",
                        fname);
                goto r1;
            }
            unit = (int) fidbuf->begin / c->pagesize;
            begin = unit * c->pagesize;
            offset = fidbuf->begin - begin;
            len = offset + fidbuf->totalsize;
        } else {
            if(0 > ftruncate(ysdc->fd,c->chunksize)){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "truncate file:%s is fail.",
                        fname);
                goto r1;
            }
            len = fidbuf->totalsize;

        }
        char *ptr = SpxMmap(fd,begin,len);
        if(MAP_FAILED == ptr){
            err = errno;
            SpxLogFmt2(c->log,SpxLogError,err,
                    "mmap fid:%s to file:%s is fail.",
                    fid,fname);
            goto r1;
        }
        ysdc->ptr = ptr;
        ysdc->len = len;
    }

    err = spx_lazy_mmap_nb(c->log,ysdc->ptr + offset,ysdc->sock,fidbuf->realsize,begin);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write file context to file:%s for fid:%s is s fail.",
                fname,fid);
        goto r1;
    }
r1:
    ydb_storage_sync_context_free(&ysdc);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_dsync_modify_request(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        string_t fid,
        string_t ofid
        ){/*{{{*/
    err_t err = 0;
    err = ydb_storage_dsync_delete_request(c,ofid);
    err = ydb_storage_dsync_upload_request(c,s,fid);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_dsync_data_to_remote_storage(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_remote *s,
        string_t fid
        ){/*{{{*/
    /*
    // 1: the file is exist ?
    // 2: if is singlefile => pass
    // 3: if is chunkfile open chunkile and check buff
    // 4: if isdelete is not and metadata is all full pass
    // 5 or the opver is high with fid pass
    // 5 else sync with fid
    err_t err  = 0;
    struct ydb_storage_sync_context *ysdc = NULL;
    ysdc = spx_alloc_alone(sizeof(*ysdc),&err);
    if(NULL == ysdc){
    SpxLogFmt2(c->log,SpxLogError,err,
    "alloc dsync remote object is fail.",
    "fid:%s.",fid);
    return err;
    }
    struct ydb_storage_fid *fidbuf =
    spx_alloc_alone(sizeof(struct ydb_storage_fid),&err);
    if(NULL == fidbuf){
    SpxLogFmt2(c->log,SpxLogError,err,
    "alloc fid object is fail.",
    "fid:%s.",fid);
    goto r1;
    }
    ysdc->fid = fidbuf;
    err = ydb_storage_dio_parser_fileid(c->log,fid,
    &(fidbuf->groupname),&(fidbuf->machineid),
    &(fidbuf->syncgroup),&(fidbuf->issinglefile),
    &(fidbuf->mpidx),&(fidbuf->p1),&(fidbuf->p2),
    &(fidbuf->tidx),&(fidbuf->fcreatetime),
    &(fidbuf->rand),&(fidbuf->begin),&(fidbuf->realsize),
    &(fidbuf->totalsize),&(fidbuf->ver),&(fidbuf->opver),
    &(fidbuf->lastmodifytime),&(fidbuf->hashcode),
    &(fidbuf->has_suffix),&(fidbuf->suffix));
    if(0 != err){
    SpxLogFmt2(c->log,SpxLogError,err,
    "parser fid:%s is fail.",
    fid);
    goto r1;
    }

    string_t fname = ydb_storage_dio_make_filename(c->log,
    fidbuf->issinglefile,c->mountpoints,fidbuf->mpidx,
    fidbuf->p1,fidbuf->p2,fidbuf->machineid,
    fidbuf->tidx,fidbuf->fcreatetime,fidbuf->rand,
    fidbuf->suffix,&err);
    if(NULL == fname){
    SpxLogFmt2(c->log,SpxLogError,err,
    "make fname by fid:%s  is fail.",
    fid);
    goto r1;
    }
    ysdc->fname = fname;

    u32_t unit = 0;
    u64_t begin = 0;
    u64_t offset = 0;
    u64_t len = 0;
    if(SpxFileExist(fname)){
    if(fidbuf->issinglefile){
    SpxLogFmt1(c->log,SpxLogDebug,
    "file:%s is not exist.",
    fname);
    goto r1;
    } else {
    int fd = SpxWriteOpen(filename,false);
    if(0 >= fd){
    SpxLogFmt2(c->log,SpxLogError,err,
    "open chunkfile:%s is fail.",
    fname);
    }
    ysdc->fd = fd;
    unit = (int) fidbuf->begin / c->pagesize;
    begin = unit * c->pagesize;
    offset = fidbuf->begin - begin;
    len = offset + fidbuf->totalsize;

    char *ptr = SpxMmap(fd,begin,len);
    if(NULL == ptr){

    }
    ysdc->ptr = ptr;

    struct spx_msg *ioctx = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);
    if(NULL == ioctx){
        goto r1;
    }
    ysdc->md = ioctx;

    if(0 != (err = spx_msg_pack_ubytes(ioctx,
                    ((ubyte_t *) (mptr+ offset)),
                    YDB_CHUNKFILE_MEMADATA_SIZE))){
        goto r1;
    }
    spx_msg_seek(ioctx,0,SpxMsgSeekSet);

    bool_t io_isdelete = false;
    u32_t io_opver = 0;
    u32_t io_ver = 0;
    u64_t io_createtime = 0;
    u64_t io_lastmodifytime = 0;
    u64_t io_totalsize = 0;
    u64_t io_realsize = 0;
    string_t io_suffix = NULL;
    string_t io_hashcode = NULL;

    err = ydb_storage_dio_parser_metadata(dc->log,ioctx,
            &io_isdelete,&io_opver,
            &io_ver,&io_createtime,
            &io_lastmodifytime,&io_totalsize,&io_realsize,
            &io_suffix,&io_hashcode);
    if(fidbuf->opver <= io_opver){
        //no sync

    }
}
} else {
    int fd = SpxWriteOpen(filename,false);
    if(0 >= fd){
        SpxLogFmt2(c->log,SpxLogError,err,
                "open chunkfile:%s is fail.",
                fname);
    }
    ysdc->fd = fd;
    if(fidbuf->issinglefile) {
        if(0 > ftruncate(ysdc->fd,filesize)){
        }
    } else {
        if(0 > ftruncate(ysdc->fd,c->chunksize)){
        }
    }
}

if(0 == ystc->sock) {
    ystc->sock  = spx_socket_new(&err);
    if(0 >= ystc->sock){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new socket to remote storage:%s,"
                "net address %s:%d is fail.",
                s->machineid,
                s->host.ip,s->host.port);
        goto r1;
    }

    if(0 != (err = spx_socket_set(ystc->sock,SpxKeepAlive,SpxAliveTimeout,\
                    SpxDetectTimes,SpxDetectTimeout,\
                    SpxLinger,SpxLingerTimeout,\
                    SpxNodelay,\
                    true,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "set socket to remote storage:%s,"
                "net address %s:%d is fail.",
                s->machineid,
                s->host.ip,s->host.port);
        goto r1;
    }
    if(0 != (err = spx_socket_connect_nb(ystc->sock,
                    s->host.ip,s->host.port,c->timeout))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "connect to remote storage:%s,"
                "address %s:%d is fail.",
                s->machineid,
                s->host.ip,s->host.port);
        goto r1;
    }
}

if(NULL == ystc->request){
    ystc->request = spx_alloc_alone(sizeof(struct spx_msg_context),&err);
    if(NULL == ystc->request){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request for sync logfile "
                "to remote storage %s address %s:%d is fail.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
}

if(NULL == ystc->request->header){
    struct spx_msg_header *header = spx_alloc_alone(sizeof(struct spx_msg_header),&err);
    if(NULL == header){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new request header for sync logfile "
                "to remote storage:%s,ip:%s,port:%d.",
                s->machineid,s->host.ip,s->host.port);
        goto r1;
    }
    header->protocol = protocol;
    header->bodylen = YDB_MACHINEID_LEN + spx_string_len(fid);
    header->is_keepalive = false;//persistent connection
    ystc->request->header = header;
}

if(NULL == ystc->request->body) {
    struct spx_msg *body = spx_msg_new(ystc->request->header->bodylen,&err);
    if(NULL == body){
        SpxLog2(c->log,SpxLogError,err,
                "new body of request for sync logfile is fail.");
        goto r1;
    }
    spx_msg_pack_fixed_string(body,c->machineid,YDB_MACHINEID_LEN);
    spx_msg_pack_string(ctx,fid);
}

err = spx_write_context_nb(c->log,ystc->sock,ystc->request);
if(0 != err){
    SpxLogFmt2(c->log,SpxLogError,err,
            "send request for sync logfile "
            "to remote storage:%s,ip:%s,port:%d is fail.",
            s->machineid,s->host.ip,s->host.port);
    goto r1;

}

if(!spx_socket_read_timeout(ystc->sock,c->timeout)){
    //timeout
    SpxLogFmt1(c->log,SpxLogError,
            "recving data for query mountpoint state "
            "from remote storage:%s,ip:%s,port:%d is timeout.",
            s->machineid,s->host.ip,s->host.port);
    goto r1;
}

ystc->response->header =  spx_read_header_nb(c->log,ystc->sock,&err);
if(NULL == ystc->response->header){
    SpxLogFmt2(c->log,SpxLogError,err,
            "recving data for query mountpoint state "
            "from remote storage:%s,ip:%s,port:%d is fail.",
            s->machineid,s->host.ip,s->host.port);
    goto r1;
}

err = ystc->response->header->err;
if(0 != err){
    if(ENOENT == err){
        SpxLogFmt1(c->log,SpxLogWarn,
                "logfile of machineid:%s date:%d-%d-%d is not exist.",
                machineid,dtlogf->year,dtlogf->month,,
                dtlogf->day);
    } else {
        SpxLogFmt1(c->log,SpxLogWarn,
                "get logfile of machineid:%s date:%d-%d-%d is fail.",
                machineid,dtlogf->year,dtlogf->month,,
                dtlogf->day);
    }
    goto r1;
}

if(fidbuf->issinglefile) {
    ysdc->fd  = SpxWriteOpen(logfname,true);
    if(0 >= ysdc->fd){
        err = 0 == errno ? EACCES : errno;
        SpxLogFmt2(c->log,SpxLogWarn,err
                "create logfile of machineid:%s date:%d-%d-%d is fail.",
                machineid,dtlogf->year,dtlogf->month,,
                dtlogf->day);
        goto r1;
    }
    len = ystc->response->header->bodylen - ystc->response->header->offset;
    if(0 > ftruncate(ysdc->fd,filesize)){
        err = 0 == errno ? EACCES : errno;
        SpxLogFmt2(c->log,SpxLogWarn,err
                "change logfile filesize of machineid:%s date:%d-%d-%d is fail.",
                machineid,dtlogf->year,dtlogf->month,,
                dtlogf->day);
        goto r1;
    }

    ptr = mmap(NULL,\
            filesize,PROT_READ | PROT_WRITE ,\
            MAP_SHARED,ysdc->fd,0);
    if(MAP_FAILED == ptr){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,\
                "mmap the logfile file machineid:%s "
                "date:%d-%d-%d to memory is fail.",
                c->machineid,binlog_dt->year,
                binlog_dt->month,binlog_dt->day);
        goto r1;
    }
    ysdc->ptr = ptr;
}

err = spx_lazy_mmap_nb(c->log,ysdc->ptr + offset,ystc->sock,len,0);
if(0 != err){
    SpxLogFmt2(c->log,SpxLogError,err,\
            "write the logfile file machineid:%s "
            "date:%d-%d-%d to memory is fail.",
            c->machineid,binlog_dt->year,
            binlog_dt->month,binlog_dt->day);
}

r1:
SpxClose(ystc->fd);
SpxClose(binlog_fd);
if(NULL != ptr){
    munmap(ptr,filesize);
}
if(0 != err){
    if(SpxFileExist(logfname)){
        SpxLogFmt2(c->log,SpxLogError,err,\
                "write the logfile file machineid:%s "
                "date:%d-%d-%d to memory is fail."
                " and remove it.",
                c->machineid,binlog_dt->year,
                binlog_dt->month,binlog_dt->day);
        remove(logfname);
    }
}
if(NULL != ystc && NULL != ystc->request){
    if(NULL !=ystc->request->header){
        SpxFree(ystc->request->header);
    }
    if(NULL != ystc->request->body){
        SpxFree(ystc->request->body);
    }
}
if(NULL != ystc && NULL != ystc->response){
    if(NULL != ystc->response->header){
        SpxFree(ystc->response->header);
    }
    if(NULL != ystc->response->body){
        SpxMsgFree(ystc->response->body);
    }
    SpxFree(ystc->response);
}
return err;
*/
return 0;
}/*}}}*/

spx_private void ydb_storage_dsync_over(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt
        ){/*{{{*/
    err_t err = 0;
    struct spx_map_iter *iter = spx_map_iter_new(g_ydb_storage_remote,&(err));
    if(NULL == iter){
        SpxLog2(c->log,SpxLogError,err,\
                "init iter of remote storage is fail.");
        return;
    }
    struct spx_map_node *n = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&err))){
        if(NULL != n && NULL != n->v){
            SpxTypeConvert2(struct ydb_storage_remote,s,n->v);
            s->synclog.d.year = dsrt->sync_date_curr.year;
            s->synclog.d.month = dsrt->sync_date_curr.month;
            s->synclog.d.day = dsrt->sync_date_curr.day;
            s->synclog.off = 0;//can set offset ,because maybe the storage is offline
            //and the  offset must send by csync begin and with synclog file
            //stat(fname) for file size
        }
    }
    spx_map_iter_free(&iter);
    return;
}/*}}}*/

spx_private struct ydb_storage_dsync_runtime *ydb_storage_dsync_runtime_reader(
        struct ydb_storage_configurtion *c,
        err_t *err
        ){/*{{{*/
    struct ydb_storage_dsync_runtime  *dsrt = NULL;
    FILE *fp = NULL;
    string_t line = NULL;
    string_t fname = ydb_storage_dsync_make_runtime_filename(c,err);
    if(NULL == fname){
        SpxLog2(c->log,SpxLogError,*err,
                "make dsync runtime filename is fail.");
        return NULL;
    }

    if(!SpxFileExist(fname)){
        *err = ENOENT;
        goto r2;
    }

    dsrt = spx_alloc_alone(sizeof(*dsrt),err);
    if(NULL == dsrt){
        SpxLog2(c->log,SpxLogError,*err,
                "new dsync runtime is fail.");
        goto r2;
    }
    dsrt->c = c;

    line = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),err);
    if(NULL == line){
        SpxLog2(c->log,SpxLogError,*err,
                "new line for parsering dsync runtime file is fail.");
        goto r1;
    }

    fp = SpxFReadOpen(fname);
    if(NULL == fp){
        *err = 0 == errno ? EACCES : errno;
        SpxLogFmt2(c->log,SpxLogError,*err,
                "open dsync runtime file:%s is fail.",
                fname);
        goto r1;
    }

    int i = 0;
    while(NULL != (fgets(line,SpxLineSize,fp))){
        spx_string_updatelen(line);
        spx_string_strip_linefeed(line);
        if('#' == *line || SpxStringIsEmpty(line)){
            continue;
        }
        switch(i){
            case (0):{
                         dsrt->step = atoi(line);
                         i++;
                         break;
                     }
            case (1):{
                         spx_date_convert(c->log,
                                 &(dsrt->sync_date_curr),line,"YYYY-MM-DD",err);
                         i++;
                         break;
                     }
            case (2):{
                         dsrt->offset = atoll(line);
                         i++;
                         break;
                     }
            case (3):{
                         dsrt->last_linelen = atoll(line);
                         i++;
                         break;
                     }
            case (4):{
                         spx_date_convert(c->log,
                                 &(dsrt->sync_date_curr),line,"YYYY-MM-DD",err);
                         i++;
                         break;
                     }
            case (5):{
                         dsrt->begin_dsync_timespan = atoll(line);
                         i++;
                         break;
                     }
            case (6):{
                         int i = 0;
                         for( ; i < YDB_STORAGE_MOUNTPOINT_MAXSIZE; i++){
                             if('1' == *(line + i)){
                                 (dsrt->ydb_storage_dsync_mps + i)->need_dsync = true;
                             }
                         }
                         i++;
                         break;
                     }
            default:{
                        break;
                    }
        }
        spx_string_clear(line);
    }
    goto r2;
r1:
    if(NULL != dsrt){
        SpxFree(dsrt);
    }
r2:
    if(NULL != fname){
        SpxStringFree(fname);
    }
    if(NULL != fp){
        fclose(fp);
        fp = NULL;
    }
    if(NULL != line){
        SpxStringFree(line);
    }
    return dsrt;
}/*}}}*/

spx_private void *ydb_storage_dsync_runtime_writer(
        void *arg
        ){/*{{{*/
    if(NULL == arg){
        return NULL;
    }
    SpxTypeConvert2(struct ydb_storage_dsync_runtime,dsrt,arg);
    struct ydb_storage_configurtion *c = dsrt->c;
    err_t err = 0;
    FILE *fp = NULL;
    string_t line = NULL;
    string_t fname = ydb_storage_dsync_make_runtime_filename(c,&err);
    if(NULL == fname){
        SpxLog2(c->log,SpxLogError,err,
                "make dsync runtime filename is fail.");
        return NULL;
    }

    line = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),&err);
    if(NULL == line){
        SpxLog2(c->log,SpxLogError,err,
                "new line for parsering dsync runtime file is fail.");
        goto r1;
    }

    string_t new_line = spx_string_cat_printf(&err,line,
            "%d\n%d-%d-%d\n%lld\n%lld\n%d-%d-%d\n%lld\n",
            dsrt->step,dsrt->sync_date_curr.year,
            dsrt->sync_date_curr.month,
            dsrt->sync_date_curr.day,
            dsrt->offset,dsrt->last_linelen,
            dsrt->dsync_over_of_mps.year,
            dsrt->dsync_over_of_mps.month,
            dsrt->dsync_over_of_mps.day,
            dsrt->begin_dsync_timespan);
    if(NULL == new_line){
        SpxLog2(c->log,SpxLogError,err,
                "make context for dsync runtime file is fail.");
        goto r1;
    }
    line = new_line;
    int i = 0;
    for( ; i < YDB_STORAGE_MOUNTPOINT_MAXSIZE; i++){
        new_line = spx_string_cat(line,
                (dsrt->ydb_storage_dsync_mps + i)->need_dsync ? "1" :"0",
                &err);
        if(NULL == new_line){
            SpxLog2(c->log,SpxLogError,err,
                    "make context for dsync runtime file is fail.");
            break;
        }
        line = new_line;
    }


    fp = SpxFWriteOpen(fname,true);
    if(NULL == fp){
        err = 0 == errno ? EACCES : errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "open dsync runtime file:%s is fail.",
                fname);
        goto r1;
    }

    size_t size = spx_string_len(line);
    size_t len = 0;
    err = spx_fwrite_string(fp,line,size,&len);
    if(0 != err || size != len){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write dsync runtime file is fail."
                "size:%d,len:%d",size,len);
    }
    fclose(fp);
r1:
    if(NULL != line){
        SpxStringFree(line);
    }
    if(NULL != fname){
        SpxStringFree(fname);
    }
    return NULL;
}/*}}}*/

spx_private string_t ydb_storage_dsync_make_runtime_filename(
        struct ydb_storage_configurtion *c,
        err_t *err
        ){/*{{{*/
    string_t fname = spx_string_newlen(NULL,SpxStringRealSize(SpxPathSize),err);
    if(NULL == fname){
        SpxLog2(c->log,SpxLogError,*err,
                "new dsync runtime filename is fail.");
        return NULL;
    }
    string_t new_fname = NULL;
    if(SpxStringEndWith(c->basepath,SpxPathDlmt)){
        new_fname = spx_string_cat_printf(err,fname,"%s.%s-dsync.rtf",
                c->basepath,c->machineid);
    } else {
        new_fname = spx_string_cat_printf(err,fname,"%s%c.%s-dsync.rtf",
                c->basepath,SpxPathDlmt,c->machineid);
    }
    if(NULL == new_fname){
        SpxLog2(c->log,SpxLogError,*err,
                "make dsync runtime filename is fail.");
        SpxStringFree(fname);
        return NULL;
    }
    fname = new_fname;
    return fname;
}/*}}}*/

spx_private void ydb_storage_dsync_runtime_file_remove(
        struct ydb_storage_configurtion *c
        ){/*{{{*/
    err_t err = 0;
    string_t fname = ydb_storage_dsync_make_runtime_filename(c,&err);
    if(NULL == fname){
        SpxLog2(c->log,SpxLogError,err,
                "make dsync runtime file to remove is fail.");
        return;
    }
    remove(fname);
    SpxStringFree(fname);
    return;
}/*}}}*/

spx_private void ydb_storage_dsync_mountpoint_over(
        struct ydb_storage_dsync_runtime  *dsrt
        ){/*{{{*/
    int i = 0;
    for( ; i < YDB_STORAGE_MOUNTPOINT_MAXSIZE; i++){
        struct ydb_storage_dsync_mp *mp = dsrt->ydb_storage_dsync_mps + i;
        if(mp->need_dsync){
            struct ydb_storage_mountpoint *mp = spx_list_get(dsrt->c->mountpoints,i);
            if(mp->isusing){
                mp->last_freesize = mp->freesize;
            }
        }
    }
    return;
}/*}}}*/

spx_private err_t ydb_storage_dsync_total_mounpoint_sync_timespan(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_dsync_runtime *dsrt
        ){/*{{{*/
    err_t err = 0;
    string_t fname = NULL;
    if(0 == dsrt->dsync_over_of_mps.year){
        fname = ydb_storage_make_runtime_filename(c,&err);
        if(NULL == fname){
            SpxLog2(c->log,SpxLogError,err,
                    "make runtime filename is fail.");
            return err;
        }
        if(SpxFileExist(fname)){
            struct stat buf;
            SpxZero(buf);
            stat(fname,&buf);
            spx_get_date((time_t *) &(buf.st_mtime),&(dsrt->dsync_over_of_mps));
        } else {
            spx_get_today(&(dsrt->dsync_over_of_mps));
        }
        SpxStringFree(fname);
    }
    return err;
}/*}}}*/


//server
err_t ydb_storage_dsync_sync_data(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc
        ){/*{{{*/
    err_t err = 0;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    struct spx_msg *ctx = jc->reader_body_ctx;
    size_t len = jc->reader_header->bodylen;

    dc->rfid = spx_msg_unpack_string(ctx,len,&(err));
    if(NULL == dc->rfid){
        SpxLog2(c->log,SpxLogError,err,\
                "alloc file id for parser is fail.");
        goto r1;
    }

    if(0 != ( err = ydb_storage_dio_parser_fileid(jc->log,dc->rfid,
                    &(dc->groupname),&(dc->machineid),&(dc->syncgroup),
                    &(dc->issinglefile),&(dc->mp_idx),&(dc->p1),
                    &(dc->p2),&(dc->tidx),&(dc->file_createtime),
                    &(dc->rand),&(dc->begin),&(dc->realsize),
                    &(dc->totalsize),&(dc->ver),&(dc->opver),
                    &(dc->lastmodifytime),&(dc->hashcode),
                    &(dc->has_suffix),&(dc->suffix)))){

        SpxLog2(dc->log,SpxLogError,err,\
                "parser fid is fail.");
        goto r1;
    }

    dc->filename = ydb_storage_dio_make_filename(dc->log,
            dc->issinglefile,c->mountpoints,
            dc->mp_idx,dc->p1,dc->p2,
            dc->machineid,dc->tidx,dc->file_createtime,
            dc->rand,dc->suffix,&err);
    if(NULL == dc->filename){
        SpxLog2(dc->log,SpxLogError,err,\
                "make filename is fail.");
        goto r1;
    }

    if(!SpxFileExist(dc->filename)) {
        err = ENOENT;
        SpxLogFmt1(dc->log,SpxLogWarn,\
                "deleting-file:%s is not exist.",
                dc->filename);
        goto r1;
    }

    if(dc->issinglefile){
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_dsync_data_from_singlefile,dc);
    } else {
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_dsync_data_from_chunkfile,dc);
    }
    ev_async_start(loop,&(dc->async));
    ev_async_send(loop,&(dc->async));
    return err;
r1:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail."
                "no notify client and push jc force.");
        spx_job_pool_push(g_spx_job_pool,jc);
        return err;
    }
    jc->writer_header->protocol = YDB_S2S_DSYNC;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return 0;
}/*}}}*/

spx_private void ydb_storage_dsync_data_from_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents
        ){/*{{{*/
    ev_async_stop(loop,w);
    err_t err = 0;
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    u32_t unit = (int) dc->begin / c->pagesize;
    u64_t begin = unit * c->pagesize;
    u64_t offset = dc->begin - begin;
    u64_t len = offset + dc->totalsize;

    int fd = open(dc->filename,
            O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
    if(0 >= fd){
        err = errno;
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "open chunkfile:%s is fail.",
                dc->filename);
        goto r1;
    }

    char *mptr = mmap(NULL,\
            len,PROT_READ | PROT_WRITE ,\
            MAP_SHARED,fd,begin);
    if(MAP_FAILED == mptr){
        err = errno;
        SpxClose(fd);
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "mmap chunkfile:%s is fail.",
                dc->filename);
        goto r1;
    }

    struct spx_msg *ioctx = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);
    if(NULL == ioctx){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc io ctx is fail.");
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }

    if(0 != (err = spx_msg_pack_ubytes(ioctx,
                    ((ubyte_t *) (mptr+ offset)),
                    YDB_CHUNKFILE_MEMADATA_SIZE))){
        SpxLog2(dc->log,SpxLogError,err,\
                "pack io ctx is fail.");
        SpxMsgFree(ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }
    spx_msg_seek(ioctx,0,SpxMsgSeekSet);
    bool_t io_isdelete = false;
    u32_t io_opver = 0;
    u32_t io_ver = 0;
    u64_t io_createtime = 0;
    u64_t io_lastmodifytime = 0;
    u64_t io_totalsize = 0;
    u64_t io_realsize = 0;
    string_t io_suffix = NULL;
    string_t io_hashcode = NULL;

    err = ydb_storage_dio_parser_metadata(dc->log,ioctx,
            &io_isdelete,&io_opver,
            &io_ver,&io_createtime,
            &io_lastmodifytime,&io_totalsize,&io_realsize,
            &io_suffix,&io_hashcode);
    if(0 != err){
        SpxLog2(dc->log,SpxLogError,err,\
                "unpack io ctx is fail.");
        SpxMsgFree(ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }

    if(io_isdelete){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "the file in the chunkfile:%s "
                "begin is %lld totalsize:%lld is deleted.",
                dc->filename,dc->begin,dc->totalsize);
        err = ENOENT;
        SpxMsgFree(ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }
    if(dc->opver != io_opver || dc->ver != io_ver
            || dc->lastmodifytime != io_lastmodifytime
            || dc->totalsize != io_totalsize
            || dc->realsize != io_realsize){
        SpxLog2(dc->log,SpxLogError,err,\
                "the file is not same as want to delete-file.");
        err = ENOENT;
        SpxMsgFree(ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }

    struct spx_msg_header *wh = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*wh),&err);
    if(NULL == wh){
        SpxLog2(dc->log,SpxLogError,err,
                "alloc write header for find buffer in chunkfile is fail.");
        SpxMsgFree(ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }

    jc->moore = SpxNioMooreResponse;
    jc->writer_header =wh;
    wh->version = YDB_VERSION;
    jc->writer_header->protocol = YDB_S2S_DSYNC;
    wh->offset = 0;
    wh->bodylen = dc->realsize + YDB_CHUNKFILE_MEMADATA_SIZE;

    if(c->sendfile){
        jc->is_sendfile = true;
        jc->sendfile_fd = fd;
        jc->sendfile_begin = dc->begin ;
        jc->sendfile_size =YDB_CHUNKFILE_MEMADATA_SIZE +  dc->realsize;
    } else {
        jc->is_sendfile = false;
        struct spx_msg *ctx = spx_msg_new(
                YDB_CHUNKFILE_MEMADATA_SIZE + dc->realsize,&err);
        if(NULL == ctx){
            SpxLog2(dc->log,SpxLogError,err,\
                    "alloc buffer ctx for finding writer is fail.");
            SpxMsgFree(ioctx);
            munmap(mptr,len);
            SpxClose(fd);
            goto r1;
        }
        jc->writer_body_ctx = ctx;
        spx_msg_pack_ubytes(ctx,
                (ubyte_t *)mptr + begin,
                YDB_CHUNKFILE_MEMADATA_SIZE + dc->realsize);
        SpxClose(fd);
    }

    munmap(mptr,len);
    SpxMsgFree(ioctx);
    goto r2;
r1:

    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail."
                "no notify client and push jc force.");
        spx_task_pool_push(g_spx_task_pool,tc);
        ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
        spx_job_pool_push(g_spx_job_pool,jc);
        return;
    }
    jc->writer_header->protocol = YDB_S2S_DSYNC;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;
r2:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

spx_private void ydb_storage_dsync_data_from_singlefile(
        struct ev_loop *loop,ev_async *w,int revents
        ){/*{{{*/
    ev_async_stop(loop,w);
    err_t err = 0;
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    struct stat buf;
    memset(&buf,0,sizeof(buf));
    if(0 != lstat (dc->filename,&buf)){
        err = errno;
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "get signalfile:%s stat is fail.",
                dc->filename);
        goto r1;
    }

    if((u64_t) buf.st_size != dc->realsize){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "file:%s file size is %lld,and request size is %lld.",
                dc->filename,buf.st_size,dc->realsize);
        goto r1;
    }

    struct spx_msg_header *wh = (struct spx_msg_header *) \
                                spx_alloc_alone(sizeof(*wh),&err);
    if(NULL == wh){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc write header for find buffer in chunkfile is fail.");
        goto r1;
    }

    jc->writer_header =wh;
    jc->moore = SpxNioMooreResponse;
    wh->version = YDB_VERSION;
    jc->writer_header->protocol = YDB_S2S_DSYNC;
    wh->offset = 0;
    wh->bodylen = dc->realsize;

    int fd = open(dc->filename,\
            O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
    if(0 >= fd){
        err = errno;
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "open chunkfile:%s is fail.",
                dc->filename);
        goto r1;
    }

    if(c->sendfile){
        jc->is_sendfile = true;
        jc->sendfile_fd = fd;
        jc->sendfile_begin = 0;
        jc->sendfile_size = dc->realsize;
    } else {
        jc->is_sendfile = false;
        struct spx_msg *ctx = spx_msg_new(dc->realsize,&err);
        if(NULL == ctx){
            SpxLog2(dc->log,SpxLogError,err,\
                    "alloc buffer ctx for finding writer is fail.");
            SpxClose(fd);
            goto r1;
        }
        jc->writer_body_ctx = ctx;
        char *mptr = mmap(NULL,\
                dc->realsize,PROT_READ | PROT_WRITE ,\
                MAP_SHARED,fd,0);
        if(MAP_FAILED == mptr){
            err = errno;
            SpxClose(fd);
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "mmap chunkfile:%s is fail.",
                    dc->filename);
            goto r1;
        }
        spx_msg_pack_ubytes(ctx,(ubyte_t *)mptr,dc->realsize);
        munmap(mptr,dc->realsize);
        SpxClose(fd);
    }
    goto r2;
r1:
    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail."
                "no notify client and push jc force.");
        spx_task_pool_push(g_spx_task_pool,tc);
        ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
        spx_job_pool_push(g_spx_job_pool,jc);
        return;
    }
    jc->writer_header->protocol = YDB_S2S_DSYNC;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;
r2:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

err_t ydb_storage_dsync_logfile(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc
        ){/*{{{*/
    err_t err = 0;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    struct spx_msg *ctx = jc->reader_body_ctx;

    dc->machineid = spx_msg_unpack_string(ctx,YDB_MACHINEID_LEN,&err);
    dc->date = spx_alloc_alone(sizeof(struct spx_date),&err);
    if(NULL == dc->date){
        SpxLog2(c->log,SpxLogError,err,
                "new dsync logfile date is fail.");
        goto r1;
    }

    dc->date->year = spx_msg_unpack_u32(ctx);
    dc->date->month = spx_msg_unpack_u32(ctx);
    dc->date->day = spx_msg_unpack_u32(ctx);

    if(spx_string_casecmp_string(dc->machineid,c->machineid)){
        dc->filename = ydb_storage_binlog_make_filename(c->log,
                c->dologpath,dc->machineid,dc->date->year,
                dc->date->month,dc->date->day,&err);
    } else {
        dc->filename = ydb_storage_synclog_make_filename(c->log,
                c->dologpath,dc->machineid,dc->date->year,
                dc->date->month,dc->date->day,&err);
    }

    if(NULL == dc->filename){
        SpxLogFmt2(c->log,SpxLogError,err,
                "make logfile name by mahineid:%s and date:%d-%d-%d is fail.",
                dc->machineid,dc->date->year,dc->date->month,dc->date->day);
        goto r1;
    }

    if(!SpxFileExist(dc->filename)) {
        err = ENOENT;
        SpxLogFmt1(dc->log,SpxLogWarn,\
                "logfile for machineid:%s date:%d-%d-%d is not exist.",
                dc->machineid,dc->date->year,dc->date->month,dc->date->day);
        goto r1;
    }

    spx_dio_regedit_async(&(dc->async),
            ydb_storage_dsync_logfile_context,dc);
    ev_async_start(loop,&(dc->async));
    ev_async_send(loop,&(dc->async));
    return err;
r1:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail."
                "no notify client and push jc force.");
        spx_job_pool_push(g_spx_job_pool,jc);
        return err;
    }
    jc->writer_header->protocol = YDB_S2S_SYNC_LOGFILE;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return 0;
}/*}}}*/

spx_private void ydb_storage_dsync_logfile_context(
        struct ev_loop *loop,ev_async *w,int revents
        ){/*{{{*/
    ev_async_stop(loop,w);
    err_t err = 0;
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    struct stat buf;
    memset(&buf,0,sizeof(buf));
    if(0 != lstat (dc->filename,&buf)){
        err = errno;
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "get signalfile:%s stat is fail.",
                dc->filename);
        goto r1;
    }

    size_t len = buf.st_size;
    size_t begin = 0;
    if(0 == len){
        SpxLogFmt1(c->log,SpxLogWarn,
                "logfile:%s size is 0 and context is empty.",
                dc->filename);
        goto r1;
    }

    int fd = open(dc->filename,
            O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
    if(0 >= fd){
        err = errno;
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "open logfile:%s is fail.",
                dc->filename);
        goto r1;
    }

    char *mptr = mmap(NULL,\
            len,PROT_READ | PROT_WRITE ,\
            MAP_SHARED,fd,begin);
    if(MAP_FAILED == mptr){
        err = errno;
        SpxClose(fd);
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "mmap logfile:%s is fail.",
                dc->filename);
        goto r1;
    }


    struct spx_msg_header *wh = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == wh){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail."
                "no notify client and push jc force.");
        goto r1;
    }
    jc->writer_header =wh;
    jc->moore = SpxNioMooreResponse;
    wh->version = YDB_VERSION;
    wh->protocol = YDB_S2S_SYNC_LOGFILE;
    wh->offset = 0;
    wh->bodylen = len;
    dc->realsize = len;

    if(c->sendfile){
        jc->is_sendfile = true;
        jc->sendfile_fd = fd;
        jc->sendfile_begin = dc->begin ;
        jc->sendfile_size = dc->realsize;
    } else {
        jc->is_sendfile = false;
        struct spx_msg *ctx = spx_msg_new(dc->realsize,&err);
        if(NULL == ctx){
            SpxLog2(dc->log,SpxLogError,err,\
                    "alloc buffer ctx for dsyncing logfile writer is fail.");
            munmap(mptr,len);
            SpxClose(fd);
            goto r1;
        }
        jc->writer_body_ctx = ctx;
        spx_msg_pack_ubytes(ctx,
                (ubyte_t *)mptr + begin,
                dc->realsize);
        SpxClose(fd);
    }

    munmap(mptr,len);
    goto r2;
r1:

    if(NULL == jc->writer_header){
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    }
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail."
                "no notify client and push jc force.");
        spx_task_pool_push(g_spx_task_pool,tc);
        ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
        spx_job_pool_push(g_spx_job_pool,jc);
        return;
    }
    jc->writer_header->protocol = YDB_S2S_SYNC_LOGFILE;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;
r2:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/


