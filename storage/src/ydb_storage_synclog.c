/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_synclog.c
 *        Created:  2014/08/12 14时19分46秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <ev.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "spx_types.h"
#include "spx_defs.h"
#include "spx_alloc.h"
#include "spx_string.h"
#include "spx_io.h"
#include "spx_path.h"
#include "spx_time.h"
#include "spx_thread.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_synclog.h"
#include "ydb_storage_runtime.h"

//struct ydb_storage_synclog *g_ydb_storage_synclog = NULL;

spx_private err_t ydb_storage_synclog_open(struct ydb_storage_synclog *synclog);
spx_private err_t ydb_storage_synclog_reopen(struct ydb_storage_synclog *synclog);

spx_private err_t ydb_storage_synclog_reopen(struct ydb_storage_synclog *synclog){/*{{{*/
    if(NULL != synclog->fp){
        fflush(synclog->fp);
        fclose(synclog->fp);
        synclog->off = 0;
        if(NULL != synclog->filename) {
            SpxStringFree(synclog->filename);
        }
    }

    synclog->filename = ydb_storage_synclog_make_filename(synclog->log,synclog->path,
            synclog->machineid,synclog->d.year,synclog->d.month,synclog->d.day,&(synclog->err));
    if(NULL == synclog->filename){
        SpxLog2(synclog->log,SpxLogError,synclog->err,
                "make synclog filename is fail.");
        return synclog->err;
    }

    synclog->fp = fopen(synclog->filename,"a+");
    if(NULL == synclog->fp){
        synclog->err = errno;
        SpxLogFmt2(synclog->log,SpxLogError,synclog->err,\
                "open synclog file:%s is fail.",\
                synclog->filename);
        SpxStringFree(synclog->filename);
        return synclog->err;
    }
    setlinebuf(synclog->fp);
    synclog->off = 0;
    return 0;
}/*}}}*/

spx_private err_t ydb_storage_synclog_open(struct ydb_storage_synclog *synclog){/*{{{*/
    //skip synclog but no flush state into disk
    synclog->filename = ydb_storage_synclog_make_filename(synclog->log,synclog->path,
            synclog->machineid,synclog->d.year,synclog->d.month,synclog->d.day,&(synclog->err));
    if(NULL == synclog->filename){
        SpxLog2(synclog->log,SpxLogError,synclog->err,
                "make synclog filename is fail.");
        return synclog->err;
    }

    struct stat buf;
    memset(&buf,0,sizeof(buf));
    if(SpxFileExist(synclog->filename)){
        stat(synclog->filename,&buf);
    }

    string_t basepath = spx_basepath(synclog->filename,&(synclog->err));
    if(NULL == basepath || 0 != synclog->err){
        SpxLogFmt2(synclog->log,SpxLogError,synclog->err,
                "get basepath from filename:%s is fail.",
                synclog->filename);
        SpxStringFree(synclog->filename);
        return synclog->err;
    }
    if(!spx_is_dir(basepath,&(synclog->err))){
        spx_mkdir(synclog->log,basepath,SpxPathMode);
    }
    SpxStringFree(basepath);

    synclog->fp = fopen(synclog->filename,"a+");
    if(NULL == synclog->fp){
        synclog->err = errno;
        SpxLogFmt2(synclog->log,SpxLogError,synclog->err,\
                "open synclog file:%s is fail.",\
                synclog->filename);
        SpxStringFree(synclog->filename);
        return synclog->err;
    }
    if(0 != buf.st_size) {
        fseek(synclog->fp,buf.st_size,SEEK_CUR);
        synclog->off = buf.st_size;
    }
    setlinebuf(synclog->fp);
    return 0;
}/*}}}*/

err_t ydb_storage_synclog_init(
        struct ydb_storage_synclog *synclog,
        SpxLogDelegate *log,
        string_t path,string_t machineid, int year,
        int month,int day,u64_t offset){/*{{{*/
    err_t err = 0;
    if(NULL == synclog){
        SpxLog2(log,SpxLogError,err,\
                "new storage synclog is fail.");
        return err;
    }

    synclog->log = log;
    synclog->path = path;
    synclog->machineid = machineid;
    synclog->d.year = year;
    synclog->d.month = month;
    synclog->d.day = day;
    if(0 != (err = ydb_storage_synclog_open(synclog))){
        SpxLogFmt2(log,SpxLogError,err,
                "open synclog:%s is fail.",
                synclog->filename);
        return err;
    }
    if(0 != offset){
        if(0 != fseek(synclog->fp,offset,SEEK_SET)){
            err = errno;
            SpxLogFmt2(log,SpxLogError,err,
                    "seek synclog file pointer to %d is fail."
                    "reset begining with 0.",
                    offset);
        } else {
            synclog->off = offset;
        }
    }
    return err;;
}/*}}}*/

void  ydb_storage_synclog_write(struct ydb_storage_synclog *synclog,
        time_t log_time, char op,string_t fid,string_t rfid){/*{{{*/
    err_t err = 0;
    string_t loginfo = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == loginfo){
        SpxLog2(synclog->log,SpxLogError,err,\
                "new loginfo line is fail.");
        return;
    }

    if(YDB_STORAGE_LOG_MODIFY == op){
        spx_string_cat_printf(&err,loginfo,\
                "%c\t%s\t%s\n",
                op,fid,rfid);
    } else {
        spx_string_cat_printf(&err,loginfo,\
                "%c\t%s\n",
                op,fid);
    }

    size_t size = spx_string_len(loginfo);
    if(log_time != spx_zero(&(synclog->d))){
        spx_get_date(&log_time,&(synclog->d));
        ydb_storage_synclog_reopen(synclog);
    }
    size_t len = 0;
    synclog->err = spx_fwrite_string(synclog->fp,loginfo,size,&len);
    if(0 != synclog->err || size != len){
        SpxLogFmt2(synclog->log,SpxLogError,synclog->err,\
                "write synclog is fail.loginfo:%s,size:%lld,realsize:%lld.",
                loginfo,size,len);
    }
    synclog->off += len;
    SpxStringFree(loginfo);
    return;
}/*}}}*/

void ydb_storage_synclog_free(struct ydb_storage_synclog **synclog){/*{{{*/
    if(NULL != (*synclog)->fp){
        fflush((*synclog)->fp);
        fclose((*synclog)->fp);
        (*synclog)->off = 0;
        if(NULL != (*synclog)->filename) {
            SpxStringFree((*synclog)->filename);
        }
    }
    SpxFree(*synclog);
}/*}}}*/


void ydb_storage_synclog_clear(struct ydb_storage_synclog *synclog){/*{{{*/
    if(NULL != (synclog)->fp){
        fflush((synclog)->fp);
        fclose((synclog)->fp);
        (synclog)->off = 0;
    }
    if(NULL != (synclog)->filename) {
        SpxStringFree((synclog)->filename);
    }
    if(NULL != synclog->path) {
        SpxStringFree(synclog->path);
    }
    if(NULL != synclog->machineid) {
        SpxStringFree(synclog->machineid);
    }
    SpxZero(synclog->d);
}/*}}}*/

string_t ydb_storage_synclog_make_filename(SpxLogDelegate *log,string_t path,string_t machineid,
        int year,int month,int day,err_t *err){/*{{{*/
    string_t filename = spx_string_newlen(NULL,SpxPathSize,err);
    if(NULL == filename){
        SpxLog2(log,SpxLogError,*err,
                "alloc synclog filename is fail.");
        return NULL;
    }
    string_t new_filename = spx_string_cat_printf(err,filename,
            "%s%s.synclog/%s/%04d-%02d-%02d.log",\
            path,SpxStringEndWith(path,SpxPathDlmt) ? "" : SpxPathDlmtString,\
            machineid,year,month,day);
    if(NULL == new_filename){
        SpxLog2(log,SpxLogError,*err,
                "cat synclog filename is fail.");
        SpxStringFree(filename);
        return NULL;
    }
    filename = new_filename;
    return filename;
}/*}}}*/


