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
    spx_get_today(&(synclog->d));

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

struct ydb_storage_synclog *ydb_storage_synclog_new(SpxLogDelegate *log,\
        string_t path,string_t machineid,
        err_t *err){/*{{{*/
    struct ydb_storage_synclog *synclog = (struct ydb_storage_synclog *)\
                                        spx_alloc_alone(sizeof(*synclog),err);
    if(NULL == synclog){
        SpxLog2(log,SpxLogError,*err,\
                "new storage synclog is fail.");
        return NULL;
    }

    synclog->log = log;
    synclog->path = path;
    synclog->machineid = machineid;
    synclog->mlock = spx_thread_mutex_new(log,err);
    if(NULL == synclog->mlock){
        goto r1;
    }
    if(0 != (*err = ydb_storage_synclog_open(synclog))){
        SpxLogFmt2(log,SpxLogError,*err,
                "open synclog:%s is fail.",
                synclog->filename);
        goto r1;
    }
    return synclog;
r1:
    ydb_storage_synclog_free(&synclog);
    return NULL;
}/*}}}*/

void ydb_storage_synclog_write(struct ydb_storage_synclog *synclog,\
        char op,string_t fid,string_t rfid){/*{{{*/
    if(NULL == synclog){
        return;
    }

    err_t err = 0;
    string_t loginfo = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == loginfo){
        SpxLog2(synclog->log,SpxLogError,err,\
                "new loginfo line is fail.");
        return;
    }

    if(YDB_STORAGE_MODIFY == op){
        spx_string_cat_printf(&err,loginfo,\
                "%c\t%s\t%s\n",
                op,fid,rfid);
    } else {
        spx_string_cat_printf(&err,loginfo,\
                "%c\t%s\n",
                op,fid);
    }

    size_t size = spx_string_len(loginfo);
    time_t now = spx_now();
    if(0 == pthread_mutex_lock(synclog->mlock)){
        do{
            if(now > spx_zero(&(synclog->d)) + SpxSecondsOfDay){
                spx_date_add(&(synclog->d),1);
                ydb_storage_synclog_reopen(synclog);
            }
            size_t len = 0;
            synclog->err = spx_fwrite_string(synclog->fp,loginfo,size,&len);
            if(0 != synclog->err || size != len){
                SpxLogFmt2(synclog->log,SpxLogError,synclog->err,\
                        "write synclog is fail.loginfo:%s,size:%lld,realsize:%lld.",
                        loginfo,size,len);
                break;
            }
            synclog->off += len;
        }while(false);
        pthread_mutex_unlock(synclog->mlock);
    }
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
    if(NULL != (*synclog)->mlock){
        spx_thread_mutex_free(&(*synclog)->mlock);
    }
    SpxFree(*synclog);
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

err_t ydb_storage_synclog_line_parser(string_t line,
        char *op,string_t *fid,string_t *rfid){/*{{{*/
    if(SpxStringIsNullOrEmpty(line)){
        return EINVAL;
    }
    if(SpxStringEndWith(line,SpxLineEndDlmt)){
        spx_string_strip_linefeed(line);
    }
    err_t err = 0;
    int count = 0;
    string_t *strs = spx_string_split(line,"\t",strlen("\t"),&count,&err);
    if(0 != err){
        return err;
    }
    if(YDB_STORAGE_MODIFY == *line){
        if(3 != count){
            spx_string_free_splitres(strs,count);
            return EIO;
        }
        *op = **strs;
        *fid = *(strs + 1);
        *rfid = *(strs + 2);
    } else {
        if(2 != count){
            spx_string_free_splitres(strs,count);
            return EIO;
        }
        *op = **strs;
        *fid = *(strs + 1);
    }
    return 0;
}/*}}}*/

