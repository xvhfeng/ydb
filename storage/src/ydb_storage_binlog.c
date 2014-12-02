/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_binlog.c
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
#include "ydb_storage_binlog.h"
#include "ydb_storage_runtime.h"

struct ydb_storage_binlog *g_ydb_storage_binlog = NULL;

spx_private err_t ydb_storage_binlog_open(struct ydb_storage_binlog *binlog);
spx_private err_t ydb_storage_binlog_reopen(struct ydb_storage_binlog *binlog);

spx_private err_t ydb_storage_binlog_reopen(struct ydb_storage_binlog *binlog){/*{{{*/
    if(NULL != binlog->fp){
        fflush(binlog->fp);
        fclose(binlog->fp);
        binlog->off = 0;
        if(NULL != binlog->filename) {
            SpxStringFree(binlog->filename);
        }
    }

    binlog->filename = ydb_storage_binlog_make_filename(binlog->log,binlog->path,
            binlog->machineid,binlog->d.year,binlog->d.month,binlog->d.day,&(binlog->err));
    if(NULL == binlog->filename){
        SpxLog2(binlog->log,SpxLogError,binlog->err,
                "make binlog filename is fail.");
        return binlog->err;
    }

    binlog->fp = fopen(binlog->filename,"a+");
    if(NULL == binlog->fp){
        binlog->err = errno;
        SpxLogFmt2(binlog->log,SpxLogError,binlog->err,\
                "open binlog file:%s is fail.",\
                binlog->filename);
        SpxStringFree(binlog->filename);
        return binlog->err;
    }
    setlinebuf(binlog->fp);
    binlog->off = 0;
    return 0;
}/*}}}*/

spx_private err_t ydb_storage_binlog_open(struct ydb_storage_binlog *binlog){/*{{{*/
    //skip binlog but no flush state into disk
    spx_get_today(&(binlog->d));

    binlog->filename = ydb_storage_binlog_make_filename(binlog->log,binlog->path,
            binlog->machineid,binlog->d.year,binlog->d.month,binlog->d.day,&(binlog->err));
    if(NULL == binlog->filename){
        SpxLog2(binlog->log,SpxLogError,binlog->err,
                "make binlog filename is fail.");
        return binlog->err;
    }

    struct stat buf;
    memset(&buf,0,sizeof(buf));
    if(SpxFileExist(binlog->filename)){
        stat(binlog->filename,&buf);
    }

    string_t basepath = spx_basepath(binlog->filename,&(binlog->err));
    if(NULL == basepath || 0 != binlog->err){
        SpxLogFmt2(binlog->log,SpxLogError,binlog->err,
                "get basepath from filename:%s is fail.",
                binlog->filename);
        SpxStringFree(binlog->filename);
        return binlog->err;
    }
    if(!spx_is_dir(basepath,&(binlog->err))){
        spx_mkdir(binlog->log,basepath,SpxPathMode);
    }
    SpxStringFree(basepath);

    binlog->fp = fopen(binlog->filename,"a+");
    if(NULL == binlog->fp){
        binlog->err = errno;
        SpxLogFmt2(binlog->log,SpxLogError,binlog->err,\
                "open binlog file:%s is fail.",\
                binlog->filename);
        SpxStringFree(binlog->filename);
        return binlog->err;
    }
    if(0 != buf.st_size) {
        fseek(binlog->fp,buf.st_size,SEEK_CUR);
        binlog->off = buf.st_size;
    }
    setlinebuf(binlog->fp);
    return 0;
}/*}}}*/

struct ydb_storage_binlog *ydb_storage_binlog_new(SpxLogDelegate *log,\
        string_t path,string_t machineid,
        err_t *err){/*{{{*/
    struct ydb_storage_binlog *binlog = (struct ydb_storage_binlog *)\
                                        spx_alloc_alone(sizeof(*binlog),err);
    if(NULL == binlog){
        SpxLog2(log,SpxLogError,*err,\
                "new storage binlog is fail.");
        return NULL;
    }

    binlog->log = log;
    binlog->path = path;
    binlog->machineid = machineid;
    binlog->mlock = spx_thread_mutex_new(log,err);
    if(NULL == binlog->mlock){
        goto r1;
    }
    if(0 != (*err = ydb_storage_binlog_open(binlog))){
        SpxLogFmt2(log,SpxLogError,*err,
                "open binlog:%s is fail.",
                binlog->filename);
        goto r1;
    }
    return binlog;
r1:
    ydb_storage_binlog_free(&binlog);
    return NULL;
}/*}}}*/

void ydb_storage_binlog_write(struct ydb_storage_binlog *binlog,\
        char op,string_t fid,string_t rfid){/*{{{*/
    if(NULL == binlog || SpxStringIsNullOrEmpty(fid)){
        return;
    }

    err_t err = 0;
    string_t loginfo = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == loginfo){
        SpxLog2(binlog->log,SpxLogError,err,\
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
    time_t now = spx_now();
    if(0 == pthread_mutex_lock(binlog->mlock)){
        do{
            if(now > spx_zero(&(binlog->d)) + SpxSecondsOfDay){
                spx_date_add(&(binlog->d),1);
                ydb_storage_binlog_reopen(binlog);
            }
            size_t len = 0;
            binlog->err = spx_fwrite_string(binlog->fp,loginfo,size,&len);
            if(0 != binlog->err || size != len){
                SpxLogFmt2(binlog->log,SpxLogError,binlog->err,\
                        "write binlog is fail.loginfo:%s,size:%lld,realsize:%lld.",
                        loginfo,size,len);
                break;
            }
            binlog->off += len;
        }while(false);
        pthread_mutex_unlock(binlog->mlock);
    }
    SpxStringFree(loginfo);
    return;
}/*}}}*/

void ydb_storage_binlog_free(struct ydb_storage_binlog **binlog){/*{{{*/
    if(NULL != (*binlog)->fp){
        fflush((*binlog)->fp);
        fclose((*binlog)->fp);
        (*binlog)->off = 0;
        if(NULL != (*binlog)->filename) {
            SpxStringFree((*binlog)->filename);
        }
    }
    if(NULL != (*binlog)->mlock){
        spx_thread_mutex_free(&(*binlog)->mlock);
    }
    SpxFree(*binlog);
}/*}}}*/

string_t ydb_storage_binlog_make_filename(SpxLogDelegate *log,string_t path,string_t machineid,
        int year,int month,int day,err_t *err){/*{{{*/
    string_t filename = spx_string_newlen(NULL,SpxPathSize,err);
    if(NULL == filename){
        SpxLog2(log,SpxLogError,*err,
                "alloc binlog filename is fail.");
        return NULL;
    }
    string_t new_filename = spx_string_cat_printf(err,filename,
            "%s%s.binlog/%s/%04d-%02d-%02d.log",\
            path,SpxStringEndWith(path,SpxPathDlmt) ? "" : SpxPathDlmtString,\
            machineid,year,month,day);
    if(NULL == new_filename){
        SpxLog2(log,SpxLogError,*err,
                "cat binlog filename is fail.");
        SpxStringFree(filename);
        return NULL;
    }
    filename = new_filename;
    return filename;
}/*}}}*/

err_t ydb_storage_binlog_line_parser(string_t line,
        char *op,string_t *fid,string_t *rfid){
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
    if(YDB_STORAGE_LOG_MODIFY == *line){
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
}

