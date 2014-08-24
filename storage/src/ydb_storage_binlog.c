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


#include "include/spx_types.h"
#include "include/spx_queue.h"
#include "include/spx_defs.h"
#include "include/spx_alloc.h"
#include "include/spx_string.h"
#include "include/spx_io.h"
#include "include/spx_path.h"


#include "ydb_storage_configurtion.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_state.h"

struct ydb_storage_binlog *g_ydb_storage_binlog = NULL;

spx_private err_t ydb_storage_binlog_node_free(void **arg);
spx_private err_t ydb_storage_binlog_open(struct ydb_storage_binlog *binlog);
spx_private err_t ydb_storage_binlog_reopen(struct ydb_storage_binlog *binlog);
spx_private void ydb_storage_do_binlog (struct ev_loop *loop, ev_async *w, int revents);
spx_private void *ydb_storage_binlog_thread_listening(void *arg);

spx_private err_t ydb_storage_binlog_node_free(void **arg){
    string_t *s = (string_t *) arg;
    if(NULL != s){
        spx_string_free(*s);
    }
    return 0;
}

spx_private err_t ydb_storage_binlog_reopen(struct ydb_storage_binlog *binlog){
    if(NULL != binlog->fp){
        fflush(binlog->fp);
        fclose(binlog->fp);
        binlog->off = 0;
        if(NULL != binlog->filename) {
            spx_string_clear(binlog->filename);
        }
        binlog->idx++;
    }
    spx_string_cat_printf(&(binlog->err),binlog->filename,"%s%s.metadata/%s%04X.binlog",\
            binlog->path,SpxStringEndWith(binlog->path,SpxPathDlmt) ? "" : SpxPathDlmtString,\
            binlog->machineid,binlog->idx);

    binlog->fp = fopen(binlog->filename,"a+");
    if(NULL == binlog->fp){
        binlog->err = errno;
        SpxLogFmt2(binlog->log,SpxLogError,binlog->err,\
                "open binlog file:%s is fail.",\
                binlog->filename);
        spx_string_free(binlog->filename);
        return binlog->err;
    }
    binlog->off = 0;
    return 0;
}

spx_private err_t ydb_storage_binlog_open(struct ydb_storage_binlog *binlog){
    binlog->idx = g_ydb_storage_state->binlog_idx;
    binlog->filename = spx_string_newlen(NULL,SpxFileNameSize,&(binlog->err));
    if(NULL == binlog->filename){
        SpxLog2(binlog->log,SpxLogError,binlog->err,\
                "new binlog filename is fail.");
        return binlog->err;
    }

    //skip binlog but no flush state into disk
    u32_t idx = binlog->idx + 1;
    spx_string_cat_printf(&(binlog->err),binlog->filename,"%s%s.metadata/%s%04X.binlog",\
            binlog->path,SpxStringEndWith(binlog->path,SpxPathDlmt) ? "" : SpxPathDlmtString,\
            binlog->machineid,idx);
    if(SpxFileExist(binlog->filename)){
        binlog->idx = idx;
    } else {
        spx_string_cat_printf(&(binlog->err),binlog->filename,"%s%s.metadata/%s%04X.binlog",\
                binlog->path,SpxStringEndWith(binlog->path,SpxPathDlmt) ? "" : SpxPathDlmtString,\
                binlog->machineid,binlog->idx);
    }


    struct stat buf;
    stat(binlog->filename,&buf);

    binlog->fp = fopen(binlog->filename,"a+");
    if(NULL == binlog->fp){
        binlog->err = errno;
        SpxLogFmt2(binlog->log,SpxLogError,binlog->err,\
                "open binlog file:%s is fail.",\
                binlog->filename);
        spx_string_free(binlog->filename);
        return binlog->err;
    }
    fseek(binlog->fp,buf.st_size,SEEK_CUR);
    binlog->off = buf.st_size;
    return 0;
}

spx_private void *ydb_storage_binlog_thread_listening(void *arg){
    struct ydb_storage_binlog *binlog = (struct ydb_storage_binlog *) arg;
    err_t err = 0;
    if(0 != (err = ydb_storage_binlog_open(binlog))){
        SpxLog2(binlog->log,SpxLogError,err,\
                "open binlog file is fail.");
        return NULL;
    }
    binlog->loop = ev_loop_new(0);
    ev_async_init (&(binlog->async), ydb_storage_do_binlog);
    ev_run(binlog->loop,0);
    return NULL;
}

spx_private void ydb_storage_do_binlog (struct ev_loop *loop, ev_async *w, int revents){
    struct ydb_storage_binlog *binlog = (struct ydb_storage_binlog *) w;
    string_t s = NULL;
    while(NULL != (s = spx_queue_pop(binlog->q,&(binlog->err)))) {//multi-async call will compare to one
        size_t size = spx_string_len(s);
        if(binlog->off + size > binlog->maxsize){
            ydb_storage_binlog_reopen(binlog);
        }
        size_t len = 0;
        binlog->err = spx_fwrite_string(binlog->fp,s,size,&len);
        if(0 != binlog->err || size != len){
            SpxLogFmt2(binlog->log,SpxLogError,binlog->err,\
                    "write binlog is fail.loginfo:%s,size:%lld,realsize:%lld.",
                    s,size,len);
            return;
        }
        binlog->off += len;
        spx_string_free(s);
    }
}

struct ydb_storage_binlog *ydb_storage_binlog_new(SpxLogDelegate *log,\
        struct ydb_storage_configurtion *c,\
        string_t path,string_t machineid,\
        size_t maxsize){
    err_t err = 0;
    struct ydb_storage_binlog *binlog = (struct ydb_storage_binlog *)\
                                        spx_alloc_alone(sizeof(*binlog),&err);
    if(NULL == binlog){
        SpxLog2(log,SpxLogError,err,\
                "new storage binlog is fail.");
        return NULL;
    }

    binlog->log = log;
    binlog->path = path;
    binlog->machineid = machineid;
    binlog->maxsize = maxsize;
    binlog->q = spx_queue_new(log,ydb_storage_binlog_node_free,&err);
    if(NULL == binlog->q){
        SpxLog2(log,SpxLogError,err,\
                "init binlog queue is fail.");
        goto r1;
    }

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t ostack_size = 0;
    pthread_attr_getstacksize(&attr, &ostack_size);
    do{
        if (ostack_size != c->stacksize
                && (0 != (binlog->err = pthread_attr_setstacksize(&attr,c->stacksize)))){
            SpxLog2(log,SpxLogError,binlog->err,\
                    "set thread stack size is fail.");
            pthread_attr_destroy(&attr);
            goto r1;
        }
        if (0 !=(binlog->err =  pthread_create(&(binlog->tid), &attr, ydb_storage_binlog_thread_listening,
                        binlog))){
            SpxLog2(log,SpxLogError,binlog->err,\
                    "create nio thread is fail.");
            pthread_attr_destroy(&attr);
            goto r1;
        }
    }while(false);
    pthread_attr_destroy(&attr);
    return binlog;
r1:
    if(NULL == binlog->q){
        spx_queue_free(&(binlog->q));
    }
    SpxFree(binlog);
    return NULL;
}

void ydb_storage_binlog_write(struct ydb_storage_binlog *binlog,\
        char op,bool_t issinglefile,\
        u32_t ver,u32_t opver,\
        string_t mid,u64_t fcreatetime,u64_t createtime,\
        u64_t lastmodifytime,u8_t mpidx,u8_t p1,u8_t p2, int tid,\
        u32_t rand,u64_t begin,u64_t totalsize,u64_t realsize,string_t suffix){
    if(NULL == binlog){
        return;
    }

    err_t err = 0;
    string_t loginfo = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == loginfo){
        SpxLog2(binlog->log,SpxLogError,err,\
                "new loginfo line is fail.");
        return;
    }

    spx_string_cat_printf(&err,loginfo,\
            "%d:%d:%s:%ud:%ud:%lld:%lld:%lld:%d:%d:%d:%d:%ud:%ulld:%ulld:%ulld:%s.\n",\
            op,issinglefile,mid,ver,opver,fcreatetime,createtime,lastmodifytime,\
            mpidx,p1,p2,tid,rand,begin,totalsize,realsize,SpxStringIsNullOrEmpty(suffix) ? "" : suffix);

    spx_queue_push(binlog->q,loginfo);
    ev_async_send(binlog->loop,&(binlog->async));
}

void ydb_storage_binlog_free(struct ydb_storage_binlog **binlog){
    ev_async_stop((*binlog)->loop,&(*binlog)->async);
    if(NULL != (*binlog)->fp){
        fflush((*binlog)->fp);
        fclose((*binlog)->fp);
        (*binlog)->off = 0;
        if(NULL != (*binlog)->filename) {
            spx_string_free((*binlog)->filename);
        }
    }
    ev_loop_destroy((*binlog)->loop);
    spx_queue_free(&((*binlog)->q));
    SpxFree(*binlog);
}

