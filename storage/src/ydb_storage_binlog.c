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
#include "spx_queue.h"
#include "spx_defs.h"
#include "spx_alloc.h"
#include "spx_string.h"
#include "spx_io.h"
#include "spx_path.h"
#include "spx_time.h"


#include "ydb_storage_configurtion.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_runtime.h"

struct ydb_storage_binlog *g_ydb_storage_binlog = NULL;

spx_private err_t ydb_storage_binlog_node_free(void **arg);
spx_private err_t ydb_storage_binlog_open(struct ydb_storage_binlog *binlog);
spx_private err_t ydb_storage_binlog_reopen(struct ydb_storage_binlog *binlog);
spx_private void ydb_storage_do_binlog (struct ev_loop *loop, ev_async *w, int revents);
spx_private void *ydb_storage_binlog_thread_listening(void *arg);

spx_private err_t ydb_storage_binlog_node_free(void **arg){/*{{{*/
    string_t *s = (string_t *) arg;
    if(NULL != s){
        spx_string_free(*s);
    }
    return 0;
}/*}}}*/

spx_private err_t ydb_storage_binlog_reopen(struct ydb_storage_binlog *binlog){/*{{{*/
    if(NULL != binlog->fp){
        fflush(binlog->fp);
        fclose(binlog->fp);
        binlog->off = 0;
        if(NULL != binlog->filename) {
            spx_string_free(binlog->filename);
        }
    }

    binlog->filename = ydb_storage_binlog_make_filename(binlog->log,binlog->path,
            binlog->machineid,binlog->d.year,binlog->d.month,binlog->d.day,&(binlog->err));
    if(NULL == binlog->filename){
        SpxLog2(binlog->log,SpxLogError,binlog->err,
                "make binlog filename is fail.");
    }

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
}/*}}}*/

spx_private err_t ydb_storage_binlog_open(struct ydb_storage_binlog *binlog){/*{{{*/
    //skip binlog but no flush state into disk
    spx_get_today(&(binlog->d));

    binlog->filename = ydb_storage_binlog_make_filename(binlog->log,binlog->path,
            binlog->machineid,binlog->d.year,binlog->d.month,binlog->d.day,&(binlog->err));
    if(NULL == binlog->filename){
        SpxLog2(binlog->log,SpxLogError,binlog->err,
                "make binlog filename is fail.");
    }

    struct stat buf;
    memset(&buf,0,sizeof(buf));
    if(SpxFileExist(binlog->filename)){
        stat(binlog->filename,&buf);
    }

    binlog->fp = fopen(binlog->filename,"a+");
    if(NULL == binlog->fp){
        binlog->err = errno;
        SpxLogFmt2(binlog->log,SpxLogError,binlog->err,\
                "open binlog file:%s is fail.",\
                binlog->filename);
        spx_string_free(binlog->filename);
        return binlog->err;
    }
    if(0 != buf.st_size) {
        fseek(binlog->fp,buf.st_size,SEEK_CUR);
        binlog->off = buf.st_size;
    }
    return 0;
}/*}}}*/

spx_private void *ydb_storage_binlog_thread_listening(void *arg){/*{{{*/
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
}/*}}}*/

spx_private void ydb_storage_do_binlog (struct ev_loop *loop, ev_async *w, int revents){/*{{{*/
    struct ydb_storage_binlog *binlog = (struct ydb_storage_binlog *) w;
    string_t s = NULL;
    while(NULL != (s = spx_queue_pop(binlog->q,&(binlog->err)))) {//multi-async call will compare to one
        size_t size = spx_string_len(s);
        if(ev_now(loop) > spx_zero(&(binlog->d)) + SpxSecondsOfDay){
            spx_date_add(&(binlog->d),1);
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
}/*}}}*/

struct ydb_storage_binlog *ydb_storage_binlog_new(SpxLogDelegate *log,\
        struct ydb_storage_configurtion *c,\
        string_t path,string_t machineid){/*{{{*/
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
}/*}}}*/

void ydb_storage_binlog_write(struct ydb_storage_binlog *binlog,\
        char op,bool_t issinglefile,\
        u32_t ver,u32_t opver,\
        string_t mid,u64_t fcreatetime,u64_t createtime,\
        u64_t lastmodifytime,u8_t mpidx,u8_t p1,u8_t p2, int tid,\
        u32_t rand,u64_t begin,u64_t totalsize,u64_t realsize,string_t suffix){/*{{{*/
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
}/*}}}*/

void ydb_storage_binlog_free(struct ydb_storage_binlog **binlog){/*{{{*/
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
}/*}}}*/

string_t ydb_storage_binlog_make_filename(SpxLogDelegate *log,string_t path,string_t machineid,
        int year,int month,int day,err_t *err){
    string_t filename = spx_string_newlen(NULL,SpxPathSize,err);
    if(NULL == filename){
        SpxLog2(log,SpxLogError,*err,
                "alloc binlog filename is fail.");
        return NULL;
    }
    string_t new_filename = spx_string_cat_printf(err,filename,
            "%s%s.metadata/%s%04-%02-%02.binlog",\
            path,SpxStringEndWith(path,SpxPathDlmt) ? "" : SpxPathDlmtString,\
            machineid,year,month,day);
    if(NULL == new_filename){
        SpxLog2(log,SpxLogError,*err,
                "cat binlog filename is fail.");
        spx_string_free(filename);
        return NULL;
    }
    filename = new_filename;
    return filename;
}

err_t ydb_storage_binlog_context_parser(SpxLogDelegate *log,string_t line,i32_t *op,bool_t *issinglefile,
        string_t *mid,u32_t *ver,u32_t *opver,u64_t *fcreatetime,u64_t *createtime,
        u64_t *lastmodifytime,i32_t *mpidx,i32_t *p1,i32_t *p2,u32_t *tid,u32_t *rand,
        u64_t *begin,u64_t *totalsize,u64_t *realsize,string_t *suffix){

    int count = 0;
    err_t err = 0;
    string_t *contexts = spx_string_split(line,";",strlen(":"),&count,&err);
    if(NULL == contexts){
        return err;
    }
    int i = 0;
    for( ; i < count ; i++){
        string_t ctx = *(contexts + i);
        switch(i){
            case 0:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "op of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *op = atoi(ctx);
                       break;
                   }
            case 1:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "issinglefile of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *issinglefile = (bool_t) atoi(ctx);
                       break;
                   }
            case 2:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "mid of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *mid = spx_string_dup(ctx,&err);
                       break;
                   }
            case 3:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "ver of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *ver = atoi(ctx);
                       break;
                   }
            case 4:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "opver of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *opver = atoi(ctx);
                       break;
                   }
            case 5:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "fcreatetime of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *fcreatetime = strtoul(ctx,NULL,10);
                       break;
                   }
            case 6:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "createtime of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *createtime = strtoul(ctx,NULL,10);
                       break;
                   }
            case 7:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "lastmodifytime of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *lastmodifytime = strtoul(ctx,NULL,10);
                       break;
                   }
            case 8:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "mpidx of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *mpidx = atoi(ctx);
                       break;
                   }
            case 9:{
                       if(SpxStringIsNullOrEmpty(ctx)){
                           SpxLog1(log,SpxLogError,
                                   "p1 of binlog is null or empty.");
                           err = ENOENT;
                           goto r1;
                       }
                       *p1 = atoi(ctx);
                       break;
                   }
            case 10:{
                        if(SpxStringIsNullOrEmpty(ctx)){
                            SpxLog1(log,SpxLogError,
                                    "p2 of binlog is null or empty.");
                            err = ENOENT;
                            goto r1;
                        }
                        *p2 = atoi(ctx);
                        break;
                    }
            case 11:{
                        if(SpxStringIsNullOrEmpty(ctx)){
                            SpxLog1(log,SpxLogError,
                                    "tid of binlog is null or empty.");
                            err = ENOENT;
                            goto r1;
                        }
                        *tid = atoi(ctx);
                        break;
                    }
            case 12:{
                        if(SpxStringIsNullOrEmpty(ctx)){
                            SpxLog1(log,SpxLogError,
                                    "rand of binlog is null or empty.");
                            err = ENOENT;
                            goto r1;
                        }
                        *rand = atoi(ctx);
                        break;
                    }
            case 13:{
                        if(SpxStringIsNullOrEmpty(ctx)){
                            SpxLog1(log,SpxLogError,
                                    "begin of binlog is null or empty.");
                            err = ENOENT;
                            goto r1;
                        }
                        *begin = strtoul(ctx,NULL,10);
                        break;
                    }
            case 14:{
                        if(SpxStringIsNullOrEmpty(ctx)){
                            SpxLog1(log,SpxLogError,
                                    "totalsize of binlog is null or empty.");
                            err = ENOENT;
                            goto r1;
                        }
                        *totalsize = strtoul(ctx,NULL,10);
                        break;
                    }
            case 15:{
                        if(SpxStringIsNullOrEmpty(ctx)){
                            SpxLog1(log,SpxLogError,
                                    "realsize of binlog is null or empty.");
                            err = ENOENT;
                            goto r1;
                        }
                        *realsize = strtoul(ctx,NULL,10);
                        break;
                    }
            case 16:{
                        if(!SpxStringIsNullOrEmpty(ctx)){
                            *suffix = spx_string_dup(ctx,&err);
                        }
                        break;
                    }
            default:{
                        break;
                    }
        }
    }
r1:
    spx_string_free_splitres(contexts,count);
    return err;
}
