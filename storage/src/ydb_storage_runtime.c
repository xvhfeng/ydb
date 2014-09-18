/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_runtime.c
 *        Created:  2014/08/14 10时48分27秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "spx_io.h"
#include "spx_time.h"
#include "spx_path.h"
#include "spx_types.h"
#include "spx_alloc.h"
#include "spx_defs.h"
#include "spx_string.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_runtime.h"


struct ydb_storage_runtime *g_ydb_storage_runtime = NULL;

spx_private void ydb_storage_runtime_line_parser(string_t line,\
        struct ydb_storage_runtime *rt,err_t *err);
spx_private void ydb_storage_runtime_flush(struct ev_loop *loop,ev_timer *w,int revents);

struct ydb_storage_runtime *ydb_storage_runtime_init(struct ev_loop *loop,\
        SpxLogDelegate *log,struct ydb_storage_configurtion *c,err_t *err){/*{{{*/

    struct ydb_storage_runtime *rt = (struct ydb_storage_runtime *) \
                                     spx_alloc_alone(sizeof(*rt),err);
    if(NULL == rt){
        SpxLog2(log,SpxLogError,*err,\
                "new storage runtime is fail.");
        return NULL;
    }
    string_t new_basepath = NULL;
    string_t line = NULL;
    string_t filename = NULL;
    rt->c = c;
    rt->log = log;
    /*
    rt->mp_sync = spx_alloc(YDB_STORAGE_MOUNTPOINT_MAXSIZE,sizeof(struct ydb_mp_sync),err);
    if(NULL == rt->mp_sync){
        SpxLog2(log,SpxLogError,*err,
                "new sync state of mp is fail.");
        goto r1;
    }
    */
    new_basepath = spx_string_dup(c->basepath,err);
    if(NULL == new_basepath){
        SpxLog2(log,SpxLogError,*err,"dup the basepath is fail.");
        goto r1;
    }

    if(SpxStringEndWith(new_basepath,SpxPathDlmt)){
        filename = spx_string_cat_printf(err,new_basepath,\
                "%s-%s-ydb-storage.rtf",\
                c->groupname,c->machineid);
    } else {
        filename = spx_string_cat_printf(err,new_basepath,\
                "/%s-%s-ydb-storage.rtf",\
                c->groupname,c->machineid);
    }
    if(NULL == filename){
        SpxLog2(log,SpxLogError,*err,"get storage mid filename is fail.");
        goto r1;
    }

    if(!SpxFileExist(filename)){
        rt->first_start_time = spx_now();
        spx_string_free(filename);
        return rt;
    }

    FILE *fp = fopen(filename,"r");
    if(NULL == fp) {
        *err = errno;
        SpxLogFmt2(log,SpxLogError,*err,\
                "open the mid file is fail.filename:&s.",filename);
        goto r1;
    }
    line = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),err);
    if(NULL == line){
        goto r1;
    }

    while(NULL != fgets(line,SpxLineSize,fp)){
        spx_string_updatelen(line);
        spx_string_trim(line," ");
        if(!SpxStringBeginWith(line,'#')){
            ydb_storage_runtime_line_parser(line,rt,err);
            if(0 != err){
                goto r1;
            }
        }
        spx_string_clear(line);
    }
    spx_string_free(line);
    fclose(fp);
    ev_timer_init(&(rt->w),ydb_storage_runtime_flush,(double) c->runtime_flush_timespan,1);
    ev_timer_start (loop, &(rt->w));
    ev_run(loop,0);
    return rt;

r1:
    /*
    if(NULL != rt->mp_sync){
        SpxFree(rt->mp_sync);
    }
    */
    if(NULL != rt){
        SpxFree(rt);
    }
    if(NULL != filename){
        spx_string_free(new_basepath);
    }
    if(NULL != line){
        spx_string_free(line);
    }
    return NULL;
}/*}}}*/

spx_private void ydb_storage_runtime_line_parser(string_t line,\
        struct ydb_storage_runtime *rt,err_t *err){/*{{{*/
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

    if(0 == spx_string_casecmp(*kv,"first_start_time")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of first_start_time.");
            goto r1;
        }
        u64_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of first_start_time.");
            goto r1;
        }
        rt->first_start_time = v;
        goto r1;
    }
    if(0 == spx_string_casecmp(*kv,"mpidx")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of mpidx.");
            goto r1;
        }
        u8_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of mpidx.");
            goto r1;
        }
        rt->mpidx = v;
        goto r1;
    }
    if(0 == spx_string_casecmp(*kv,"p1")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of p1.");
            goto r1;
        }
        u8_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of p1.");
            goto r1;
        }
        rt->p1 = v;
        goto r1;
    }
    if(0 == spx_string_casecmp(*kv,"p2")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of p2.");
            goto r1;
        }
        u8_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of p2.");
            goto r1;
        }
        rt->p2 = v;
        goto r1;
    }
    if(0 == spx_string_casecmp(*kv,"storecount")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of storecount.");
            goto r1;
        }
        u32_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of storecount.");
            goto r1;
        }
        rt->storecount = v;
        goto r1;
    }
    if(0 == spx_string_casecmp(*kv,"total_disksize")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of total_disksize.");
            goto r1;
        }
        u64_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of total_disksize.");
            goto r1;
        }
        rt->total_disksize = v;
        goto r1;
    }
    if(0 == spx_string_casecmp(*kv,"total_freesize")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of total_freesize.");
            goto r1;
        }
        u64_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of total_freesize.");
            goto r1;
        }
        rt->total_freesize = v;
        goto r1;
    }
    if(0 == spx_string_casecmp(*kv,"sync_binlog_date")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of sync_binlog_date.");
            goto r1;
        }
        u64_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of sync_binlog_date.");
            goto r1;
        }
        spx_get_date((time_t *) &v,&(rt->sync_binlog_date));
        goto r1;
    }
    if(0 == spx_string_casecmp(*kv,"sync_binlog_offset")){

        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of sync_binlog_offset.");
            goto r1;
        }
        u64_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of sync_binlog_offset.");
            goto r1;
        }
        rt->sync_binlog_offset = v;
        goto r1;
    }
    /*
    if(0 == spx_string_begin_with(*kv,"mp")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of mp sync state.");
            goto r1;
        }
        int idx = atol((*kv) + sizeof("mp"));
        char *p = index(*(kv + 1),':');
        if(NULL == p){
            SpxLogFmt1(rt->log,SpxLogError,"bad value of mp sync state.value:%s.",*(kv + 1));
            goto r1;
        }
        u64_t v1 = strtoul(*(kv + 1),NULL,10);
        u64_t v2 = strtoul(p + 1,NULL,10);
        if(ERANGE == v1 || ERANGE == v2) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of sync_binlog_offset.");
            goto r1;
        }
        struct ydb_mp_sync *mp = rt->mp_sync + idx;
        mp->d = v1;
        mp->offset = v2;
        goto r1;
    }
    */
r1:
    spx_string_free_splitres(kv,count);
}/*}}}*/

spx_private void ydb_storage_runtime_flush(struct ev_loop *loop,ev_timer *w,int revents){/*{{{*/
    struct ydb_storage_runtime *rt = (struct ydb_storage_runtime *) w;
    struct ydb_storage_configurtion *c = rt->c;
    err_t err = 0;

    string_t filename = NULL;
    string_t new_basepath = NULL;

    string_t buf = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == buf){
        SpxLog2(rt->log,SpxLogError,err,\
                "new runtime buffer is fail.");
        return;
    }

    string_t newbuf = spx_string_cat_printf(&err,buf,\
            "first_start_time = %ulld \n" \
            "mpidx = %d \n" \
            "p1 = %d \n" \
            "p2 = %d \n" \
            "storecount = %ud \n" \
            "total_disksize = %ulld \n" \
            "total_freesize = %ulld \n" \
            "sync_binlog_date = %ud \n" \
            "sync_binlog_offset = %ulld ",\
            rt->first_start_time,\
            rt->mpidx,rt->p1,rt->p2,\
            rt->storecount,\
            rt->total_disksize,\
            rt->total_freesize,\
            spx_zero(&(rt->sync_binlog_date)),\
            rt->sync_binlog_offset);
    if(NULL == newbuf){
        SpxLog2(rt->log,SpxLogError,err,
                "cat line context of runtime is fail.");
        goto r1;
    }
    buf = newbuf;
    /*
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_MAXSIZE; i++){
        struct ydb_mp_sync *mp = rt->mp_sync + i;
        if(0 != mp->d){
            newbuf = spx_string_cat_printf(&err,buf,
                    "mp%d = %ulld:%ulld\n",
                    i,mp->d,mp->offset);
            if(NULL == newbuf){
                SpxLog2(rt->log,SpxLogError,err,
                        "cat mp sync stat to line is fail.");
            }else {
                buf = newbuf;
            }
        }
    }
    */

    new_basepath = spx_string_dup(c->basepath,&err);
    if(NULL == new_basepath){
        SpxLog2(rt->log,SpxLogError,err,"dup the basepath is fail.");
        goto r1;
    }

    if(SpxStringEndWith(new_basepath,SpxPathDlmt)){
        filename = spx_string_cat_printf(&err,new_basepath,\
                "%s%s-%s-ydb-storage.rtf",\
                new_basepath,c->groupname,c->machineid);
    } else {
        filename = spx_string_cat_printf(&err,new_basepath,\
                "%s/%s-%s-ydb-storage.rtf",\
                new_basepath,c->groupname,c->machineid);
    }
    if(NULL == filename){
        SpxLog2(rt->log,SpxLogError,err,"get storage mid filename is fail.");
        goto r1;
    }
    new_basepath = filename;
    FILE *fp = fopen(filename,"w+");
    if(NULL == fp) {
        err = errno;
        SpxLogFmt2(rt->log,SpxLogError,err,\
                "open the mid file is fail.filename:&s.",filename);
        goto r1;
    }
    size_t size = spx_string_len(buf);
    size_t len = 0;
    err = spx_fwrite_string(fp,buf,size,&len);
    fflush(fp);
    fclose(fp);
    if(size != len){
        SpxLogFmt2(rt->log,SpxLogError,err,\
                "write buf to runtime file is fail."
                "size:%d,write size:%d.",
                size,len);
    }
r1:
    if(NULL != buf){
        spx_string_free(buf);
    }
    if(NULL != filename){
        spx_string_free(new_basepath);
    }
    return;

}/*}}}*/

