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

struct ydb_storage_runtime *ydb_storage_runtime_init(
        SpxLogDelegate *log,struct ydb_storage_configurtion *c,err_t *err){/*{{{*/

    struct ydb_storage_runtime *rt = (struct ydb_storage_runtime *) \
                                     spx_alloc_alone(sizeof(*rt),err);
    if(NULL == rt){
        SpxLog2(log,SpxLogError,*err,\
                "new storage runtime is fail.");
        return NULL;
    }
    string_t line = NULL;
    string_t filename = NULL;
    FILE *fp = NULL;
    rt->c = c;
    rt->log = log;

    filename = ydb_storage_make_runtime_filename(c,err);
    if(NULL == filename){
        SpxLog2(log,SpxLogError,*err,"get storage mid filename is fail.");
        goto r1;
    }

    if(!SpxFileExist(filename)){
        rt->this_startup_time = spx_now();
        rt->first_statrup_time = rt->this_startup_time;
        spx_string_free(filename);
        return rt;
    }

    fp = fopen(filename,"r");
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
    return rt;

r1:
    if(NULL != rt){
        SpxFree(rt);
    }
    if(NULL != filename){
        spx_string_free(filename);
    }
    if(NULL != line){
        spx_string_free(line);
    }
    if(NULL != fp){
        fclose(fp);
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

//    if(0 == spx_string_casecmp(*kv,"first_start_time")){
//        if(2 != count){
//            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of first_start_time.");
//            goto r1;
//        }
//        u64_t v = strtoul(*(kv + 1),NULL,10);
//        if(ERANGE == v) {
//            SpxLog1(rt->log,SpxLogError,"bad the runtime item of first_start_time.");
//            goto r1;
//        }
//        rt->first_start_time = v;
//        goto r1;
//    }

    if(0 == spx_string_casecmp(*kv,"first_startup_time")){
        if(2 != count){
            SpxLog1(rt->log,SpxLogError,"no the value for the runtime item of first_startup_time.");
            goto r1;
        }
        u64_t v = strtoul(*(kv + 1),NULL,10);
        if(ERANGE == v) {
            SpxLog1(rt->log,SpxLogError,"bad the runtime item of first_startup_time.");
            goto r1;
        }
        rt->first_statrup_time = v;
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
    /*
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
    */
r1:
    spx_string_free_splitres(kv,count);
}/*}}}*/

void ydb_storage_runtime_flush(
       struct ydb_storage_runtime *srt
        ){/*{{{*/
    struct ydb_storage_configurtion *c = srt->c;
    err_t err = 0;
    string_t filename = NULL;
    string_t buf = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == buf){
        SpxLog2(srt->log,SpxLogError,err,\
                "new runtime buffer is fail.");
        return;
    }

    string_t newbuf = spx_string_cat_printf(&err,buf,\
            "first_startup_time = %ulld \n" \
            "mpidx = %d \n" \
            "p1 = %d \n" \
            "p2 = %d \n" \
            "storecount = %ud \n" \
            "total_disksize = %ulld \n" \
            "total_freesize = %ulld \n" ,
//            "sync_binlog_date = %ud \n"
//            "sync_binlog_offset = %ulld ",
            srt->first_statrup_time,
            srt->mpidx,srt->p1,srt->p2,\
            srt->storecount,\
            srt->total_disksize,\
            srt->total_freesize);
//            spx_zero(&(srt->sync_binlog_date)),
//            srt->sync_binlog_offset);
    if(NULL == newbuf){
        SpxLog2(srt->log,SpxLogError,err,
                "cat line context of runtime is fail.");
        goto r1;
    }
    buf = newbuf;

    filename = ydb_storage_make_runtime_filename(c,&err);
    if(NULL == filename){
        SpxLog2(c->log,SpxLogError,err,"get storage mid filename is fail.");
        goto r1;
    }
    FILE *fp = fopen(filename,"w+");
    if(NULL == fp) {
        err = errno;
        SpxLogFmt2(srt->log,SpxLogError,err,
                "open the mid file is fail.filename:%s.",
                filename);
        goto r1;
    }
    size_t size = spx_string_len(buf);
    size_t len = 0;
    err = spx_fwrite_string(fp,buf,size,&len);
    fflush(fp);
    fclose(fp);
    if(size != len){
        SpxLogFmt2(srt->log,SpxLogError,err,\
                "write buf to runtime file is fail."
                "size:%d,write size:%d.",
                size,len);
    }
r1:
    if(NULL != buf){
        spx_string_free(buf);
    }
    if(NULL != filename){
        spx_string_free(filename);
    }
    return;

}/*}}}*/

string_t ydb_storage_make_runtime_filename(
        struct ydb_storage_configurtion *c,
        err_t *err
        ){/*{{{*/
    string_t fname = NULL;
    string_t new_fname = NULL;
    fname = spx_string_dup(c->basepath,err);
    if(NULL == fname){
        SpxLog2(c->log,SpxLogError,*err,"dup the basepath is fail.");
        return NULL;
    }

    if(SpxStringEndWith(fname,SpxPathDlmt)){
        new_fname = spx_string_cat_printf(err,fname,\
                ".%s-%s-rtf.spx",\
                c->groupname,c->machineid);
    } else {
        new_fname = spx_string_cat_printf(err,fname,\
                "/.%s-%s-rtf.spx",\
                c->groupname,c->machineid);
    }
    if(NULL == new_fname){
        SpxLog2(c->log,SpxLogError,*err,"get storage mid filename is fail.");
        SpxStringFree(fname);
        return NULL;
    }
    fname = new_fname;
    return fname;
}/*}}}*/

