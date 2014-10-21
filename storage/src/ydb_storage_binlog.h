/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_binlog.h
 *        Created:  2014/08/12 14时19分52秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_BINLOG_H_
#define _YDB_STORAGE_BINLOG_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <ev.h>

#include "spx_types.h"
#include "spx_queue.h"

#include "ydb_storage_configurtion.h"

    struct ydb_storage_binlog{
        err_t err;
        FILE *fp;
        off_t off;
        string_t path;
        string_t machineid;
        string_t filename;
        struct spx_date d;
        size_t maxsize;
        SpxLogDelegate *log;
        pthread_mutex_t *mlock;
    };

#define YDB_BINLOG_ADD 'A'
#define YDB_BINLOG_MODIFY 'M'
#define YDB_BINLOG_DELETE 'D'

    extern struct ydb_storage_binlog *g_ydb_storage_binlog;

    struct ydb_storage_binlog *ydb_storage_binlog_new(SpxLogDelegate *log,\
            string_t path,string_t machineid,
            err_t *err);

    void ydb_storage_binlog_write(struct ydb_storage_binlog *binlog,\
            char op,bool_t issinglefile,\
            u32_t ver,u32_t opver,\
            string_t mid,u64_t fcreatetime,u64_t createtime,\
            u64_t lastmodifytime,u8_t mpidx,u8_t p1,u8_t p2, int tid,\
            u32_t rand,u64_t begin,u64_t totalsize,u64_t realsize,string_t suffix);

    void ydb_storage_binlog_free(struct ydb_storage_binlog **binlog);
    string_t ydb_storage_binlog_make_filename(SpxLogDelegate *log,string_t path,string_t machineid,
            int year,int month,int day,err_t *err);

#define YdbStorageBinlog(op,issinglefile,ver,opver,mid,fcreatetime,\
        createtime,lastmodifytime,mpidx,p1,p2,tid,rand,begin,totalsize,realsize,suffix) \
    ydb_storage_binlog_write(g_ydb_storage_binlog,op,issinglefile,ver, opver,\
            mid, fcreatetime, createtime,lastmodifytime, mpidx, p1, p2,  tid,\
            rand, begin, totalsize, realsize,suffix)

    err_t ydb_storage_binlog_context_parser(SpxLogDelegate *log,string_t line,i32_t *op,bool_t *issinglefile,
            string_t *mid,u32_t *ver,u32_t *opver,u64_t *fcreatetime,u64_t *createtime,
            u64_t *lastmodifytime,i32_t *mpidx,i32_t *p1,i32_t *p2,u32_t *tid,u32_t *rand,
            u64_t *begin,u64_t *totalsize,u64_t *realsize,string_t *suffix);

#ifdef __cplusplus
}
#endif
#endif
