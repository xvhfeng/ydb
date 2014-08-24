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

#include "include/spx_types.h"
#include "include/spx_queue.h"

#include "ydb_storage_configurtion.h"

    struct ydb_storage_binlog{
        ev_async async;
        struct ev_loop *loop;//one thread so one loop in the struct
        err_t err;
        pthread_t tid;
        FILE *fp;
        off_t off;
        struct spx_queue *q;
        string_t path;
        string_t machineid;
        string_t filename;
        u32_t idx;
        size_t maxsize;
        SpxLogDelegate *log;
    };

#define YDB_BINLOG_ADD 'A'
#define YDB_BINLOG_MODIFY 'M'
#define YDB_BINLOG_DELETE 'D'

    extern struct ydb_storage_binlog *g_ydb_storage_binlog;

    struct ydb_storage_binlog *ydb_storage_binlog_new(SpxLogDelegate *log,\
            struct ydb_storage_configurtion *c,\
            string_t path,string_t machineid,\
            size_t maxsize);

    void ydb_storage_binlog_write(struct ydb_storage_binlog *binlog,\
            char op,bool_t issinglefile,\
            u32_t ver,u32_t opver,\
            string_t mid,u64_t fcreatetime,u64_t createtime,\
            u64_t lastmodifytime,u8_t mpidx,u8_t p1,u8_t p2, int tid,\
            u32_t rand,u64_t begin,u64_t totalsize,u64_t realsize,string_t suffix);

    void ydb_storage_binlog_free(struct ydb_storage_binlog **binlog);

#define YdbStorageBinlog(op,issinglefile,ver,opver,mid,fcreatetime,\
        createtime,lastmodifytime,mpidx,p1,p2,tid,rand,begin,totalsize,realsize,suffix) \
        ydb_storage_binlog_write(g_ydb_storage_binlog,op,issinglefile,ver, opver,\
             mid, fcreatetime, createtime,lastmodifytime, mpidx, p1, p2,  tid,\
             rand, begin, totalsize, realsize,suffix)


#ifdef __cplusplus
}
#endif
#endif
