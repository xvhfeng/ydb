#ifndef _YDB_STORAGE_SYNCLOG_H_
#define _YDB_STORAGE_SYNCLOG_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#include "spx_types.h"

#include "ydb_storage_configurtion.h"

    struct ydb_storage_synclog{
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

#define YDB_synclog_ADD 'A'
#define YDB_synclog_MODIFY 'M'
#define YDB_synclog_DELETE 'D'

    struct ydb_storage_synclog *ydb_storage_synclog_new(SpxLogDelegate *log,\
            string_t path,string_t machineid,
            err_t *err);

    void ydb_storage_synclog_write(struct ydb_storage_synclog *synclog,\
            char op,bool_t issinglefile,\
            u32_t ver,u32_t opver,\
            string_t mid,u64_t fcreatetime,u64_t createtime,\
            u64_t lastmodifytime,u8_t mpidx,u8_t p1,u8_t p2, int tid,\
            u32_t rand,u64_t begin,u64_t totalsize,u64_t realsize,string_t suffix);

    void ydb_storage_synclog_free(struct ydb_storage_synclog **synclog);

    string_t ydb_storage_synclog_make_filename(SpxLogDelegate *log,string_t path,string_t machineid,
            int year,int month,int day,err_t *err);

#define YdbStorageSynclog(op,issinglefile,ver,opver,mid,fcreatetime,\
        createtime,lastmodifytime,mpidx,p1,p2,tid,rand,begin,totalsize,realsize,suffix) \
    ydb_storage_synclog_write(g_ydb_storage_synclog,op,issinglefile,ver, opver,\
            mid, fcreatetime, createtime,lastmodifytime, mpidx, p1, p2,  tid,\
            rand, begin, totalsize, realsize,suffix)

    err_t ydb_storage_synclog_context_parser(SpxLogDelegate *log,string_t line,i32_t *op,bool_t *issinglefile,
            string_t *mid,u32_t *ver,u32_t *opver,u64_t *fcreatetime,u64_t *createtime,
            u64_t *lastmodifytime,i32_t *mpidx,i32_t *p1,i32_t *p2,u32_t *tid,u32_t *rand,
            u64_t *begin,u64_t *totalsize,u64_t *realsize,string_t *suffix);

#ifdef __cplusplus
}
#endif
#endif
