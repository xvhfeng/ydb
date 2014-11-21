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
        SpxLogDelegate *log;
        pthread_mutex_t *mlock;
    };

//    extern struct ydb_storage_synclog *g_ydb_storage_synclog;


    struct ydb_storage_synclog *ydb_storage_synclog_write(
            struct ydb_storage_synclog *synclog,
            struct ydb_storage_configurtion *c,
            int year,int month,int day,u64_t offset,
            char op,string_t fid,string_t rfid);

    void ydb_storage_synclog_free(struct ydb_storage_synclog **synclog);

    string_t ydb_storage_synclog_make_filename(SpxLogDelegate *log,string_t path,string_t machineid,
            int year,int month,int day,err_t *err);

    err_t ydb_storage_synclog_line_parser(string_t line,
        char *op,string_t *fid,string_t *rfid);

#ifdef __cplusplus
}
#endif
#endif
