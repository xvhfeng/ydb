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
        SpxLogDelegate *log;
        err_t err;
        FILE *fp;
        off_t off;
        string_t path;
        string_t machineid;
        string_t filename;
        struct spx_date d;
    };

err_t ydb_storage_synclog_init(
        struct ydb_storage_synclog *synclog,
        SpxLogDelegate *log,
        string_t path,string_t machineid, int year,
        int month,int day,u64_t offset);

void  ydb_storage_synclog_write(struct ydb_storage_synclog *synclog,
    time_t log_time, char op,string_t fid,string_t rfid);

    void ydb_storage_synclog_free(struct ydb_storage_synclog **synclog);
void ydb_storage_synclog_clear(struct ydb_storage_synclog *synclog);

    string_t ydb_storage_synclog_make_filename(SpxLogDelegate *log,string_t path,string_t machineid,
            int year,int month,int day,err_t *err);


#ifdef __cplusplus
}
#endif
#endif
