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

#include "spx_types.h"
#include "spx_queue.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"

#define YdbStorageBinlogUploadWriter(fid)  \
    ydb_storage_binlog_write(g_ydb_storage_binlog,\
            YDB_STORAGE_LOG_UPLOAD,fid,NULL)

#define YdbStorageBinlogDeleteWriter(fid)  \
    ydb_storage_binlog_write(g_ydb_storage_binlog,\
            YDB_STORAGE_LOG_DELETE,fid,NULL)

#define YdbStorageBinlogModifyWriter(fid,rfid)  \
    ydb_storage_binlog_write(g_ydb_storage_binlog,\
            YDB_STORAGE_LOG_MODIFY,fid,rfid)

    struct ydb_storage_binlog{
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


    extern struct ydb_storage_binlog *g_ydb_storage_binlog;

    struct ydb_storage_binlog *ydb_storage_binlog_new(SpxLogDelegate *log,\
            string_t path,string_t machineid,
            err_t *err);

    void ydb_storage_binlog_write(struct ydb_storage_binlog *binlog,\
            char op,string_t fid,string_t rfid);

    void ydb_storage_binlog_free(struct ydb_storage_binlog **binlog);

    string_t ydb_storage_binlog_make_filename(SpxLogDelegate *log,string_t path,string_t machineid,
            int year,int month,int day,err_t *err);

    err_t ydb_storage_binlog_line_parser(string_t line,
            char *op,string_t *fid,string_t *rfid);


#ifdef __cplusplus
}
#endif
#endif
