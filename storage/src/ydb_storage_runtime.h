/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_runtime.h
 *        Created:  2014/08/14 10时48分29秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_RUNTIME_H_
#define _YDB_STORAGE_RUNTIME_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <ev.h>

#include "spx_types.h"

#include "ydb_storage_configurtion.h"

    struct ydb_storage_runtime{
        SpxLogDelegate *log;
        struct ydb_storage_configurtion *c;
        u8_t mpidx;
        u8_t p1;
        u8_t p2;
        u32_t storecount;
        u32_t chunkfile_count;
        u32_t singlefile_count;
        u64_t this_startup_time;
        u64_t first_statrup_time;
        u64_t total_disksize;
        u64_t total_freesize;
        u8_t status;
    };

    extern struct ydb_storage_runtime *g_ydb_storage_runtime;

    struct ydb_storage_runtime *ydb_storage_runtime_init(
            SpxLogDelegate *log,struct ydb_storage_configurtion *c,err_t *err);

void ydb_storage_runtime_flush(
       struct ydb_storage_runtime *srt);

    string_t ydb_storage_make_runtime_filename(
            struct ydb_storage_configurtion *c,
            err_t *err);

#ifdef __cplusplus
}
#endif
#endif
