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
#include <ev.h>

#include "spx_types.h"

#include "ydb_storage_configurtion.h"
    struct ydb_mp_sync{
        u64_t d;
        u64_t offset;
    };

    struct ydb_storage_runtime{
        ev_timer w;
        SpxLogDelegate *log;
        struct ydb_storage_configurtion *c;
        u8_t mpidx;
        u8_t p1;
        u8_t p2;
        u32_t storecount;
        u64_t first_start_time;
        u64_t total_disksize;
        u64_t total_freesize;
        struct spx_date sync_binlog_date;
        u32_t sync_binlog_offset;
        u8_t status;
    //    struct ydb_mp_sync *mp_sync;
    };

    extern struct ydb_storage_runtime *g_ydb_storage_runtime;

    struct ydb_storage_runtime *ydb_storage_runtime_init(struct ev_loop *loop,\
        SpxLogDelegate *log,struct ydb_storage_configurtion *c,err_t *err);


#ifdef __cplusplus
}
#endif
#endif
