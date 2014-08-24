/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_state.h
 *        Created:  2014/08/04 13时55分18秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_STATE_H_
#define _YDB_STORAGE_STATE_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>

#include "include/spx_types.h"

    struct ydb_storage_state{
        u64_t disksize;
        u64_t freesize;
        int ydb_storage_status;
        u64_t ydb_storage_first_start;
        u8_t mpidx;//current store mountpoint index
        u8_t didx1;//current 1th level store dir
        u8_t didx2;//current 2th level store dir
        u64_t storecount;
        u32_t binlog_idx;
    };

    extern struct ydb_storage_state *g_ydb_storage_state;
struct ydb_storage_state *ydb_storage_state_init(SpxLogDelegate *log,err_t *err);

#ifdef __cplusplus
}
#endif
#endif
