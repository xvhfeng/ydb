/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_sync.h
 *        Created:  2014/08/14 15时46分49秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

/*
 * 1:query tracker for storage
 * 2:judge which storage need sync and it state is OK
 * 3:query sync state from remote storage and judge with local
 * 4:sync with push
 **/

#ifndef _YDB_STORAGE_SYNC_H_
#define _YDB_STORAGE_SYNC_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>

#include "include/spx_socket.h"
    struct ydb_storage_slave_sync_runtime{
        string_t machineid;
        int state;
        struct spx_host host;
        u32_t read_binlog_idx;
        u64_t read_binlog_offset;
        bool_t is_syncing;
    };

    struct ydb_storage_master_sync_runtime{
        string_t machineid;
        u32_t remote_sync_binlog_idx;
        u64_t remote_sync_binlog_offet;
        u64_t sync_timespan;
    };




#ifdef __cplusplus
}
#endif
#endif
