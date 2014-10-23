#ifndef _YDB_STORAGE_SYNC_H_
#define _YDB_STORAGE_SYNC_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "spx_types.h"

#include "ydb_protocol.h"
#include "ydb_storage_binlog.h"

    struct ydb_storage_sync_mp{
        u8_t mpidx;
        time_t last_sync_pull_time;
        time_t last_sync_push_time;
    };

    struct ydb_storage_remote{
        string_t machineid;
        i32_t runtime_state;
        bool_t is_syncing;
        struct ydb_storage_binlog *read_binlog;
        struct ydb_storage_synclog *write_synclog;
        struct ydb_storage_sync_mp sync_mps[YDB_STORAGE_MOUNTPOINT_COUNT];
    };


#ifdef __cplusplus
}
#endif
#endif
