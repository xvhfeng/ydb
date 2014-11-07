#ifndef _YDB_STORAGE_SYNC_H_
#define _YDB_STORAGE_SYNC_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "spx_types.h"
#include "spx_map.h"

#include "ydb_protocol.h"
#include "ydb_storage_binlog.h"

#define YDB_STORAGE_REMOTE_SYNCSTATE_NONE 0
#define YDB_STORAGE_REMOTE_SYNCSTATE_DSYNCING 1
#define YDB_STORAGE_REMOTE_SYNCSTATE_CSYNCING 2

    struct ydb_storage_sync_mp{
        u8_t mpidx;
        time_t last_sync_pull_time;
        time_t last_sync_push_time;
    };

    struct ydb_storage_sync_remote{
        string_t machineid;
        struct spx_host host;
        i32_t runtime_state;
        int sync_state;
        u64_t update_timespan;
        struct ydb_storage_binlog *read_binlog;
        struct ydb_storage_synclog *write_synclog;
        struct ydb_storage_sync_mp sync_mps[YDB_STORAGE_MOUNTPOINT_COUNT];
    };

    extern struct spx_map *g_ydb_storage_remote;

struct spx_periodic *ydb_storage_sync_query_storages_init(
        struct ydb_storage_configurtion *c,err_t *err);

#ifdef __cplusplus
}
#endif
#endif
