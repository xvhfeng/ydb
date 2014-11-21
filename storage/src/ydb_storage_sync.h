#ifndef _YDB_STORAGE_SYNC_H_
#define _YDB_STORAGE_SYNC_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "spx_types.h"
#include "spx_map.h"
#include "spx_threadpool.h"

#include "ydb_protocol.h"
#include "ydb_storage_binlog.h"


#define YDB_STORAGE_REMOTE_SYNCSTATE_NONE 0
#define YDB_STORAGE_REMOTE_SYNCSTATE_DSYNCING 1
#define YDB_STORAGE_REMOTE_SYNCSTATE_CSYNCING 2

extern struct spx_threadpool *g_sync_threadpool;

struct ydb_storage_csync_beginpoint{
            struct spx_date date;
            u64_t offset;
};

struct ydb_storage_remote{
    struct ydb_storage_configurtion *c;
    string_t machineid;
    struct spx_host host;
    i32_t runtime_state;
    u64_t update_timespan;
    struct {
        struct spx_date date;
        FILE *fp;
        u64_t offset;
        string_t fname;
    }read_binlog;
    struct {
        struct spx_date date;
        FILE *fp;
        u64_t offset;
        string_t fname;
    }synclog;
    bool_t is_doing;
    bool_t is_restore_over;
};


    extern struct spx_map *g_ydb_storage_remote;

struct spx_periodic *ydb_storage_sync_query_storages_init(
        struct ydb_storage_configurtion *c,err_t *err);

#ifdef __cplusplus
}
#endif
#endif
