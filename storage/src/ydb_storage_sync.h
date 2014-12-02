#ifndef _YDB_STORAGE_SYNC_H_
#define _YDB_STORAGE_SYNC_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <ev.h>

#include "spx_types.h"
#include "spx_map.h"
#include "spx_threadpool.h"

#include "ydb_protocol.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_synclog.h"
#include "ydb_storage_dio_context.h"
#include "ydb_storage_runtime.h"


#define YDB_STORAGE_REMOTE_SYNCSTATE_NONE 0
#define YDB_STORAGE_REMOTE_SYNCSTATE_DSYNCING 1
#define YDB_STORAGE_REMOTE_SYNCSTATE_syncING 2

extern struct spx_threadpool *g_sync_threadpool;

struct ydb_storage_sync_beginpoint{
            struct spx_date date;
            u64_t offset;
};


struct ydb_storage_sync_context {
    int sock;
    int fd;
    char *ptr;
    size_t len;
    struct spx_msg_context *request;
    struct spx_msg_context *response;
    struct ydb_storage_fid *fid;
    struct spx_msg *md;
    string_t fname;
    string_t smd;
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
    struct ydb_storage_synclog synclog;
    bool_t is_doing;
    bool_t is_restore_over;
};


extern struct spx_map *g_ydb_storage_remote;

void *ydb_storage_sync_query_remote_storages(void *arg);
void *ydb_storage_sync_heartbeat(void *arg);

bool_t ydb_storage_sync_consistency(
        struct ydb_storage_configurtion *c);

struct spx_periodic *ydb_storage_sync_startup(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_runtime *srt,
        err_t *err);

err_t ydb_storage_sync_reply_sync_beginpoint(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);

err_t ydb_storage_sync_reply_begin(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);

err_t ydb_storage_ydb_storage_sync_reply_consistency(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);

err_t ydb_storage_sync_restore(
        struct ydb_storage_configurtion *c);

err_t ydb_storage_sync_state_writer(
        struct ydb_storage_configurtion *c);

void ydb_storage_sync_context_free(
        struct ydb_storage_sync_context **ysdc);

err_t ydb_storage_sync_upload(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);

err_t ydb_storage_sync_delete(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);

err_t ydb_storage_sync_modify(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);




#ifdef __cplusplus
}
#endif
#endif
