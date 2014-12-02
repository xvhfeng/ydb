#ifndef _YDB_STORAGE_DISKSYNC_H_
#define _YDB_STORAGE_DISKSYNC_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "ydb_storage_configurtion.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_dio_context.h"

err_t ydb_storage_dsync_startup(struct ydb_storage_configurtion *c,
        struct ydb_storage_runtime *srt);

err_t ydb_storage_dsync_sync_data(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);

err_t ydb_storage_dsync_logfile(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);


#ifdef __cplusplus
}
#endif
#endif
