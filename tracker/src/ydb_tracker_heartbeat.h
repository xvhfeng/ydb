/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_heartbeat.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/24 11时27分27秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#ifndef _YDB_TRACKER_HEARTBEAT_H_
#define _YDB_TRACKER_HEARTBEAT_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <ev.h>

#include "spx_types.h"
#include "spx_task.h"

    struct ydb_remote_storage{
        u64_t fisrt_start;
        u64_t last_heartbeat;
        u64_t disksize;
        u64_t freesize;
        u32_t status;
        string_t machineid;
        string_t groupname;
        string_t syncgroup;
        string_t ip;
        int port;
    };

extern struct spx_map *ydb_remote_storages;

err_t ydb_tracker_regedit_from_storage(struct ev_loop *loop,struct spx_task_context *tcontext);
err_t ydb_tracker_heartbeat_from_storage(struct ev_loop *loop,struct spx_task_context *tcontext);
err_t ydb_tracker_shutdown_from_storage(struct ev_loop *loop,struct spx_task_context *tcontext);

#ifdef __cplusplus
}
#endif
#endif
