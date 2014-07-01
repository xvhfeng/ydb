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

#include "include/spx_types.h"
#include "include/spx_nio_context.h"

    struct ydb_remote_storage{
        u64_t fisrt_start;
        u64_t last_heartbeat;
        u64_t disksize;
        u64_t freesize;
        u32_t status;
        string_t machineid;
        string_t groupname;
        string_t ip;
        int port;
    };

extern struct spx_map *ydb_remote_storages;

err_t ydb_tracker_regedit_from_storage(int fd,struct spx_nio_context *nio_context);
err_t ydb_tracker_heartbeat_from_storage(int fd,struct spx_nio_context *nio_context);
err_t ydb_tracker_shutdown_from_storage(int fd,struct spx_nio_context *nio_context);

#ifdef __cplusplus
}
#endif
#endif
