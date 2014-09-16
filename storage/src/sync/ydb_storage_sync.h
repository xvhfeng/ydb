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

#include "include/spx_types.h"
#include "include/spx_socket.h"
#include "include/spx_vector.h"

#include "ydb_storage_configurtion.h"

    /*
     * this struct is the runtime status of slave storage,
     * it store in the master-stotage,and store into disk every 30 seconds
     */
    struct ydb_storage_slave{
        string_t machineid;
        int state;
        struct spx_host host;
        struct spx_date read_binlog_date;
        u64_t read_binlog_offset;
        u64_t timespan;
        bool_t is_syncing;
        struct ydb_storage_configurtion *c;
    };

    /* this struct is the runtime status of the master storage,
     * it store in the slave-storage,and storage into disk every 30 seconds,
     * and it useful when starting syncing for judging the postion of the begin*/
    struct ydb_storage_master{
        string_t machineid;
        u32_t remote_sync_binlog_idx;
        u64_t remote_sync_binlog_offet;
        u64_t sync_timespan;
    };

extern struct spx_map *g_ydb_storage_slaves;

string_t ydb_storage_make_marklog_filename(struct ydb_storage_configurtion *c,err_t *err);

#ifdef __cplusplus
}
#endif
#endif
