/*
 * =====================================================================================
 *
 *       Filename:  ydb_storage_configurtion.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/07/01 15时40分43秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_CONFIGURTION_H_
#define _YDB_STORAGE_CONFIGURTION_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "spx_types.h"
#include "spx_job.h"
#include "spx_socket.h"

    struct ydb_storage_transport_context{
        int fd;
        struct spx_msg_context *request;
        struct spx_msg_context *response;
    };

    //there is mp.rtf file in the every mountpoint
    //and the file context is
    //is_disk_sync | last_modify_time | availlysize

    //and theresi mp.init file in the every mountpoint
    //and the file context is the current time of the initing
    struct ydb_storage_mountpoint{
        int idx;
        string_t path;
        u64_t disksize;
        u64_t freesize;//now
        time_t last_modify_time;
        time_t init_timespan;
        u64_t last_freesize;
        bool_t need_dsync;
        bool_t isusing;
    };

#define YDB_STORAGE_MOUNTPOINT_LOOP 0
#define YDB_STORAGE_MOUNTPOINT_MAXSIZE 1
#define YDB_STORAGE_MOUNTPOINT_TURN 2
#define YDB_STORAGE_MOUNTPOINT_MASTER 3
    spx_private  char *mountpoint_balance_mode_desc[] = {
        "loop",
        "maxsize",
        "turn",
        "master"
    };

#define YDB_STORAGE_MOUNTPOINT_COUNT 256

#define YDB_STORAGE_STOREMODE_LOOP 0
#define YDB_STORAGE_STOREMODE_TURN 1
    spx_private char *storemode_desc[] = {
        "loop",
        "turn"
    };

#define YDB_STORAGE_OVERMODE_RELATIVE 0
#define YDB_STORAGE_OVERMODE_ABSSOLUTE 1
    spx_private char *overmode_desc[]={
        "relative",
        "abssolute"
    };

#define YDB_STORAGE_HOLEREFRESH_FIXEDTIME 0
#define YDB_STORAGE_HOLEREFRESH_TIMESTAMPS 1
    spx_private char *holerefresh_mode_desc[]={
        "fixed",
        "timestamps"
    };

#define YDB_STORAGE_SYNC_REALTIME 0
#define YDB_STORAGE_SYNC_FIXEDTIME 1
    spx_private char *sync_mode_desc[] = {
        "realtime",
        "fixedtime"
    };

struct ydb_tracker{
    struct spx_host host;
    struct spx_job_context *hjc;//heartbeat job context
    struct ydb_storage_transport_context *ystc;
};

    struct ydb_storage_configurtion{
        SpxLogDelegate *log;
        string_t ip;
        i32_t port;
        u32_t timeout;
        string_t basepath;
        string_t logpath;
        string_t logprefix;
        u64_t logsize;
        i8_t loglevel;
        u64_t binlog_size;
        u32_t runtime_flush_timespan;
        i8_t balance;
        u8_t master;
        u32_t heartbeat;
        bool_t daemon;
        u32_t notifier_module_thread_size;
        u32_t network_module_thread_size;
        u32_t task_module_thread_size;
        u32_t context_size;
        u64_t stacksize;
        string_t groupname;
        string_t machineid;
        u64_t freedisk;
        struct spx_list *mountpoints;
        struct spx_vector *trackers;
        i32_t storerooms;
        i8_t storemode;
        u32_t storecount;
        bool_t fillchunk;
        i8_t holerefresh;
        u64_t refreshtime;
        bool_t compress;
        bool_t chunkfile;
        u64_t chunksize;
        bool_t overload;
        u64_t oversize;
        i8_t overmode;
        u64_t singlemin;
        bool_t lazyrecv;
        u64_t lazysize;
        bool_t sendfile;
        size_t pagesize;
        u8_t sync;
        u32_t query_sync_timespan;
        string_t syncgroup;
        u32_t sync_wait;
        struct spx_time sync_begin;
        struct spx_time sync_end;
        u32_t disksync_timespan;
        u64_t disksync_busysize;
        time_t start_timespan;
    };

    void *ydb_storage_config_before_handle(SpxLogDelegate *log,err_t *err);
    void ydb_storage_config_line_parser(string_t line,void *config,err_t *err);


#ifdef __cplusplus
}
#endif
#endif
