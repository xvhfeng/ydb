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

#include "include/spx_types.h"

    struct ydb_storage_mountpoint{
        int idx;
        string_t path;
        u64_t disksize;
        u64_t freesize;
    };

#define YDB_STORAGE_MOUNTPOINT_LOOP 0
#define YDB_STORAGE_MOUNTPOINT_MAXSIZE 1
#define YDB_STORAGE_MOUNTPOINT_TURN 2
#define YDB_STORAGE_MOUNTPOINT_MASTER 3
    const  char *mountpoint_balance_mode_desc[] = {
        "loop",
        "maxsize",
        "turn",
        "master"
    };

#define YDB_STORAGE_MOUNTPOINT_COUNT 256

#define YDB_STORAGE_STOREMODE_LOOP 0
#define YDB_STORAGE_STOREMODE_TURN 1
    const char *storemode_desc[] = {
        "loop",
        "turn"
    };

#define YDB_STORAGE_OVERMODE_RELATIVE 0
#define YDB_STORAGE_OVERMODE_ABSSOLUTE 1
    const char *overmode_desc[]={
        "relative",
        "abssolute"
    };

#define YDB_STORAGE_HOLEREFRESH_FIXEDTIME 0
#define YDB_STORAGE_HOLEREFRESH_TIMESTAMPS 1
    const char *holerefresh_mode_desc[]={
        "fixed",
        "timestamps"
    };


    extern int ydb_storage_status;
    extern u64_t ydb_storage_first_start;

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
        i8_t balance;
        string_t master;
        u32_t heartbeat;
        bool_t daemon;
        u32_t nio_thread_size;
        u32_t nio_context_size;
        u32_t sio_thread_size;
        u64_t stacksize;
        string_t groupname;
        string_t machineid;
        u64_t freedisk;
        u32_t dio_thread_size;
        u32_t dio_context_size;
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
        bool_t lazysize;
        bool_t sendfile;

    };


    void *ydb_storage_config_before_handle(SpxLogDelegate *log,err_t *err);
    void ydb_storage_config_line_parser(string_t line,void *config,err_t *err);


#ifdef __cplusplus
}
#endif
#endif