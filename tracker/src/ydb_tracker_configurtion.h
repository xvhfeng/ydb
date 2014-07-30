/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_configurtion.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/20 17时13分18秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#ifndef _YDB_TRACKER_CONFIGURTION_H_
#define _YDB_TRACKER_CONFIGURTION_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "include/spx_types.h"

#define YDB_TRACKER_BALANCE_LOOP 0
#define YDB_TRACKER_BALANCE_MAXDISK 1
#define YDB_TRACKER_BALANCE_TURN 2
#define YDB_TRACKER_BALANCE_MASTER 3

#define ToYdbTrackerConfigurtion(p) \
    (struct ydb_tracker_configurtion *) p

    const char *tracker_balance_mode_desc[]={
        "loop",
        "maxdisk",
        "turn",
        "master",
    };

    struct ydb_tracker_configurtion{
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
        u64_t stacksize;
        u32_t notifier_module_thread_size;
        u32_t network_module_thread_size;
        u32_t task_module_thread_size;
        u32_t context_size;
    };


    void *ydb_tracker_config_before_handle(SpxLogDelegate *log,err_t *err);
    void ydb_tracker_config_line_parser_handle(string_t line,void *config,err_t *err);

#ifdef __cplusplus
}
#endif
#endif
