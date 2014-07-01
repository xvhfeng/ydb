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
#include "include/spx_properties.h"

#define YDB_TRACKER_BALANCE_LOOP 0
#define YDB_TRACKER_BALANCE_MAXDISK 1
#define YDB_TRACKER_BALANCE_TURN 2
#define YDB_TRACKER_BALANCE_MASTER 3

    extern string_t ydb_tracker_config_ip_key;
    extern string_t ydb_tracker_config_port_key;
    extern string_t ydb_tracker_config_timeout_key;
    extern string_t ydb_tracker_config_basepath_key;
    extern string_t ydb_tracker_config_logpath_key;
    extern string_t ydb_tracker_config_logprefix_key;
    extern string_t ydb_tracker_config_logsize_key;
    extern string_t ydb_tracker_config_loglevel_key;
    extern string_t ydb_tracker_config_balance_key;
    extern string_t ydb_tracker_config_master_key;
    extern string_t ydb_tracker_config_heartbeat_key;
    extern string_t ydb_tracker_config_daemon_key ;
    extern string_t ydb_tracker_config_niosize_key;
    extern string_t ydb_tracker_config_siosize_key;
    extern string_t ydb_tracker_config_stacksize_key;

    err_t ydb_tracker_config_parser_before_handle();
    void ydb_tracker_config_line_deserialize(string_t line,struct spx_properties *p,err_t *err);

#ifdef __cplusplus
}
#endif
#endif
