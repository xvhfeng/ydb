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
#include "include/spx_properties.h"

    struct ydb_storage_mouintpoint{
        int idx;
        string_t path;
        u64_t disksize;
        u64_t freesize;
    };

#define YDB_STORAGE_MOUNTPOINT_LOOP 0
#define YDB_STORAGE_MOUNTPOINT_MAXSIZE 1
#define YDB_STORAGE_MOUNTPOINT_TRUN 2
#define YDB_STORAGE_MOUNTPOINT_MASTER 3
#define YDB_STORAGE_MOUNTPOINT_COUNT 256

#define YDB_STORAGE_STOREMODE_LOOP 0
#define YDB_STORAGE_STOREMODE_TURN 1

#define YDB_STORAGE_OVERMODE_RELATIVE 0
#define YDB_STORAGE_OVERMODE_ABSSOLUTE 1

    extern string_t ydb_storage_config_ip_key;
    extern string_t ydb_storage_config_port_key;
    extern string_t ydb_storage_config_timeout_key;
    extern string_t ydb_storage_config_basepath_key;
    extern string_t ydb_storage_config_logpath_key;
    extern string_t ydb_storage_config_logprefix_key;
    extern string_t ydb_storage_config_logsize_key;
    extern string_t ydb_storage_config_loglevel_key;
    extern string_t ydb_storage_config_balance_key;
    extern string_t ydb_storage_config_master_key;
    extern string_t ydb_storage_config_heartbeat_key;
    extern string_t ydb_storage_config_daemon_key ;
    extern string_t ydb_storage_config_niosize_key;
    extern string_t ydb_storage_config_siosize_key;
    extern string_t ydb_storage_config_stacksize_key;
    extern string_t ydb_storage_config_groupname_key;
    extern string_t ydb_storage_config_machineid_key;
    extern string_t ydb_storage_config_diosize_key;
    extern string_t ydb_storage_config_freedisk_key;
    extern string_t ydb_storage_config_mountpoint_key;
    extern string_t ydb_storage_config_tracker_key;
    extern string_t ydb_storage_config_storepaths_key;
    extern string_t ydb_storage_config_storemode_key;
    extern string_t ydb_storage_config_storecount_key;
    extern string_t ydb_storage_config_hole_key;
    extern string_t ydb_storage_config_compress_key;
    extern string_t ydb_storage_config_chunkfile_key;
    extern string_t ydb_storage_config_chunksize_key;
    extern string_t ydb_storage_config_overload_key;
    extern string_t ydb_storage_config_overmode_key;//this operator is not config and from the oversize
    extern string_t ydb_storage_config_oversize_key;

    extern int ydb_storage_status;
    extern u64_t ydb_storage_first_start;

void ydb_storage_config_line_deserialize(string_t line,struct spx_properties *p,err_t *err);

#ifdef __cplusplus
}
#endif
#endif
