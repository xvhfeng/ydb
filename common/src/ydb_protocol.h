/*
 * =====================================================================================
 *
 *       Filename:  ydb_protocol.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/24 10时06分28秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#ifndef _YDB_PROTOCOL_H_
#define _YDB_PROTOCOL_H_
#ifdef __cplusplus
extern "C" {
#endif

#define YDB_VERSION 1

#define YDB_GROUPNAME_LEN 7
#define YDB_MACHINEID_LEN 7

#define YDB_REGEDIT_STORAGE 1
#define YDB_HEARTBEAT_STORAGE 2
#define YDB_SHUTDOWN_STORAGE 3

#define YDB_TRACKER_QUERY_UPLOAD_STORAGE 4
#define YDB_TRACKER_QUERY_SELECT_STORAGE 5
#define YDB_TRACKER_QUERY_DELETE_STORAGE 6
#define YDB_TRACKER_QUERY_MODIFY_STORAGE 7

#define YDB_QUERY_SYNC_STORAGES 8
#define YDB_QUERY_STORAGE_STATUS 9

#define YDB_STORAGE_NORMAL 0
#define YDB_STORAGE_INITING 1
#define YDB_STORAGE_SYNCING 2
#define YDB_STORAGE_RUNNING 3
#define YDB_STORAGE_CLOSING 4
#define YDB_STORAGE_CLOSED 5

#ifdef __cplusplus
}
#endif
#endif
