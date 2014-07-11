/*
 * =====================================================================================
 *
 *       Filename:  ydb_storage_heartbeat.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/07/01 13时58分49秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_HEARTBEAT_H_
#define _YDB_STORAGE_HEARTBEAT_H_
#ifdef __cplusplus
extern "C" {
#endif

/*
 * msg:groupname + machienid + ip + port + first-start-timestamp + disksize
 *      + freesize + status
 *
 * length:GROUPNAMELEN + MACHINEIDLEN + SpxIpv4Size + sizeof(int) +
 * sizeof(u64_t) + sizeof(u64_t) + sizeof(64_t) + sizeof(int)
 *
 *
 */
#include "include/spx_nio_context.h"

    void ydb_storage_regedit();
    void ydb_storage_report();
    void ydb_storage_shutdown();
void ydb_storage_heartbeat_nio_writer_body_handler(int fd,struct spx_nio_context *nio_context);

#ifdef __cplusplus
}
#endif
#endif
