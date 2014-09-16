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

#include "spx_socket.h"
#include "spx_job.h"

#include "ydb_storage_configurtion.h"

pthread_t ydb_storage_heartbeat_service_init(
        SpxLogDelegate *log,
        u32_t timeout,
        struct ydb_storage_configurtion *config,\
        err_t *err);

void ydb_storage_shutdown(struct ev_loop *loop,struct ydb_tracker *tracker,\
        string_t groupname,string_t machineid,\
        string_t syncgroup, string_t ip,int port,\
        u64_t first_start,u64_t disksize,u64_t freesize);

#ifdef __cplusplus
}
#endif
#endif
