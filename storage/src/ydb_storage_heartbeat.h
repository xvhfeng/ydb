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

#include "include/spx_socket.h"
#include "include/spx_job.h"

#include "ydb_storage_configurtion.h"

pthread_t ydb_storage_heartbeat_service_init(
        SpxLogDelegate *log,
        u32_t timeout,
        SpxNioDelegate *nio_reader,
        SpxNioDelegate *nio_writer,
        SpxNioHeaderValidatorDelegate *reader_header_validator,
        SpxNioHeaderValidatorFailDelegate *reader_header_validator_fail,
        SpxNioBodyProcessDelegate *reader_body_process,
        SpxNioBodyProcessDelegate *writer_body_process,
        struct ydb_storage_configurtion *config,\
        err_t *err);
    void ydb_storage_shutdown(struct spx_host *tracker,\
            string_t groupname,string_t machineid,\
            string_t ip,int port,\
            u64_t first_start,u64_t disksize,u64_t freesize);
    void ydb_storage_heartbeat_nio_writer_body_handler(\
            int fd,struct spx_job_context *jc,size_t size);

#ifdef __cplusplus
}
#endif
#endif
