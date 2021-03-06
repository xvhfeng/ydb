/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_network_module.h
 *        Created:  2014/07/31 11时15分43秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_NETWORK_MODULE_H_
#define _YDB_STORAGE_NETWORK_MODULE_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>

#include "spx_types.h"
#include "spx_job.h"


bool_t ydb_storage_network_module_header_validator_handler(struct spx_job_context *jcontext);
void ydb_storage_network_module_header_validator_fail_handler(struct spx_job_context *jcontext);
void ydb_storage_network_module_request_body_before_handler(struct spx_job_context *jc);
void ydb_storage_network_module_request_body_handler(
        struct ev_loop *loop,int fd,struct spx_job_context *jcontext);
void ydb_storage_network_module_response_body_handler(
        struct ev_loop  *loop,int fd,struct spx_job_context *jcontext);

#ifdef __cplusplus
}
#endif
#endif
