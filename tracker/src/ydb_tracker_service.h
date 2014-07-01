/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_service.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/23 18时04分09秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#ifndef _YDB_TRACKER_SERVICE_H_
#define _YDB_TRACKER_SERVICE_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "include/spx_types.h"
#include "include/spx_nio_context.h"

bool_t ydb_tracker_nio_header_validator_handler(struct spx_nio_context *nio_context);
void ydb_tracker_nio_header_validator_fail_handler(struct spx_nio_context *nio_context);
void ydb_tracker_nio_request_body_handler(int fd,struct spx_nio_context *nio_context);
void ydb_tracker_nio_response_body_handler(int fd,struct spx_nio_context *nio_context);

#ifdef __cplusplus
}
#endif
#endif
