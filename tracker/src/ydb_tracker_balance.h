/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_balance.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/25 18时01分48秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#ifndef _YDB_TRACKER_BALANCE_H_
#define _YDB_TRACKER_BALANCE_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "include/spx_types.h"
#include "include/spx_nio_context.h"

err_t ydb_tracker_query_upload_storage(int fd,struct spx_nio_context *nio_context);
err_t ydb_tracker_query_modify_storage(int fd,struct spx_nio_context *nio_context);
err_t ydb_tracker_query_delete_storage(int fd,struct spx_nio_context *nio_context);
err_t ydb_tracker_query_select_storage(int fd,struct spx_nio_context *nio_context);

#ifdef __cplusplus
}
#endif
#endif
