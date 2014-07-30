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
#include "include/spx_task.h"

err_t ydb_tracker_query_upload_storage(struct ev_loop *loop,struct spx_task_context *tcontext);
err_t ydb_tracker_query_modify_storage(struct ev_loop *loop,struct spx_task_context *tcontext);
err_t ydb_tracker_query_delete_storage(struct ev_loop *loop,struct spx_task_context *tcontext);
err_t ydb_tracker_query_select_storage(struct ev_loop *loop,struct spx_task_context *tcontext);

#ifdef __cplusplus
}
#endif
#endif
