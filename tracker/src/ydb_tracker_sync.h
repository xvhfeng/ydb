/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_tracker_sync.h
 *        Created:  2014/08/25 17时42分26秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_TRACKER_SYNC_H_
#define _YDB_TRACKER_SYNC_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "spx_task.h"

err_t ydb_tracker_query_sync_storage(struct ev_loop *loop,\
        struct spx_task_context *tcontext);

err_t ydb_tracker_query_base_storage(struct ev_loop *loop,\
        struct spx_task_context *tcontext);

err_t ydb_tracker_query_timespan_for_begining_sync(struct ev_loop *loop,\
        struct spx_task_context *tcontext);

#ifdef __cplusplus
}
#endif
#endif
