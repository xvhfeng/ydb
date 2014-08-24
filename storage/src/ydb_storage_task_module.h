/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_task_module.h
 *        Created:  2014/07/31 11时16分01秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */



#ifndef _YDB_STORAGE_TASK_MODULE_H_
#define _YDB_STORAGE_TASK_MODULE_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>
#include <ev.h>


#include "include/spx_types.h"
#include "include/spx_task.h"
#include "include/spx_job.h"
#include "include/spx_task_module.h"
#include "include/spx_network_module.h"

err_t ydb_storage_task_module_handler(struct ev_loop *loop,struct spx_task_context *tcontext);

#ifdef __cplusplus
}
#endif
#endif
