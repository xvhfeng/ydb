/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_modify.h
 *        Created:  2014/08/22 16时40分39秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_MODIFY_H_
#define _YDB_STORAGE_MODIFY_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "spx_types.h"
#include "ydb_storage_dio_context.h"


err_t ydb_storage_dio_modify(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);

#ifdef __cplusplus
}
#endif
#endif
