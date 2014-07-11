/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_start.h
 *        Created:  2014/07/08 16时16分47秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_START_H_
#define _YDB_STORAGE_START_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "include/spx_types.h"
#include "include/spx_properties.h"

err_t ydb_storage_fixed_start_point_set(SpxLogDelegate *log,struct spx_properties *p);
err_t ydb_storage_fixed_start_point_reset(SpxLogDelegate *log,struct spx_properties *p);

#include <stdlib.h>
#include <stdio.h>

#ifdef __cplusplus
}
#endif
#endif
