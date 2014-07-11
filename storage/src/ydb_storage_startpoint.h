/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_startpoint.h
 *        Created:  2014/07/10 00时07分41秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_STARTPOINT_H_
#define _YDB_STORAGE_STARTPOINT_H_
#ifdef __cplusplus
extern "C" {
#endif

#include "include/spx_types.h"
#include "include/spx_properties.h"

err_t ydb_storage_startpoint_load(SpxLogDelegate *log,struct spx_properties *p);
err_t ydb_storage_startpoint_reset(SpxLogDelegate *log,struct spx_properties *p);

#ifdef __cplusplus
}
#endif
#endif
