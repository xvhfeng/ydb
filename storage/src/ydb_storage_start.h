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

#include <stdlib.h>
#include <stdio.h>

#include "spx_types.h"
#include "ydb_storage_configurtion.h"


err_t ydb_storage_mountpoint_init(struct ydb_storage_configurtion *c);
err_t ydb_storage_mountpoint_initfile_writer(struct ydb_storage_configurtion *c,
        struct ydb_storage_mountpoint *mp);

#ifdef __cplusplus
}
#endif
#endif
