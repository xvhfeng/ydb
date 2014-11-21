/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_delete.h
 *        Created:  2014/08/14 17时15分01秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_DELETE_H_
#define _YDB_STORAGE_DELETE_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "spx_types.h"

#include "ydb_storage_dio_context.h"
#include "ydb_storage_configurtion.h"

err_t ydb_storage_dio_delete(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc);

err_t ydb_storage_dio_delete_context_from_chunkfile(
        struct ydb_storage_configurtion *c,
        string_t fname,
        u64_t cbegin,
        u64_t ctotalsize,
        u32_t copver,
        u32_t cver,
        u64_t clastmodifytime,
        u64_t crealsize,
        u64_t lastmodifytime);

void ydb_storage_dio_do_delete_form_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents);
#ifdef __cplusplus
}
#endif
#endif
