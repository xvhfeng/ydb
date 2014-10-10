/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_dio.h
 *        Created:  2014/07/31 16时57分30秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_UPLOAD_H_
#define _YDB_STORAGE_UPLOAD_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "spx_types.h"
#include "spx_job.h"
#include "spx_task.h"

#include "ydb_storage_configurtion.h"

#include "ydb_storage_storefile.h"
#include "ydb_storage_dio_context.h"

    struct ydb_storage_hole{
        string_t machineid;
        u8_t mp_idx;
        u32_t thread_idx;
        u64_t file_createtime;
        u32_t rand;
        u64_t begin;
        u64_t totalsize;
    };


    err_t ydb_storage_dio_upload(struct ev_loop *loop,\
            struct ydb_storage_dio_context *dc);


    err_t ydb_storage_dio_upload_to_chunkfile(
            struct ydb_storage_dio_context *dc);

    err_t ydb_storage_dio_upload_to_singlefile(
            struct ydb_storage_dio_context *dc);
#ifdef __cplusplus
}
#endif
#endif
