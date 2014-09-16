/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_dio_context.h
 *        Created:  2014/08/07 13时45分50秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_DIO_CONTEXT_H_
#define _YDB_STORAGE_DIO_CONTEXT_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "spx_types.h"
#include "spx_job.h"
#include "spx_task.h"
#include "spx_fixed_vector.h"
#include "spx_message.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_storefile.h"

#define YdbBufferSizeForLazyRecv 10 * 1024



    struct ydb_storage_dio_context{
        ev_async async;
        size_t idx;
        SpxLogDelegate *log;
        struct ydb_storage_configurtion *c;

        u32_t ver;//version for suppert operator
        u32_t opver;//the operator in the chunk data version
        err_t err;

        bool_t isdelete;
        u64_t createtime;// the buffer put into the disk time first time
        u64_t lastmodifytime;//the buffer last modify
        string_t hashcode;//length 64,now the operator is space and no useful
        u64_t totalsize;//if file is single file totalsize == realsize
        u64_t realsize;


        string_t groupname;// from configurtion
        string_t machineid;//same as above
        u64_t file_createtime;//if it is single file then the operator equ creattime
        u8_t mp_idx;
        u8_t p1;//path of level 1
        u8_t p2;//path of level 2
        u32_t tidx;
        u32_t rand;
        bool_t issignalfile;
        u64_t begin;
        bool_t has_suffix;
        string_t suffix;

        struct spx_job_context *jc;
        struct spx_task_context *tc;
        struct ydb_storage_storefile *storefile;//disk file for put into buffer

        //network buffer for lazy recv,
        //and if the op is find or delete,
        //then the member is filename
        string_t buf;
        struct spx_msg *metadata;//if the file is chunkfile the member store metadata
    };

    struct ydb_storage_dio_pool{
        SpxLogDelegate *log;
        struct spx_fixed_vector *pool;
    };

    extern struct ydb_storage_dio_pool *g_ydb_storage_dio_pool;

    struct ydb_storage_dio_pool *ydb_storage_dio_pool_new(SpxLogDelegate *log,\
            size_t size,\
            err_t *err);

    struct ydb_storage_dio_context *ydb_storage_dio_pool_pop(\
            struct ydb_storage_dio_pool *pool,\
            err_t *err);

    err_t ydb_storage_dio_pool_push(\
            struct ydb_storage_dio_pool *pool,\
            struct ydb_storage_dio_context *dc);

    err_t ydb_storage_dio_pool_free(struct ydb_storage_dio_pool **pool);


#ifdef __cplusplus
}
#endif
#endif
