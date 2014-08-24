/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_storefile.h
 *        Created:  2014/08/07 15时45分16秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_STOREFILE_H_
#define _YDB_STORAGE_STOREFILE_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>

#include "include/spx_types.h"
#include "include/spx_list.h"


struct ydb_storage_storefile{
    struct {
        u8_t mpidx;
        u8_t p1;
        u8_t p2;
        int proxyfd;
        int fd;
        char *mptr;
        off_t offset;
        u64_t fcreatetime;
        u32_t rand;
        string_t filename;
        struct spx_queue *dio_queue;
    }chunkfile;
    struct {
        u8_t mpidx;
        u8_t p1;
        u8_t p2;
        int fd;
        char *mptr;
        off_t offset;
        u64_t fcreatetime;
        u32_t rand;
        string_t filename;
    }singlefile;
    string_t machineid;
    u32_t tidx;
};


struct ydb_storage_storefile_pool{
    SpxLogDelegate *log;
    struct spx_list *p;
};

extern struct ydb_storage_storefile_pool *g_ydb_storage_storefile_pool;
struct ydb_storage_storefile_pool *ydb_storage_storefile_pool_new(\
        SpxLogDelegate *log,\
        size_t size,err_t *err);

struct ydb_storage_storefile *ydb_storage_storefile_get(\
        struct ydb_storage_storefile_pool *pool,int idx);

err_t ydb_storage_storefile_pool_free(struct ydb_storage_storefile_pool **pool);

#ifdef __cplusplus
}
#endif
#endif
