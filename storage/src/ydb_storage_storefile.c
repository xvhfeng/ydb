/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_storefile.c
 *        Created:  2014/08/07 15时45分21秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include "spx_types.h"
#include "spx_list.h"
#include "spx_alloc.h"
#include "spx_defs.h"
#include "spx_queue.h"

#include "ydb_storage_storefile.h"

 struct ydb_storage_storefile_pool *g_ydb_storage_storefile_pool = NULL;

spx_private void *ydb_storage_storefile_new(size_t idx,void *arg,err_t *err);
spx_private err_t ydb_storage_storefile_free(void **arg);


spx_private void *ydb_storage_storefile_new(size_t idx,void *arg,err_t *err){
    SpxLogDelegate *log = (SpxLogDelegate *) arg;
    struct ydb_storage_storefile *f = (struct ydb_storage_storefile *) \
                                      spx_alloc_alone(sizeof(*f),err);
    if(NULL == f){
        SpxLog2(log,SpxLogError,*err,\
                "new node for storage storefile pool is fail.");
        return NULL;
        }
    f->tidx = idx;
//    f->chunkfile.proxyfd = open("/dev/null",O_RDWR);
//    if(0 >= f->chunkfile.proxyfd){
//        SpxLog2(log,SpxLogError,*err,
//                "open /dev/null for proxyfd is fail.");
//        goto r1;
//    }

//    f->chunkfile.dio_queue =  spx_queue_new(log,
//             ydb_storage_storefile_free,
//           err);
//    if(NULL == f->chunkfile.dio_queue){
//        SpxLog2(log,SpxLogError,*err,
//                "init dio queue is fail.");
//        goto r1;
//    }
    return f;
//r1:
//    if(0 < f->chunkfile.proxyfd){
//        SpxClose(f->chunkfile.proxyfd);
//    }
//    SpxFree(f);
//    return NULL;
}

spx_private err_t ydb_storage_storefile_free(void **arg){
    struct ydb_storage_storefile **f = (struct ydb_storage_storefile **) arg;
    if(NULL == f){
        return 0;
    }
    SpxFree(*f);
    return 0;
}



struct ydb_storage_storefile_pool *ydb_storage_storefile_pool_new(\
        SpxLogDelegate *log,\
        size_t size,err_t *err){
    struct ydb_storage_storefile_pool *p = (struct ydb_storage_storefile_pool *)\
                                           spx_alloc_alone(sizeof(*p),err);
    if(NULL == p){
        SpxLog2(log,SpxLogError,*err,\
                "new storage storefile pool is fail.");
        return NULL;
    }
    p->log = log;
    p->p = spx_list_init(log,\
            size,\
            ydb_storage_storefile_new,\
            NULL,
            ydb_storage_storefile_free,
            err);

    if(NULL == p->p){
        SpxLog2(log,SpxLogError,*err,\
                "new node for storage storefile pool is fail.");
        goto r1;
    }
    return p;
r1:
    SpxFree(p);
    return NULL;
}

struct ydb_storage_storefile *ydb_storage_storefile_get(\
        struct ydb_storage_storefile_pool *pool,int idx){
    return (struct ydb_storage_storefile *) spx_list_get(pool->p,idx);
}

err_t ydb_storage_storefile_pool_free(struct ydb_storage_storefile_pool **pool){
    spx_list_free(&((*pool)->p));
    SpxFree(pool);
    return 0;
}

