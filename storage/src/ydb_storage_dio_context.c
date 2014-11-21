/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_dio_context.c
 *        Created:  2014/08/07 13时45分57秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <ev.h>

#include "spx_types.h"
#include "spx_job.h"
#include "spx_task.h"
#include "spx_fixed_vector.h"
#include "spx_defs.h"
#include "spx_alloc.h"
#include "spx_string.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_dio_context.h"

struct ydb_storage_dio_pool *g_ydb_storage_dio_pool = NULL;

struct ydb_storage_dio_context_transport{
    SpxLogDelegate *log;
    struct ydb_storage_configurtion *c;
};

spx_private  void *ydb_storage_dio_context_new(size_t idx,void *arg,err_t *err);
spx_private err_t ydb_storage_dio_context_free(void **arg);


spx_private  void *ydb_storage_dio_context_new(size_t idx,void *arg,err_t *err){
    struct ydb_storage_dio_context_transport *dct = (struct ydb_storage_dio_context_transport *) arg;
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *) \
                                         spx_alloc_alone(sizeof(*dc),err);
    if(NULL == dc){
        SpxLog2(dct->log,SpxLogError,*err,\
                "new storage dio context.");
        return NULL;
    }
    dc->log = dct->log;
    dc->idx = idx;
    dc->c = dct->c;
    return dc;
}

spx_private err_t ydb_storage_dio_context_free(void **arg){
    struct ydb_storage_dio_context **dc = (struct ydb_storage_dio_context **) arg;
    (*dc)->jc = NULL;
    (*dc)->tc = NULL;
    SpxFree(*dc);
    return 0;
}

struct ydb_storage_dio_pool *ydb_storage_dio_pool_new(SpxLogDelegate *log,\
        struct ydb_storage_configurtion *c,
        size_t size,
        err_t *err){
    struct ydb_storage_dio_pool *p = (struct ydb_storage_dio_pool *) \
                                     spx_alloc_alone(sizeof(*p),err);
    if(NULL == p){
        SpxLog2(log,SpxLogError,*err,\
                "new dio context pool is fail.");
        return NULL;
    }
    struct ydb_storage_dio_context_transport dct;
    SpxZero(dct);
    dct.log = log;
    dct.c = c;
    p->log = log;
    p->pool = spx_fixed_vector_new(log,size,\
            ydb_storage_dio_context_new,\
            &dct,
            ydb_storage_dio_context_free,\
            err);
    if(NULL == p->pool){
        SpxLog2(log,SpxLogError,*err,\
                "new node for dio context pool is fail.");
        goto r1;
    }
    return p;
r1:
    SpxFree(p);
    return NULL;


}

struct ydb_storage_dio_context *ydb_storage_dio_pool_pop(\
        struct ydb_storage_dio_pool *pool,\
        err_t *err){
    return (struct ydb_storage_dio_context *) spx_fixed_vector_pop(pool->pool,err);
}

err_t ydb_storage_dio_pool_push(\
        struct ydb_storage_dio_pool *pool,\
        struct ydb_storage_dio_context *dc){
    dc->ver = 0;
    dc->opver = 0;
    dc->err = 0;

    dc->isdelete = false;
    dc->createtime = 0;
    dc->lastmodifytime = 0;
    if(NULL != dc->hashcode){
        SpxStringFree(dc->hashcode);
    }
    dc->totalsize = 0;
    dc->realsize = 0;

    dc->file_createtime = 0;
    dc->mp_idx = 0;
    dc->p1 = 0;
    dc->p2 = 0;
    dc->tidx = 0;
    dc->rand = 0;
    dc->issinglefile = false;
    dc->begin = 0;
    dc->has_suffix = false;
    if(NULL != dc->suffix){
        SpxStringFree(dc->suffix);
    }
    if(NULL != dc->groupname){
        SpxStringFree(dc->groupname);
    }
    if(NULL != dc->machineid){
        SpxStringFree(dc->machineid);
    }
    if(NULL != dc->syncgroup){
        SpxStringFree(dc->syncgroup);
    }
    if(NULL != dc->date){
        SpxFree(dc->date);
    }

    dc->jc = NULL;
    dc->tc = NULL;
    dc->storefile = NULL;

    if(NULL != dc->buf){
        SpxStringFree(dc->buf);
    }
    if(NULL != dc->filename){
        SpxStringFree(dc->filename);
    }
    if(NULL != dc->rfid){
        SpxStringFree(dc->rfid);
    }
    if(NULL != dc->fid){
        SpxStringFree(dc->fid);
    }
    return spx_fixed_vector_push(pool->pool,dc);
}

err_t ydb_storage_dio_pool_free(struct ydb_storage_dio_pool **pool){
    spx_fixed_vector_free(&((*pool)->pool));
    SpxFree(*pool);
    return 0;
}

