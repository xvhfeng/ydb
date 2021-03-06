/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_dio.c
 *        Created:  2014/08/01 11时16分41秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include "spx_types.h"
#include "spx_alloc.h"
#include "spx_string.h"
#include "spx_list.h"
#include "spx_path.h"
#include "spx_defs.h"
#include "spx_job.h"
#include "spx_task.h"
#include "spx_message.h"
#include "spx_nio.h"
#include "spx_rand.h"
#include "spx_time.h"
#include "spx_io.h"
#include "spx_message.h"
#include "spx_queue.h"
#include "spx_module.h"
#include "spx_network_module.h"
#include "spx_task.h"
#include "spx_atomic.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_dio_context.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_upload.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_dio.h"


spx_private void ydb_storage_dio_do_upload_for_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents);
spx_private void ydb_storage_dio_do_upload_for_singlefile(
        struct ev_loop *loop,ev_async *w,int revents);


spx_private err_t ydb_storage_upload_after(
        struct ydb_storage_dio_context *dc);


err_t ydb_storage_dio_upload(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc){/*{{{*/
    err_t err = 0;
    struct ydb_storage_configurtion *c = dc->jc->config;
    struct spx_msg_header *rqh = dc->jc->reader_header;
    struct spx_job_context *jc = dc->jc;

    //deal suffix
    dc->has_suffix = spx_msg_unpack_bool(dc->jc->reader_body_ctx);
    if(dc->has_suffix){
        dc->suffix = spx_msg_unpack_string(dc->jc->reader_body_ctx,
                YDB_FILENAME_SUFFIX_SIZE,&err);
        if(NULL == dc->suffix){
            SpxLog2(dc->log,SpxLogError,err,\
                    "unpack suffix is fail.");
            goto r1;
        }
    }

    dc->createtime = spx_now();
    dc->lastmodifytime = dc->createtime;
    dc->isdelete = false;
    dc->ver = YDB_VERSION;
    dc->realsize = rqh->bodylen - rqh->offset;
    if(c->overload){
        dc->totalsize = YDB_STORAGE_OVERMODE_ABSSOLUTE == c->overmode
            ? dc->realsize + c->oversize \
            : dc->realsize + (int) (dc->realsize * c->oversize / 100);
    } else {
        dc->totalsize = dc->realsize;
    }

    if(c->chunkfile && (dc->realsize <= c->singlemin)){
        dc->issinglefile = false;
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_dio_do_upload_for_chunkfile,dc);
    } else {//singlefile
        dc->issinglefile = true;
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_dio_do_upload_for_singlefile,dc);
    }
    ev_async_start(loop,&(dc->async));
    ev_async_send(loop,&(dc->async));
    return err;
r1:
    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(jc->log,SpxLogError,err,\
                "dispatch network module is fail,"
                "and push jcontext to pool force.");
        spx_job_pool_push(g_spx_job_pool,jc);
        return err;
    }
    jc->writer_header->protocol = YDB_C2S_UPLOAD;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext_err =
        spx_get_thread(g_spx_network_module,i);
    jc->tc = threadcontext_err;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return err;

}/*}}}*/

spx_private void ydb_storage_dio_do_upload_for_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
    err_t  err = 0;
    ev_async_stop(loop,w);
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_job_context *jc = dc->jc;
    SpxTypeConvert2(struct ydb_storage_configurtion,c,dc->c);

    if(0 != (err = ydb_storage_dio_upload_to_chunkfile(dc))){
        SpxLog2(dc->log,SpxLogError,err,\
                "write context to chunkfile is fail.");
        goto r1;

    }


    if(0 != (err = ydb_storage_upload_after(dc))){
        SpxLog2(dc->log,SpxLogError,err,\
                "make the response for uploading is fail.");
        goto r1;
    }

    YdbStorageBinlogUploadWriter(dc->fid);

    struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints, dc->mp_idx);
    mp->last_modify_time = spx_now();

    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
r1:
    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    if(NULL == jc->writer_header){
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
        if(NULL == jc->writer_header){
            SpxLog2(jc->log,SpxLogError,err,\
                    "dispatch network module is fail,"
                    "and push jcontext to pool force.");
            spx_job_pool_push(g_spx_job_pool,jc);
            return;
        }
    }
    jc->writer_header->protocol = YDB_C2S_UPLOAD;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext_err =
        spx_get_thread(g_spx_network_module,i);
    jc->tc = threadcontext_err;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

spx_private void ydb_storage_dio_do_upload_for_singlefile(
        struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
    ev_async_stop(loop,w);
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_job_context *jc = dc->jc;
    SpxTypeConvert2(struct ydb_storage_configurtion,c,dc->c);
    err_t err = 0;

    if(0 != (err = ydb_storage_dio_upload_to_singlefile(dc))){
        SpxLog2(dc->log,SpxLogError,err,
                "write context to single file is fail.");
        goto r1;
    }

    /*
       YdbStorageBinlog(YDB_BINLOG_ADD,dc->issignalfile,dc->ver,dc->opver,cf->machineid,\
       dc->file_createtime,dc->createtime,dc->lastmodifytime,dc->mp_idx,dc->p1,dc->p2,\
       cf->tidx,dc->rand,dc->begin,dc->totalsize,dc->realsize,dc->suffix);
       */


    if(0 != (err = ydb_storage_upload_after(dc))){
        SpxLog2(dc->log,SpxLogError,err,\
                "make reponse for uploading is fail.");
        goto r1;
    }

    YdbStorageBinlogUploadWriter(dc->fid);

    struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints, dc->mp_idx);
    mp->last_modify_time = spx_now();

    SpxAtomicVIncr(g_ydb_storage_runtime->storecount);

    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
r1:
    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    if(NULL == jc->writer_header){
        jc->writer_header = (struct spx_msg_header *)
            spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
        if(NULL == jc->writer_header){
            SpxLog2(jc->log,SpxLogError,err,\
                    "dispatch network module is fail,"
                    "and push jcontext to pool force.");
            spx_job_pool_push(g_spx_job_pool,jc);
            return;
        }
    }
    jc->writer_header->protocol = YDB_C2S_UPLOAD;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext_err =
        spx_get_thread(g_spx_network_module,i);
    jc->tc = threadcontext_err;
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

err_t ydb_storage_dio_upload_to_chunkfile(
        struct ydb_storage_dio_context *dc){/*{{{*/

    err_t err = 0;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;
    struct ydb_storage_storefile *sf = dc->storefile;
    err = ydb_storage_upload_check_and_open_chunkfile(c,dc,sf);
    if(0 != err){
        SpxLog2(c->log,SpxLogError,err,\
                "check and open chunkfile is fail.");
        return err;
    }

    dc->metadata = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);

    if(NULL == dc->metadata){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc metadata for chunkfile is fail.");
        return err;
    }
    dc->opver ++;//begin with 1 and for sync
    spx_msg_pack_false(dc->metadata);//isdelete
    spx_msg_pack_u32(dc->metadata,dc->opver);
    spx_msg_pack_u32(dc->metadata,dc->ver);
    spx_msg_pack_u64(dc->metadata,dc->createtime);
    spx_msg_pack_u64(dc->metadata,dc->lastmodifytime);
    spx_msg_pack_u64(dc->metadata,dc->totalsize);
    spx_msg_pack_u64(dc->metadata,dc->realsize);
    if(dc->has_suffix){
        spx_msg_pack_fixed_string(dc->metadata,
                dc->suffix,YDB_FILENAME_SUFFIX_SIZE);
    } else {
        spx_msg_align(dc->metadata,YDB_FILENAME_SUFFIX_SIZE);
    }

    if(NULL == dc->hashcode){
        spx_msg_align(dc->metadata,YDB_HASHCODE_SIZE);
    } else {
        spx_msg_pack_fixed_string(dc->metadata,
                dc->hashcode,YDB_HASHCODE_SIZE);
    }

    size_t off = sf->chunkfile.offset;
    size_t recvbytes = 0;
    size_t writebytes = 0;
    size_t len = 0;

    memcpy(sf->chunkfile.mptr + off,
            dc->metadata->buf,YDB_CHUNKFILE_MEMADATA_SIZE);
    off += YDB_CHUNKFILE_MEMADATA_SIZE;
    if(jc->is_lazy_recv){
        if(NULL == dc->buf){
            dc->buf = spx_alloc_alone(YdbBufferSizeForLazyRecv,&err);
            if(NULL == dc->buf){
                SpxLogFmt2(dc->log,SpxLogError,err,
                        "new lazy-recv buffer is fail."
                        "size:%lld.",YdbBufferSizeForLazyRecv);
                return err;
            }
        }

        while(true){
            recvbytes = dc->realsize - writebytes > YdbBufferSizeForLazyRecv \
                        ? YdbBufferSizeForLazyRecv \
                        : dc->realsize - writebytes;
            err = spx_read_nb(jc->fd,(byte_t *) dc->buf,recvbytes,&len);
            if(0 != err || recvbytes != len){
                SpxLogFmt2(dc->log,SpxLogError,err,
                        "lazy read buffer and cp to mmap is fail."
                        "recvbytes:%lld,real recvbytes:%lld."
                        "writedbytes:%lld,total size:%lld.",
                        recvbytes,len,writebytes,dc->realsize);
                return err;
            }

            memcpy(sf->chunkfile.mptr + off,dc->buf,len);
            off += len;
            writebytes += len;
            if(writebytes >= dc->realsize) {
                break;
            }
        }
    } else {
        memcpy(sf->chunkfile.mptr + off,
                jc->reader_body_ctx->buf + jc->reader_header->offset,
                jc->reader_header->bodylen - jc->reader_header->offset);
        off += jc->reader_header->bodylen - jc->reader_header->offset;
    }

    dc->begin = sf->chunkfile.offset;
    sf->chunkfile.offset += dc->totalsize;
    dc->rand = sf->chunkfile.rand;
    dc->tidx = sf->tidx;
    dc->p1 = sf->chunkfile.p1;
    dc->p2 = sf->chunkfile.p2;
    dc->mp_idx = sf->chunkfile.mpidx;
    dc->file_createtime = sf->chunkfile.fcreatetime;

    return err;
}/*}}}*/

err_t ydb_storage_dio_upload_to_singlefile(
        struct ydb_storage_dio_context *dc){/*{{{*/
    err_t err = 0;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;
    struct ydb_storage_storefile *sf = dc->storefile;

    sf->singlefile.fcreatetime = dc->lastmodifytime;

    int count = SpxAtomicLazyIncr(&(g_ydb_storage_runtime->singlefile_count));
    SpxAtomicCas(&(g_ydb_storage_runtime->singlefile_count),10000,0);
    sf->singlefile.rand = count;

    ydb_storage_dio_get_path(c,g_ydb_storage_runtime,&(sf->singlefile.mpidx),\
            &(sf->singlefile.p1),&(sf->singlefile.p2));

    sf->singlefile.filename = ydb_storage_dio_make_filename(\
            dc->log,dc->issinglefile,
            c->mountpoints,g_ydb_storage_runtime->mpidx,\
            sf->singlefile.p1,sf->singlefile.p2,
            c->machineid,sf->tidx,
            sf->singlefile.fcreatetime,sf->singlefile.rand,\
            dc->suffix,&err);
    if(SpxStringIsNullOrEmpty(sf->singlefile.filename)){
        SpxLog2(c->log,SpxLogError,err,
                "alloc singlefile name is fail.");
        goto r1;
    }

    sf->singlefile.fd = open(sf->singlefile.filename,\
            O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
    if(0 == sf->singlefile.fd){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,\
                "open singlefile is fail.",
                sf->singlefile.filename);
        goto r1;
    }
    if(0 != (err = ftruncate(sf->singlefile.fd,dc->realsize))){
        SpxLogFmt2(c->log,SpxLogError,err,\
                "truncate singlefile:%s to size:%lld is fail.",
                sf->singlefile.filename,dc->realsize);
        goto r1;
    }
    sf->singlefile.mptr = mmap(NULL,\
            c->chunksize,PROT_READ | PROT_WRITE ,\
            MAP_SHARED,sf->singlefile.fd,0);
    if(MAP_FAILED == sf->singlefile.mptr){
        SpxLogFmt2(c->log,SpxLogError,err,\
                "mmap the single file:%s to memory is fail.",
                sf->singlefile.filename);
        goto r1;
    }

    size_t recvbytes = 0;
    size_t writebytes = 0;
    size_t len = 0;
    if(jc->is_lazy_recv){
        dc->buf = spx_alloc_alone(YdbBufferSizeForLazyRecv,&err);
        if(NULL == dc->buf){
            SpxLogFmt2(dc->log,SpxLogError,err,
                    "new lazy-recv buffer is fail."
                    "size:%lld.",YdbBufferSizeForLazyRecv);
            goto r1;
        }

        while(true){
            recvbytes = dc->realsize - writebytes > YdbBufferSizeForLazyRecv \
                        ? YdbBufferSizeForLazyRecv \
                        : dc->realsize - writebytes;
            err = spx_read_nb(jc->fd,(byte_t *) dc->buf,recvbytes,&len);
            if(0 != err || recvbytes != len){
                SpxLogFmt2(dc->log,SpxLogError,err,\
                        "lazy read buffer and cp to mmap is fail."
                        "recvbytes:%lld,real recvbytes:%lld."
                        "writedbytes:%lld,total size:%lld.",
                        recvbytes,len,writebytes,dc->realsize);
                goto r1;
            }

            memcpy(sf->singlefile.mptr + writebytes,dc->buf,len);
            writebytes += len;
            if(writebytes >= dc->realsize) {
                break;
            }
        }
    } else {
        memcpy(sf->singlefile.mptr,
                jc->reader_body_ctx->buf + jc->reader_header->offset,
                jc->reader_header->bodylen - jc->reader_header->offset);
    }

    if(0 != (err = spx_modify_filetime(
                    sf->singlefile.filename,
                    sf->singlefile.fcreatetime))){
        SpxLogFmt2(c->log,SpxLogError,err,
                "modify file:%s creatime is fail.",
                sf->singlefile.filename);
        goto r1;
    }

    dc->begin = 0;
    dc->rand = sf->singlefile.rand;
    dc->tidx = sf->tidx;
    dc->p1 = sf->singlefile.p1;
    dc->p2 = sf->singlefile.p2;
    dc->mp_idx = sf->singlefile.mpidx;
    dc->file_createtime = sf->singlefile.fcreatetime;
    dc->totalsize = dc->realsize;

r1:
    SpxClose(sf->singlefile.fd);
    if(NULL != sf->singlefile.mptr) {
        munmap(sf->singlefile.mptr,dc->realsize);
    }
    SpxStringFree(sf->singlefile.filename);

    return err;
}/*}}}*/

spx_private err_t ydb_storage_upload_after(
        struct ydb_storage_dio_context *dc){/*{{{*/
    err_t err = 0;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;
    struct ydb_storage_storefile *cf = dc->storefile;

    size_t len = 0;

    dc->fid =  ydb_storage_dio_make_fileid(c->log,
            c->groupname,c->machineid,c->syncgroup,
            dc->issinglefile,dc->mp_idx,dc->p1,dc->p2,
            cf->tidx,dc->file_createtime,dc->rand,
            dc->begin,dc->realsize,dc->totalsize,
            dc->ver,dc->opver,dc->lastmodifytime,
            dc->hashcode,dc->has_suffix,dc->suffix,
            &len,&err);

    if(NULL == dc->fid){
        SpxLog2(dc->log,SpxLogError,err,\
                "new file is fail.");
        return err;
    }
    struct spx_msg * ctx = spx_msg_new(len,&err);
    if(NULL == ctx){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "new response body ctx is fail.the fid:%s.",
                dc->fid);
        return err;
    }

    jc->writer_body_ctx = ctx;
    spx_msg_pack_fixed_string(ctx,dc->fid,len);

    struct spx_msg_header *h = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*h),&err);
    if(NULL == h){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail.");
        return err;
    }
    jc->writer_header = h;
    h->protocol = YDB_C2S_UPLOAD;
    h->bodylen = len;
    h->version = YDB_VERSION;
    h->offset = len;
    jc->is_sendfile = false;
    return err;
}/*}}}*/

