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

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_dio_context.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_upload.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_dio.h"

spx_private err_t ydb_storage_dio_get_path(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,\
        u8_t *mpdix,u8_t *pq,u8_t *p2);
spx_private u8_t ydb_storage_get_mountpoint_with_loop(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,bool_t isfirst,err_t *err);
spx_private u8_t ydb_storage_get_mountpoint_with_turn(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err);
spx_private u8_t ydb_storage_get_mountpoint_with_maxsize(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err);
spx_private u8_t ydb_storage_get_mountpoint_with_master(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err);

spx_private void ydb_storage_dio_do_upload_for_chunkfile(struct ev_loop *loop,ev_async *w,int revents);
spx_private void ydb_storage_dio_do_upload_for_singlefile(struct ev_loop *loop,ev_async *w,int revents);

spx_private err_t ydb_storage_upload_check_and_open_chunkfile(struct ydb_storage_configurtion *c,\
        struct ydb_storage_dio_context *dc,\
        struct ydb_storage_storefile *cf);

spx_private err_t ydb_storage_upload_after(struct ydb_storage_dio_context *dc);


err_t ydb_storage_dio_upload(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc){/*{{{*/
    err_t err = 0;
    struct ydb_storage_configurtion *c = dc->jc->config;
    struct spx_msg_header *rqh = dc->jc->reader_header;

    //deal suffix
    dc->has_suffix = spx_msg_unpack_bool(dc->jc->reader_body_ctx);
    if(dc->has_suffix){
        dc->suffix = spx_msg_unpack_string(dc->jc->reader_body_ctx,YDB_FILENAME_SUFFIX_SIZE,&err);
        if(NULL == dc->suffix){
            SpxLog2(dc->log,SpxLogError,err,\
                    "unpack suffix is fail.");
            return err;
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
            : dc->realsize + dc->realsize * c->oversize / 100;
    } else {
        dc->totalsize = dc->realsize;
    }

    if(c->chunkfile && (dc->realsize <= c->singlemin)){
        dc->issignalfile = false;
        spx_dio_regedit_async(&(dc->async),ydb_storage_dio_do_upload_for_chunkfile,dc);
    } else {//singlefile
        dc->issignalfile = true;
        spx_dio_regedit_async(&(dc->async),ydb_storage_dio_do_upload_for_singlefile,dc);
    }
    ev_async_start(loop,&(dc->async));
    ev_async_send(loop,&(dc->async));
    return err;
}/*}}}*/

spx_private void ydb_storage_dio_do_upload_for_chunkfile(struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
    err_t  err = 0;
    ev_async_stop(loop,w);
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *) w->data;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;
    struct ydb_storage_storefile *cf = dc->storefile;

    err = ydb_storage_upload_check_and_open_chunkfile(c,dc,cf);
    if(0 != err){
        SpxLog2(c->log,SpxLogError,err,\
                "check and open chunkfile is fail.");
        goto r1;
    }

//    dc->createtime = spx_now();
//    dc->issignalfile = false;
//    dc->lastmodifytime = dc->createtime;
//    dc->isdelete = false;
//    dc->ver = YDB_VERSION;
    dc->metadata = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);

    if(NULL == dc->metadata){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc metadata for chunkfile is fail.");
        goto r1;
    }

    spx_msg_pack_false(dc->metadata);//isdelete
    spx_msg_pack_u32(dc->metadata,dc->opver);
    spx_msg_pack_u32(dc->metadata,dc->ver);
    spx_msg_pack_u64(dc->metadata,dc->createtime);
    spx_msg_pack_u64(dc->metadata,dc->lastmodifytime);
    spx_msg_pack_u64(dc->metadata,dc->totalsize);
    spx_msg_pack_u64(dc->metadata,dc->realsize);
    if(dc->has_suffix){
        spx_msg_pack_fixed_string(dc->metadata,dc->suffix,YDB_FILENAME_SUFFIX_SIZE);
    } else {
        spx_msg_align(dc->metadata,YDB_FILENAME_SUFFIX_SIZE);
    }

    if(NULL == dc->hashcode){
        spx_msg_align(dc->metadata,YDB_HASHCODE_SIZE);
    } else {
        spx_msg_pack_fixed_string(dc->metadata,dc->hashcode,YDB_HASHCODE_SIZE);
    }

    size_t off = cf->chunkfile.offset;
    size_t recvbytes = 0;
    size_t writebytes = 0;
    size_t len = 0;

    off += spx_mmap_form_msg(cf->chunkfile.mptr,off,dc->metadata);
    if(jc->is_lazy_recv){
        do{
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

            memcpy(cf->chunkfile.mptr + off,dc->buf,len);
            off += len;
            writebytes += len;
            if(writebytes < dc->realsize) {
                continue;
            }
        }while(false);
    } else {
        memcpy(cf->chunkfile.mptr + off,
                jc->reader_body_ctx->buf + jc->reader_header->offset,
                jc->reader_header->bodylen - jc->reader_header->offset);
        off += jc->reader_header->bodylen - jc->reader_header->offset;
    }

    dc->begin = cf->chunkfile.offset;
    cf->chunkfile.offset += dc->totalsize;
    dc->rand = cf->chunkfile.rand;
    dc->tidx = cf->tidx;
    dc->p1 = cf->chunkfile.p1;
    dc->p2 = cf->chunkfile.p2;
    dc->mp_idx = cf->chunkfile.mpidx;
    dc->file_createtime = cf->chunkfile.fcreatetime;

    /*
    YdbStorageBinlog(YDB_BINLOG_ADD,dc->issignalfile,dc->ver,dc->opver,cf->machineid,\
            dc->file_createtime,dc->createtime,dc->lastmodifytime,dc->mp_idx,dc->p1,dc->p2,\
            cf->tidx,dc->rand,dc->begin,dc->totalsize,dc->realsize,dc->suffix);
*/


    if(0 != (err = ydb_storage_upload_after(dc))){
        SpxLog2(dc->log,SpxLogError,err,\
                "make the response for uploading is fail.");
        goto r1;
    }

    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    err = spx_module_dispatch(threadcontext,spx_network_module_wakeup_handler,jc);
    return;
r1:

    jc->writer_header = (struct spx_msg_header *) spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail.");
    }
    jc->writer_header->protocol = YDB_STORAGE_UPLOAD;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext_err = spx_get_thread(g_spx_network_module,i);
    jc->tc = threadcontext_err;
    err = spx_module_dispatch(threadcontext_err,spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

spx_private void ydb_storage_dio_do_upload_for_singlefile(struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
    ev_async_stop(loop,w);
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *) w->data;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;
    struct ydb_storage_storefile *cf = dc->storefile;
    err_t err = 0;

    cf->singlefile.fcreatetime = dc->createtime;
    cf->singlefile.rand = spx_random(cf->tidx);

    ydb_storage_dio_get_path(c,g_ydb_storage_runtime,&(cf->singlefile.mpidx),\
            &(cf->singlefile.p1),&(cf->singlefile.p2));

    cf->singlefile.filename = ydb_storage_dio_make_filename(\
            dc->log,c->mountpoints,g_ydb_storage_runtime->mpidx,\
            cf->singlefile.p1,cf->singlefile.p2,
            c->machineid,cf->tidx,cf->singlefile.fcreatetime,cf->singlefile.rand,\
            dc->suffix,&err);
    if(SpxStringIsNullOrEmpty(cf->singlefile.filename)){
        SpxLog2(c->log,SpxLogError,err,
                "alloc singlefile name is fail.");
        goto r1;
    }

    cf->singlefile.fd = open(cf->singlefile.filename,\
            O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
    if(0 == cf->singlefile.fd){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,\
                "open singlefile is fail.",
                cf->singlefile.filename);
        goto r1;
    }
    if(0 != (err = ftruncate(cf->singlefile.fd,dc->realsize))){
        SpxLogFmt2(c->log,SpxLogError,err,\
                "truncate singlefile:%s to size:%lld is fail.",
                cf->singlefile.filename,dc->realsize);
        goto r1;
    }
    cf->singlefile.mptr = mmap(NULL,\
            c->chunksize,PROT_READ | PROT_WRITE ,\
            MAP_SHARED,cf->singlefile.fd,0);
    if(MAP_FAILED == cf->singlefile.mptr){
        SpxLogFmt2(c->log,SpxLogError,err,\
                "mmap the single file:%s to memory is fail.",
                cf->singlefile.filename);
        goto r1;
    }

    size_t recvbytes = 0;
    size_t writebytes = 0;
    size_t len = 0;
    if(jc->is_lazy_recv){
        do{
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

            memcpy(cf->singlefile.mptr + writebytes,dc->buf,len);
            writebytes += len;
            if(writebytes < dc->realsize) {
                continue;
            }
        }while(false);
    } else {
        memcpy(cf->singlefile.mptr,
                jc->reader_body_ctx->buf + jc->reader_header->offset,
                jc->reader_header->bodylen - jc->reader_header->offset);
//        off += jc->reader_header->bodylen - jc->reader_header->offset;
//        spx_mmap_form_msg(cf->chunkfile.mptr,0,jc->reader_body_ctx);
    }

    spx_string_free(cf->singlefile.filename);
    SpxClose(cf->singlefile.fd);
    if(NULL != cf->singlefile.mptr) {
        munmap(cf->singlefile.mptr,dc->realsize);
    }

    dc->begin = 0;
    dc->rand = cf->singlefile.rand;
    dc->tidx = cf->tidx;
    dc->p1 = cf->singlefile.p1;
    dc->p2 = cf->singlefile.p2;
    dc->mp_idx = cf->singlefile.mpidx;
    dc->file_createtime = cf->singlefile.fcreatetime;

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

    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    err = spx_module_dispatch(threadcontext,spx_network_module_wakeup_handler,jc);
        return;
r1:
    spx_string_free(cf->singlefile.filename);
    SpxClose(cf->singlefile.fd);
    if(NULL != cf->singlefile.mptr) {
        munmap(cf->singlefile.mptr,dc->realsize);
    }

    jc->writer_header = (struct spx_msg_header *) spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail.");
    }
    jc->writer_header->protocol = YDB_STORAGE_UPLOAD;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext_err = spx_get_thread(g_spx_network_module,i);
    jc->tc = threadcontext_err;
    err = spx_module_dispatch(threadcontext_err,spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/


spx_private err_t ydb_storage_upload_after(struct ydb_storage_dio_context *dc){/*{{{*/
    err_t err = 0;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;
    struct ydb_storage_storefile *cf = dc->storefile;

    size_t len = 0;
    string_t fid =  ydb_storage_dio_make_fileid(c,dc,cf,&len,&err);
    if(NULL == fid){
        SpxLog2(dc->log,SpxLogError,err,\
                "new file is fail.");
        return err;
    }
    struct spx_msg * ctx = spx_msg_new(len,&err);
    if(NULL == ctx){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "new response body ctx is fail.the fid:%s.",
                fid);
        goto r1;
    }

    jc->writer_body_ctx = ctx;
    spx_msg_pack_fixed_string(ctx,fid,len);

    struct spx_msg_header *h = (struct spx_msg_header *) spx_alloc_alone(sizeof(*h),&err);
    if(NULL == h){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail.");
        goto r1;
    }
    jc->writer_header = h;
    h->protocol = YDB_STORAGE_UPLOAD;
    h->bodylen = len;
    h->version = YDB_VERSION;
    h->offset = len;
    jc->is_sendfile = false;
r1:
    spx_string_free(fid);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_upload_check_and_open_chunkfile(struct ydb_storage_configurtion *c,\
        struct ydb_storage_dio_context *dc,\
        struct ydb_storage_storefile *cf){/*{{{*/
    err_t err = 0;
    while(true) {
        if(0 == cf->chunkfile.fd){
            cf->chunkfile.fcreatetime = dc->createtime;
            cf->chunkfile.rand = spx_random(cf->tidx);

            ydb_storage_dio_get_path(c,g_ydb_storage_runtime,\
                    &(cf->chunkfile.mpidx),\
                    &(cf->chunkfile.p1),&(cf->chunkfile.p2));

            cf->chunkfile.filename = ydb_storage_dio_make_filename(\
                    dc->log,c->mountpoints,g_ydb_storage_runtime->mpidx,\
                    cf->chunkfile.p1,cf->chunkfile.p2,
                    c->machineid,cf->tidx,cf->chunkfile.fcreatetime,cf->chunkfile.rand,\
                    dc->suffix,&err);
            SpxLogFmt1(c->log,SpxLogInfo,"reopen :%s.",cf->chunkfile.filename);

            if(SpxStringIsNullOrEmpty(cf->chunkfile.filename)){
                SpxLog2(c->log,SpxLogError,err,
                        "alloc chunkfile name is fail.");
                break;
            }

            cf->chunkfile.fd = open(cf->chunkfile.filename,\
                    O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
            if(0 >= cf->chunkfile.fd){
                err = errno;
                SpxLogFmt2(c->log,SpxLogError,err,\
                        "open chunkfile is fail.",
                        cf->chunkfile.filename);
                spx_string_free(cf->chunkfile.filename);
                break;
            }
            if(0 != (err = ftruncate(cf->chunkfile.fd,c->chunksize))){
                SpxLogFmt2(c->log,SpxLogError,err,\
                        "truncate chunkfile:%s to size:%lld is fail.",
                        cf->chunkfile.filename,c->chunksize);
                spx_string_free(cf->chunkfile.filename);
                SpxClose(cf->chunkfile.fd);
                break;
            }
            cf->chunkfile.mptr = mmap(NULL,\
                    c->chunksize,PROT_READ | PROT_WRITE ,\
                    MAP_SHARED,cf->chunkfile.fd,0);
            if(MAP_FAILED == cf->chunkfile.mptr){
                err = errno;
                SpxLogFmt2(c->log,SpxLogError,err,\
                        "mmap the chunkfile file:%s to memory is fail.",
                        cf->chunkfile.filename);
                spx_string_free(cf->chunkfile.filename);
                SpxClose(cf->chunkfile.fd);
                break;
            }
        }

        if(dc->totalsize > c->chunksize - cf->chunkfile.offset){
            SpxLogFmt1(c->log,SpxLogInfo, "total:%lld,chunksize:%lld,offset:%lld.",dc->totalsize,
                    c->chunksize,cf->chunkfile.offset);
            SpxLogFmt1(c->log,SpxLogInfo,"close the file %s",cf->chunkfile.filename);
            munmap(cf->chunkfile.mptr,c->chunksize);
            cf->chunkfile.mptr = NULL;
            SpxClose(cf->chunkfile.fd);
            spx_string_free(cf->chunkfile.filename);
            cf->chunkfile.offset = 0;
            continue;//reopen a new file
        }
        break;
    }
    SpxLog1(c->log,SpxLogInfo,"return");
    return err;
}/*}}}*/

spx_private err_t ydb_storage_dio_get_path(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,\
        u8_t *mpidx,u8_t *p1,u8_t *p2){/*{{{*/
    err_t err = 0;
    if(rt->storecount <= c->storecount){//input the current store
        return 0;
    }

    //ready,go
    u32_t total = 0;
    u8_t tp1 = rt->p1;
    u8_t tp2 = rt->p2;
    u8_t flag = 0;

    //now begin magic
    //if you no understand this ways,please donot modify it
    //bit alg
    total = tp1 << 8 | tp2;
    total++;
    flag = total >> 16 & 0xFF;
    tp1 = total >> 8 & 0xFF;
    tp2 = total & 0xFF;

    if(c->storerooms == tp2){
        tp1++;
        tp2 = 0;
    }
    if(c->storerooms == tp1){
        flag = 1;
        tp1 = 0;
    }

    *p1 = tp1;
    *p2 = tp2;

    if(1 == flag){/*{{{*/
        switch(c->balance){
            case YDB_STORAGE_MOUNTPOINT_TURN:
                {
                    *mpidx = ydb_storage_get_mountpoint_with_turn(c,rt,&err);
                    if(0 != err){
                        return err;
                    }
                    break;
                }
            case YDB_STORAGE_MOUNTPOINT_MAXSIZE:
                {
                    *mpidx = ydb_storage_get_mountpoint_with_maxsize(c,rt,&err);
                    if(0 != err){
                        return err;
                    }
                    break;
                }
            case YDB_STORAGE_MOUNTPOINT_MASTER:
                {
                    *mpidx = ydb_storage_get_mountpoint_with_master(c,rt,&err);
                    if(0 != err){
                        return err;
                    }
                    break;
                }
            case YDB_STORAGE_MOUNTPOINT_LOOP:
            default:
                {
                    *mpidx = ydb_storage_get_mountpoint_with_loop(c,rt,true,&err);
                    if(0 != err){
                        return err;
                    }
                    break;
                }
        }
    }/*}}}*/
    return err;
}/*}}}*/

spx_private u8_t ydb_storage_get_mountpoint_with_loop(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,bool_t isfirst,err_t *err){/*{{{*/
    u8_t mpidx = isfirst ? rt->mpidx : rt->mpidx + 1;
    struct ydb_storage_mountpoint *mp = NULL;
    int i = 0;
    for( ; i < YDB_STORAGE_MOUNTPOINT_MAXSIZE; i++){
        mp = spx_list_get(c->mountpoints,mpidx);
        if(NULL == mp || SpxStringIsNullOrEmpty(mp->path)){
            mpidx = (mpidx + 1) % YDB_STORAGE_MOUNTPOINT_MAXSIZE;
            continue;
        }
        u64_t size = spx_mountpoint_freesize(mp->path,err);
        if(size <= c->freedisk){
            mpidx = (mpidx + 1) % YDB_STORAGE_MOUNTPOINT_MAXSIZE;
            continue;
        }else{
            return mpidx;
        }
    }
    *err = ENOENT;
    SpxLog1(c->log,SpxLogError,\
            "no the mountpoint can use.");
    return 0;
}/*}}}*/

spx_private u8_t ydb_storage_get_mountpoint_with_turn(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err){/*{{{*/
    u8_t mpidx = rt->mpidx ;
    struct ydb_storage_mountpoint *mp = NULL;
    int i = 0;
    for( ; i < YDB_STORAGE_MOUNTPOINT_MAXSIZE; i++){
        mp = spx_list_get(c->mountpoints,mpidx);
        if(NULL == mp || SpxStringIsNullOrEmpty(mp->path)){
            mpidx = (mpidx + 1) % YDB_STORAGE_MOUNTPOINT_MAXSIZE;
            continue;
        }
        u64_t size = spx_mountpoint_freesize(mp->path,err);
        if(size <= c->freedisk){
            mpidx = (mpidx + 1) % YDB_STORAGE_MOUNTPOINT_MAXSIZE;
            continue;
        }else{
            return mpidx;
        }
    }
    *err = ENOENT;
    SpxLog1(c->log,SpxLogError,\
            "no the mountpoint can use.");
    return 0;
}/*}}}*/

spx_private u8_t ydb_storage_get_mountpoint_with_maxsize(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err){/*{{{*/
    u8_t mpidx = 0;
    u64_t max = 0;
    bool_t exist = false;
    struct ydb_storage_mountpoint *mp = NULL;
    int i = 0;
    for( ; i < YDB_STORAGE_MOUNTPOINT_MAXSIZE; i++){
        mp = spx_list_get(c->mountpoints,i);
        if(NULL == mp || SpxStringIsNullOrEmpty(mp->path)){
            continue;
        }
        u64_t size = spx_mountpoint_freesize(mp->path,err);
        if(size - c->freedisk > max){
            exist = true;
            max = size - c->freedisk;
            mpidx = i;
        }
    }
    if(!exist){
        *err = ENOENT;
        SpxLog1(c->log,SpxLogError,\
                "no the mountpoint can use.");
    }
    return mpidx;
}/*}}}*/

spx_private u8_t ydb_storage_get_mountpoint_with_master(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err){/*{{{*/
    struct ydb_storage_mountpoint *mp = NULL;
    mp = spx_list_get(c->mountpoints,c->master);
    if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)
            && c->freedisk < spx_mountpoint_freesize(mp->path,err)){
        return c->master;
    }
    return ydb_storage_get_mountpoint_with_loop(c,rt,false,err);
}/*}}}*/


