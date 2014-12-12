/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_delete.c
 *        Created:  2014/08/14 17时14分59秒
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
#include "ydb_storage_binlog.h"
#include "ydb_storage_dio_context.h"
#include "ydb_storage_delete.h"
#include "ydb_storage_dio.h"



err_t ydb_storage_dio_delete(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc){/*{{{*/
    err_t err = 0;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    struct spx_msg *ctx = jc->reader_body_ctx;
    size_t len = jc->reader_header->bodylen;

    dc->rfid = spx_msg_unpack_string(ctx,len,&(err));
    if(NULL == dc->rfid){
        SpxLog2(c->log,SpxLogError,err,\
                "alloc file id for parser is fail.");
        goto r1;
    }

    if(0 != ( err = ydb_storage_dio_parser_fileid(jc->log,dc->rfid,
        &(dc->groupname),&(dc->machineid),&(dc->syncgroup),
        &(dc->issinglefile),&(dc->mp_idx),&(dc->p1),
        &(dc->p2),&(dc->tidx),&(dc->file_createtime),
        &(dc->rand),&(dc->begin),&(dc->realsize),
        &(dc->totalsize),&(dc->ver),&(dc->opver),
        &(dc->lastmodifytime),&(dc->hashcode),
        &(dc->has_suffix),&(dc->suffix)))){

        SpxLog2(dc->log,SpxLogError,err,\
                "parser fid is fail.");
        goto r1;
    }

    dc->filename = ydb_storage_dio_make_filename(dc->log,dc->issinglefile,
            c->mountpoints,
            dc->mp_idx,
            dc->p1,dc->p2,
            dc->machineid,dc->tidx,dc->file_createtime,
            dc->rand,dc->suffix,&err);
    if(NULL == dc->filename){
        SpxLog2(dc->log,SpxLogError,err,\
                "make filename is fail.");
        goto r1;
    }

    if(!SpxFileExist(dc->filename)) {
        SpxLogFmt1(dc->log,SpxLogWarn,\
                "deleting-file:%s is not exist.",
                dc->filename);
        goto r1;
    }

    if(dc->issinglefile){
        if(0 != remove(dc->filename)){
            err = errno;
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "delete file :%s is fail.",
                    dc->filename);
        }

        YdbStorageBinlogDeleteWriter(dc->rfid);
        goto r1;
    } else {
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_dio_do_delete_form_chunkfile,dc);
        ev_async_start(loop,&(dc->async));
        ev_async_send(loop,&(dc->async));
    }
    return err;
r1:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail."
                "no notify client and push jc force.");
        spx_job_pool_push(g_spx_job_pool,jc);
        return err;
    }
    jc->writer_header->protocol = jc->reader_header->protocol;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
//    err = spx_module_dispatch(threadcontext,
//            spx_network_module_wakeup_handler,jc);
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);

    if(0 != err){
        SpxLog2(jc->log,SpxLogError,err,\
                "dispatch network module is fail,"
                "and push jcontext to pool force.");
        spx_job_pool_push(g_spx_job_pool,jc);
    }
    return 0;
}/*}}}*/

void ydb_storage_dio_do_delete_form_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
    ev_async_stop(loop,w);
    err_t err = 0;
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *)
        w->data;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    if(0 != (err =  ydb_storage_dio_delete_context_from_chunkfile(
                    c,dc->filename,dc->begin,dc->totalsize,
                    dc->opver,dc->ver,dc->lastmodifytime,
                    dc->realsize,spx_now()))){
        SpxLog2(dc->log,SpxLogError,err,
                "delete context form chunkfile is fail.");
        goto r1;
    }

    YdbStorageBinlogDeleteWriter(dc->rfid);
    struct spx_msg_header *wh = (struct spx_msg_header *) \
                                spx_alloc_alone(sizeof(*wh),&err);
    if(NULL == wh){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc write header for delete buffer in chunkfile is fail.");
        goto r1;
    }

    jc->writer_header = wh;
    wh->version = YDB_VERSION;
    wh->protocol = YDB_C2S_DELETE;
    wh->offset = 0;
    wh->bodylen = 0;
    goto r2;
r1:
    jc->writer_header = (struct spx_msg_header *)
        spx_alloc_alone(sizeof(*(jc->writer_header)),&err);
    if(NULL == jc->writer_header){
        SpxLog2(dc->log,SpxLogError,err,\
                "new response header is fail."
                "no notify client and push jc force.");
        spx_task_pool_push(g_spx_task_pool,tc);
        ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
        spx_job_pool_push(g_spx_job_pool,jc);
        return;
    }
    jc->writer_header->protocol = jc->reader_header->protocol;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;
r2:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext =
        spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
//    err = spx_module_dispatch(threadcontext,
//            spx_network_module_wakeup_handler,jc);
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    if(0 != err){
        SpxLog2(dc->log,SpxLogError,err,\
                "notify network module is fail.");
        spx_job_pool_push(g_spx_job_pool,jc);
        return;
    }
    return;
}/*}}}*/


err_t ydb_storage_dio_delete_context_from_chunkfile(
        struct ydb_storage_configurtion *c,
        string_t fname,
        u64_t cbegin,
        u64_t ctotalsize,
        u32_t copver,
        u32_t cver,
        u64_t clastmodifytime,
        u64_t crealsize,
        u64_t lastmodifytime){/*{{{*/
    err_t err = 0;
    struct spx_msg *ioctx = NULL;
    int fd = 0;
    char *mptr = NULL;
    string_t io_suffix = NULL;
    string_t io_hashcode = NULL;

    bool_t io_isdelete = false;
    u32_t io_opver = 0;
    u32_t io_ver = 0;
    u64_t io_createtime = 0;
    u64_t io_lastmodifytime = 0;
    u64_t io_totalsize = 0;
    u64_t io_realsize = 0;


    u32_t unit = (int) cbegin / c->pagesize;
    u64_t begin = unit * c->pagesize;
    u64_t offset = cbegin - begin;
    u64_t len = offset + ctotalsize;

    fd = open(fname,\
            O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
    if(0 == fd){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,\
                "open chunkfile:%s is fail.",
                fname);
        goto r1;
    }

    mptr = mmap(NULL,\
            len,PROT_READ | PROT_WRITE ,\
            MAP_SHARED,fd,begin);
    if(MAP_FAILED == mptr){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,\
                "mmap chunkfile:%s is fail.",
                fname);
        goto r1;
    }

    ioctx = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);
    if(NULL == ioctx){
        SpxLog2(c->log,SpxLogError,err,\
                "alloc io ctx is fail.");
        goto r1;
    }

    if(0 != (err = spx_msg_pack_ubytes(ioctx,
                    ((ubyte_t *) (mptr+ offset)),
                    YDB_CHUNKFILE_MEMADATA_SIZE))){
        SpxLog2(c->log,SpxLogError,err,\
                "pack io ctx is fail.");
        goto r1;
    }

    spx_msg_seek(ioctx,0,SpxMsgSeekSet);

    err = ydb_storage_dio_parser_metadata(c->log,ioctx,
            &io_isdelete,&io_opver,
            &io_ver,&io_createtime,
            &io_lastmodifytime,&io_totalsize,&io_realsize,
            &io_suffix,&io_hashcode);
    if(0 != err){
        SpxLog2(c->log,SpxLogError,err,\
                "unpack io ctx is fail.");
        goto r1;
    }

    if(io_isdelete){
        SpxLogFmt2(c->log,SpxLogError,err,\
                "the file in the chunkfile:%s "
                "begin is %lld totalsize:%lld is deleted.",
                fname,cbegin,ctotalsize);
        goto r1;
    }
    if(copver != io_opver || cver != io_ver
            || clastmodifytime != io_lastmodifytime
            || ctotalsize != io_totalsize
            || crealsize != io_realsize){
        SpxLog2(c->log,SpxLogError,err,
                "the file is not same as want to delete-file.");
        goto r1;
    }

    spx_msg_seek(ioctx,0,SpxMsgSeekSet);
    spx_msg_pack_true(ioctx);//isdelete
    spx_msg_pack_u32(ioctx,copver + 1);
    spx_msg_pack_u32(ioctx,YDB_VERSION);
    spx_msg_seek(ioctx,sizeof(u64_t),SpxMsgSeekCurrent);//jump createtime
    spx_msg_pack_u64(ioctx,lastmodifytime);

    memcpy(mptr + offset,ioctx->buf,YDB_CHUNKFILE_MEMADATA_SIZE);

r1:
    if(NULL != ioctx){
        SpxMsgFree(ioctx);
    }
    if(NULL != mptr){
        munmap(mptr,len);
    }
    if(0 < fd){
        SpxClose(fd);
    }
    if(NULL != io_hashcode){
        SpxStringFree(io_hashcode);
    }
    if(NULL != io_suffix){
        SpxStringFree(io_suffix);
    }
    return err;
}/*}}}*/
