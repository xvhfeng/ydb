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


spx_private void ydb_storage_dio_do_delete_form_chunkfile(struct ev_loop *loop,ev_async *w,int revents);

err_t ydb_storage_dio_delete(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc){
    err_t err = 0;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;
    struct ydb_storage_storefile *cf = dc->storefile;

    struct spx_msg *ctx = jc->reader_body_ctx;
    size_t len = jc->reader_header->bodylen;
    if(0 != ( err = ydb_storage_dio_parser_fileid(ctx,len,dc))){
        SpxLog2(dc->log,SpxLogError,err,\
                "parser fid is fail.");
        goto r1;
    }

    dc->buf = ydb_storage_dio_make_filename(dc->log,c->mountpoints,\
            dc->mp_idx,dc->machineid,dc->tidx,dc->file_createtime,\
            dc->rand,dc->suffix,&err);
    if(NULL == dc->buf){
        SpxLog2(dc->log,SpxLogError,err,\
                "make filename is fail.");
        goto r1;
    }

    if(!SpxFileExist(dc->buf)) {
        SpxLogFmt1(dc->log,SpxLogWarn,\
                "deleting-file:%s is not exist.",
                dc->buf);
        goto r1;
    }

    if(dc->issignalfile){
        if(0 != remove(dc->buf)){
            err = errno;
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "delete file :%s is fail.",
                    dc->buf);
        }

        YdbStorageBinlog(YDB_BINLOG_DELETE,dc->issignalfile,dc->ver,dc->opver,cf->machineid,\
                dc->file_createtime,dc->createtime,dc->lastmodifytime,dc->mp_idx,dc->p1,dc->p2,\
                cf->tidx,dc->rand,dc->begin,dc->totalsize,dc->realsize,dc->suffix);
        goto r1;
    } else {
        spx_dio_regedit_async(&(dc->async),ydb_storage_dio_do_delete_form_chunkfile,dc);
        ev_async_send(loop,&(dc->async));
    }
    return err;
r1:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    err = spx_module_dispatch(threadcontext,spx_network_module_wakeup_handler,jc);
    if(0 != err){
        SpxLog2(jc->log,SpxLogError,err,\
                "dispatch network module is fail,and push jcontext to pool force.");
        spx_job_pool_push(g_spx_job_pool,jc);
    }
    return 0;
}

spx_private void ydb_storage_dio_do_delete_form_chunkfile(struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
    err_t err = 0;
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *) w->data;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    u32_t unit = (int) dc->begin / c->pagesize;
    u64_t begin = unit * c->pagesize;
    u64_t offset = dc->begin - unit % c->pagesize;
    u64_t len = offset + dc->totalsize;

    int fd = open(dc->buf,\
            O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
    if(0 == fd){
        err = errno;
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "open chunkfile:%s is fail.",
                dc->buf);
        goto r1;
    }

    char *mptr = mmap(NULL,\
            len,PROT_READ | PROT_WRITE ,\
            MAP_SHARED,fd,begin);
    if(MAP_FAILED == mptr){
        err = errno;
        SpxClose(fd);
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "mmap chunkfile:%s is fail.",
                dc->buf);
        goto r1;
    }

    struct spx_msg *ioctx = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);
    if(NULL == ioctx){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc io ctx is fail.");
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }

    if(0 != (err = spx_msg_pack_ubytes(ioctx,\
                    ((ubyte_t *) (mptr+ offset)),YDB_CHUNKFILE_MEMADATA_SIZE))){
        SpxLog2(dc->log,SpxLogError,err,\
                "pack io ctx is fail.");
        spx_msg_free(&ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }
    bool_t io_isdelete = false;
    u32_t io_opver = 0;
    u32_t io_ver = 0;
    u64_t io_createtime = 0;
    u64_t io_lastmodifytime = 0;
    u64_t io_totalsize = 0;
    u64_t io_realsize = 0;
    string_t io_suffix = NULL;
    string_t io_hashcode = NULL;

    err = ydb_storage_dio_parser_metadata(dc->log,ioctx,&io_isdelete,&io_opver,\
            &io_ver,&io_createtime,&io_lastmodifytime,&io_totalsize,&io_realsize,\
            &io_suffix,&io_hashcode);
    if(0 != err){
        SpxLog2(dc->log,SpxLogError,err,\
                "unpack io ctx is fail.");
        spx_msg_free(&ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }

    if(io_isdelete){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "the file in the chunkfile:%s begin is %lld totalsize:%lld is deleted.",
                dc->buf,dc->begin,dc->totalsize);
        spx_msg_free(&ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }
    if(dc->opver != io_opver || dc->ver != io_ver || dc->createtime != io_createtime \
            || dc->lastmodifytime != io_lastmodifytime || dc->totalsize != io_totalsize \
            || dc->realsize != io_realsize){
        SpxLog2(dc->log,SpxLogError,err,\
                "the file is not same as want to delete-file.");
        spx_msg_free(&ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }

    dc->metadata =  ydb_storage_dio_make_metadata(dc->log,true,dc->opver + 1,dc->ver + 1,\
            dc->createtime,spx_now(),dc->totalsize,dc->realsize,dc->suffix,dc->hashcode,&err);
    if(NULL == dc->metadata){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc new metadata is fail.");
        spx_msg_free(&ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }
    spx_mmap_form_msg(mptr,offset,dc->metadata);
    spx_msg_free(&ioctx);
    munmap(mptr,len);
    SpxClose(fd);

r1:
    spx_task_pool_push(g_spx_task_pool,tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);
    jc->err = err;
    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    err = spx_module_dispatch(threadcontext,spx_network_module_wakeup_handler,jc);
    if(0 != err){
        SpxLog2(jc->log,SpxLogError,err,\
                "dispatch network module is fail,and push jcontext to pool force.");
        spx_job_pool_push(g_spx_job_pool,jc);
    }
    return;
}
