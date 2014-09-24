/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_find.c
 *        Created:  2014/08/14 17时15分20秒
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
#include "ydb_storage_dio.h"
#include "ydb_storage_find.h"


spx_private void ydb_storage_dio_do_find_form_chunkfile(struct ev_loop *loop,ev_async *w,int revents);
spx_private void ydb_storage_dio_do_find_form_signalfile(struct ev_loop *loop,ev_async *w,int revents);

err_t ydb_storage_dio_find(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc){/*{{{*/
    err_t err = 0;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

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
        err = ENOENT;
        SpxLogFmt1(dc->log,SpxLogWarn,\
                "deleting-file:%s is not exist.",
                dc->buf);
        goto r1;
    }

    if(dc->issignalfile){
        spx_dio_regedit_async(&(dc->async),ydb_storage_dio_do_find_form_chunkfile,dc);
    } else {
        spx_dio_regedit_async(&(dc->async),ydb_storage_dio_do_find_form_signalfile,dc);
    }
        ev_async_send(loop,&(dc->async));
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
}/*}}}*/

spx_private void ydb_storage_dio_do_find_form_chunkfile(struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
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

    struct spx_msg_header *wh = (struct spx_msg_header *) \
                                spx_alloc_alone(sizeof(*wh),&err);
    if(NULL == wh){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc write header for find buffer in chunkfile is fail.");
        spx_msg_free(&ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        goto r1;
    }

    jc->moore = SpxNioMooreResponse;
    jc->writer_header =wh;
    wh->version = YDB_VERSION;
    wh->protocol = YDB_STORAGE_FIND;
    wh->offset = 0;
    wh->bodylen = dc->realsize;

    if(c->sendfile){
        jc->is_sendfile = true;
        jc->sendfile_fd = fd;
        jc->sendfile_begin = dc->begin + YDB_CHUNKFILE_MEMADATA_SIZE;
        jc->sendfile_size = dc->realsize;
    } else {
        jc->is_sendfile = false;
        struct spx_msg *ctx = spx_msg_new(dc->realsize,&err);
        if(NULL == ctx){
            SpxLog2(dc->log,SpxLogError,err,\
                    "alloc buffer ctx for finding writer is fail.");
            SpxClose(fd);
            goto r1;
        }
        jc->writer_body_ctx = ctx;
        spx_msg_pack_ubytes(ctx,(ubyte_t *)mptr + begin + YDB_CHUNKFILE_MEMADATA_SIZE,dc->realsize);
        SpxClose(fd);
    }

    munmap(mptr,len);
    spx_msg_free(&ioctx);
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
}/*}}}*/

spx_private void ydb_storage_dio_do_find_form_signalfile(struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
    err_t err = 0;
    struct ydb_storage_dio_context *dc = (struct ydb_storage_dio_context *) w->data;
    struct spx_task_context *tc = dc->tc;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    struct stat buf;
    memset(&buf,0,sizeof(buf));
    if(0 != lstat (dc->buf,&buf)){
        err = errno;
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "get signalfile:%s stat is fail.",
                dc->buf);
        goto r1;
    }

    if((u64_t) buf.st_size != dc->realsize){
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "file:%s file size is %lld,and request size is %lld.",
                dc->buf,buf.st_size,dc->realsize);
        goto r1;
    }

    struct spx_msg_header *wh = (struct spx_msg_header *) \
                                spx_alloc_alone(sizeof(*wh),&err);
    if(NULL == wh){
        SpxLog2(dc->log,SpxLogError,err,\
                "alloc write header for find buffer in chunkfile is fail.");
        goto r1;
    }

    jc->writer_header =wh;
    jc->moore = SpxNioMooreResponse;
    wh->version = YDB_VERSION;
    wh->protocol = YDB_STORAGE_FIND;
    wh->offset = 0;
    wh->bodylen = dc->realsize;

    int fd = open(dc->buf,\
            O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
    if(0 == fd){
        err = errno;
        SpxLogFmt2(dc->log,SpxLogError,err,\
                "open chunkfile:%s is fail.",
                dc->buf);
        goto r1;
    }

    if(c->sendfile){
        jc->is_sendfile = true;
        jc->sendfile_fd = fd;
        jc->sendfile_begin = 0;
        jc->sendfile_size = dc->realsize;
    } else {
        jc->is_sendfile = false;
        struct spx_msg *ctx = spx_msg_new(dc->realsize,&err);
        if(NULL == ctx){
            SpxLog2(dc->log,SpxLogError,err,\
                    "alloc buffer ctx for finding writer is fail.");
            SpxClose(fd);
            goto r1;
        }
        jc->writer_body_ctx = ctx;
        char *mptr = mmap(NULL,\
                dc->realsize,PROT_READ | PROT_WRITE ,\
                MAP_SHARED,fd,0);
        if(MAP_FAILED == mptr){
            err = errno;
            SpxClose(fd);
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "mmap chunkfile:%s is fail.",
                    dc->buf);
            goto r1;
        }
        spx_msg_pack_ubytes(ctx,(ubyte_t *)mptr,dc->realsize);
        munmap(mptr,dc->realsize);
        SpxClose(fd);
    }
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
}/*}}}*/


