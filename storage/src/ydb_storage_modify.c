
/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_modify.c
 *        Created:  2014/08/22 16时46分06秒
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
#include "ydb_storage_modify.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_dio.h"
#include "ydb_storage_upload.h"
#include "ydb_storage_delete.h"


spx_private err_t ydb_storage_modify_after(
        struct ydb_storage_dio_context *dc);
spx_private void ydb_storage_do_modify_to_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents);
spx_private void ydb_storage_do_modify_to_singlefile(
        struct ev_loop *loop,ev_async *w,int revents);


err_t ydb_storage_dio_modify(struct ev_loop *loop,\
        struct ydb_storage_dio_context *dc){/*{{{*/
    err_t err = 0;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    struct spx_msg_header *rqh = dc->jc->reader_header;

    dc->lastmodifytime = spx_now();
    dc->realsize = rqh->bodylen - rqh->offset;
    dc->isdelete = false;
    dc->ver = YDB_VERSION;

    if(c->overload){
        dc->totalsize = YDB_STORAGE_OVERMODE_ABSSOLUTE == c->overmode
            ? dc->realsize + c->oversize \
            : dc->realsize + dc->realsize * c->oversize / 100;
    } else {
        dc->totalsize = dc->realsize;
    }

    if(c->chunkfile && (dc->realsize <= c->singlemin)){
        dc->issinglefile = false;
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_do_modify_to_chunkfile,dc);
    } else {//singlefile
        dc->issinglefile = true;
        spx_dio_regedit_async(&(dc->async),
                ydb_storage_do_modify_to_singlefile,dc);
    }
    ev_async_start(loop,&(dc->async));
    ev_async_send(loop,&(dc->async));
    return err;
}/*}}}*/

spx_private void ydb_storage_do_modify_to_chunkfile(
        struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
    ev_async_stop(loop,w);
    err_t err = 0;
    SpxTypeConvert2(struct ydb_storage_dio_context,dc,w->data);
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;

    string_t o_fname = NULL;

    struct spx_msg *ctx = jc->reader_body_ctx;
    spx_msg_seek(ctx,0,SpxMsgSeekSet);
    size_t ofid_len = spx_msg_unpack_i32(ctx);
    dc->rfid =  spx_msg_unpack_string(ctx,ofid_len,&err);

    dc->has_suffix = spx_msg_unpack_bool(ctx);
    if(dc->has_suffix){
        dc->suffix = spx_msg_unpack_string(ctx,YDB_FILENAME_SUFFIX_SIZE,&err);
    }

    string_t o_groupname = NULL;
    string_t o_machineid = NULL;
    string_t o_syncgroup = NULL;
    string_t o_hashcode = NULL;
    bool_t o_issinglefile = false;
    string_t o_suffix = NULL;

    u8_t o_mpidx = 0;
    u8_t o_p1 = 0;
    u8_t o_p2 = 0;
    u32_t o_tidx = 0;
    u64_t o_fcreatetime = 0l;
    u32_t o_rand = 0;
    u64_t o_begin = 0l;
    u64_t o_realsize = 0l;
    u64_t o_totalsize = 0l;
    u32_t o_ver = 0;
    u32_t o_opver = 0;
    u64_t o_lastmodifytime = 0l;
    bool_t o_has_suffix = false;
    if(0 != ( err = ydb_storage_dio_parser_fileid(jc->log,dc->rfid,
                    &(o_groupname),&(o_machineid),&(o_syncgroup),
                    &(o_issinglefile),&(o_mpidx),&(o_p1),
                    &(o_p2),&(o_tidx),&(o_fcreatetime),
                    &(o_rand),&(o_begin),&(o_realsize),
                    &(o_totalsize),&(o_ver),&(o_opver),
                    &(o_lastmodifytime),&(o_hashcode),
                    &(o_has_suffix),&(o_suffix)))){

        SpxLog2(dc->log,SpxLogError,err,\
                "parser fid is fail.");
        goto r1;
    }

    o_fname = ydb_storage_dio_make_filename(dc->log,
            o_issinglefile,c->mountpoints,
            o_mpidx,o_p1,o_p2,
            o_machineid,o_tidx,o_fcreatetime,
            o_rand,o_suffix,&err);
    if(NULL == o_fname){
        SpxLog2(dc->log,SpxLogError,err,\
                "make filename is fail.");
        goto r1;
    }

    bool_t is_ofile_exist = SpxFileExist(o_fname);

    //the file is exist and old-file context can save new file-context
    if(is_ofile_exist && !o_issinglefile && o_totalsize >= dc->realsize){
        u32_t unit = (int) o_begin / c->pagesize;
        u64_t begin = unit * c->pagesize;
        u64_t offset = o_begin - begin;
        u64_t len = offset + o_totalsize;

        int fd = open(o_fname,
                O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
        if(0 == fd){
            err = errno;
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "open chunkfile:%s is fail.",
                    o_fname);
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
                    o_fname);
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

        if(0 != (err = spx_msg_pack_ubytes(ioctx,
                        ((ubyte_t *) (mptr+ offset)),
                        YDB_CHUNKFILE_MEMADATA_SIZE))){
            SpxLog2(dc->log,SpxLogError,err,\
                    "pack io ctx is fail.");
            SpxMsgFree(ioctx);
            SpxClose(fd);
            munmap(mptr,len);
            goto r1;
        }
        spx_msg_seek(ioctx,0,SpxMsgSeekSet);
        bool_t io_isdelete = false;
        u32_t io_opver = 0;
        u32_t io_ver = 0;
        u64_t io_createtime = 0;
        u64_t io_lastmodifytime = 0;
        u64_t io_totalsize = 0;
        u64_t io_realsize = 0;
        string_t io_suffix = NULL;
        string_t io_hashcode = NULL;

        err = ydb_storage_dio_parser_metadata(dc->log,ioctx,
                &io_isdelete,&io_opver,
                &io_ver,&io_createtime,
                &io_lastmodifytime,&io_totalsize,&io_realsize,
                &io_suffix,&io_hashcode);
        if(0 != err){
            SpxLog2(dc->log,SpxLogError,err,\
                    "unpack io ctx is fail.");
            SpxMsgFree(ioctx);
            SpxClose(fd);
            munmap(mptr,len);
            if(NULL != io_suffix){
                SpxStringFree(io_suffix);
            }
            if(NULL != io_hashcode){
                SpxStringFree(io_hashcode);
            }
            goto r1;
        }

        //the file is want to modify context by client
        //if not,do upload
        if(o_opver == io_opver && o_ver == io_ver
                && o_lastmodifytime == io_lastmodifytime
                && o_totalsize == io_totalsize
                && o_realsize == io_realsize){

            dc->totalsize = o_totalsize;//update total size
            dc->opver ++;

            spx_msg_clear(ioctx);
            spx_msg_pack_false(ioctx);//isdelete
            spx_msg_pack_u32(ioctx,dc->opver);
            spx_msg_pack_u32(ioctx,dc->ver);
            spx_msg_pack_u64(ioctx,dc->createtime);
            spx_msg_pack_u64(ioctx,dc->lastmodifytime);
            spx_msg_pack_u64(ioctx,dc->totalsize);
            spx_msg_pack_u64(ioctx,dc->realsize);
            if(dc->has_suffix){
                spx_msg_pack_fixed_string(ioctx,
                        dc->suffix,YDB_FILENAME_SUFFIX_SIZE);
            } else {
                spx_msg_align(ioctx,YDB_FILENAME_SUFFIX_SIZE);
            }

            if(NULL == dc->hashcode){
                spx_msg_align(ioctx,YDB_HASHCODE_SIZE);
            } else {
                spx_msg_pack_fixed_string(ioctx,
                        dc->hashcode,YDB_HASHCODE_SIZE);
            }

            u64_t off = offset;
            size_t recvbytes = 0;
            size_t writebytes = 0;
            size_t unitlen = 0;

            memcpy(mptr + off,
                    ioctx->buf,YDB_CHUNKFILE_MEMADATA_SIZE);
            off += YDB_CHUNKFILE_MEMADATA_SIZE;
            if(jc->is_lazy_recv){
                dc->buf = spx_alloc_alone(YdbBufferSizeForLazyRecv,&err);
                if(NULL == dc->buf){
                    SpxLogFmt2(dc->log,SpxLogError,err,
                            "new lazy-recv buffer is fail."
                            "size:%lld.",YdbBufferSizeForLazyRecv);
                    SpxMsgFree(ioctx);
                    SpxClose(fd);
                    munmap(mptr,len);
                    if(NULL != io_suffix){
                        SpxStringFree(io_suffix);
                    }
                    if(NULL != io_hashcode){
                        SpxStringFree(io_hashcode);
                    }
                    goto r1;
                }

                do{
                    recvbytes = dc->realsize - writebytes > YdbBufferSizeForLazyRecv \
                                ? YdbBufferSizeForLazyRecv \
                                : dc->realsize - writebytes;
                    err = spx_read_nb(jc->fd,(byte_t *) dc->buf,recvbytes,&unitlen);
                    if(0 != err || recvbytes != unitlen){
                        SpxLogFmt2(dc->log,SpxLogError,err,\
                                "lazy read buffer and cp to mmap is fail."
                                "recvbytes:%lld,real recvbytes:%lld."
                                "writedbytes:%lld,total size:%lld.",
                                recvbytes,unitlen,writebytes,dc->realsize);

                        SpxMsgFree(ioctx);
                        SpxClose(fd);
                        munmap(mptr,len);
                        if(NULL != io_suffix){
                            SpxStringFree(io_suffix);
                        }
                        if(NULL != io_hashcode){
                            SpxStringFree(io_hashcode);
                        }
                        goto r1;
                    }

                    memcpy(mptr + off,dc->buf,unitlen);
                    off += unitlen;
                    writebytes += unitlen;
                    if(writebytes < dc->realsize) {
                        continue;
                    }
                }while(false);
            } else {
                memcpy(mptr + off,
                        jc->reader_body_ctx->buf + jc->reader_header->offset,
                        jc->reader_header->bodylen - jc->reader_header->offset);
                off += jc->reader_header->bodylen - jc->reader_header->offset;
            }

            dc->begin = o_begin;
            dc->rand = o_rand;
            dc->tidx = o_tidx;
            dc->p1 = o_p1;
            dc->p2 = o_p2;
            dc->mp_idx = o_mpidx;
            dc->file_createtime = o_fcreatetime;

            SpxMsgFree(ioctx);
            SpxClose(fd);
            munmap(mptr,len);
            if(NULL != io_suffix){
                SpxStringFree(io_suffix);
            }
            if(NULL != io_hashcode){
                SpxStringFree(io_hashcode);
            }
            if(0 != (err = ydb_storage_modify_after(dc))){
                SpxLog2(dc->log,SpxLogError,err,
                        "make response for modify is fail.");
                goto r1;
            }
            goto r2;
        }
    }

    //if no old-file,old-file is singlefile,old-totalsize < new-realsize,
    //it means upload a new file with ydb
    //so,we do upload new file and just only check old-file and delete it

    if(0 != ( err =  ydb_storage_dio_upload_to_chunkfile(dc))){
        SpxLog2(dc->log,SpxLogError,err,
                "modify context convert upload is fail.");
        goto r1;
    }
    if(0 != (err = ydb_storage_modify_after(dc))){
        SpxLog2(dc->log,SpxLogError,err,
                "make response for modify is fail.");
        goto r1;
    }

    //delete old context
    if(o_issinglefile){
        if(is_ofile_exist
                && (0 != remove(o_fname))){
            SpxLogFmt2(dc->log,SpxLogError,err,
                    "delete single file:%s is fail.",o_fname);
            goto r2;
        }
    }else {
        if(!is_ofile_exist){
            goto r2;//no file no modify metadata
        }
        if(0 != (err =  ydb_storage_dio_delete_context_from_chunkfile(
                        c,o_fname,o_begin,o_totalsize,
                        o_opver,o_ver,o_lastmodifytime,
                        o_realsize,spx_now()))){
            SpxLog2(dc->log,SpxLogError,err,
                    "delete context form chunkfile is fail.");
            goto r2;
        }
    }
    goto r2;
r1:

    if(NULL != o_groupname){
        SpxStringFree(o_groupname);
    }
    if(NULL != o_machineid){
        SpxStringFree(o_machineid);
    }
    if(NULL != o_syncgroup){
        SpxStringFree(o_syncgroup);
    }
    if(NULL != o_hashcode){
        SpxStringFree(o_hashcode);
    }
    if(NULL != o_suffix){
        SpxStringFree(o_suffix);
    }
    if(NULL != o_fname){
        SpxStringFree(o_fname);
    }
    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    if(NULL == jc->writer_header) {
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
    jc->writer_header->protocol = YDB_C2S_MODIFY;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext_err =
        spx_get_thread(g_spx_network_module,i);
    jc->tc = threadcontext_err;
    //    err = spx_module_dispatch(threadcontext_err,
    //            spx_network_module_wakeup_handler,jc);
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
r2:
    YdbStorageBinlogModifyWriter(dc->fid,dc->rfid);
    if(NULL != o_groupname){
        SpxStringFree(o_groupname);
    }
    if(NULL != o_machineid){
        SpxStringFree(o_machineid);
    }
    if(NULL != o_syncgroup){
        SpxStringFree(o_syncgroup);
    }
    if(NULL != o_hashcode){
        SpxStringFree(o_hashcode);
    }
    if(NULL != o_suffix){
        SpxStringFree(o_suffix);
    }
    if(NULL != o_fname){
        SpxStringFree(o_fname);
    }

    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    //    err = spx_module_dispatch(threadcontext,
    //            spx_network_module_wakeup_handler,jc);
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
}/*}}}*/

spx_private void ydb_storage_do_modify_to_singlefile(
        struct ev_loop *loop,ev_async *w,int revents){/*{{{*/
    ev_async_stop(loop,w);
    err_t err = 0;
    SpxTypeConvert2(struct ydb_storage_dio_context,dc,w->data);
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;
    string_t o_fname = NULL;

    struct spx_msg *ctx = jc->reader_body_ctx;
    spx_msg_seek(ctx,0,SpxMsgSeekSet);
    size_t ofid_len = spx_msg_unpack_i32(ctx);
    dc->rfid =  spx_msg_unpack_string(ctx,ofid_len,&err);

    dc->has_suffix = spx_msg_unpack_bool(ctx);
    if(dc->has_suffix){
        dc->suffix = spx_msg_unpack_string(ctx,YDB_FILENAME_SUFFIX_SIZE,&err);
    }

    string_t o_groupname = NULL;
    string_t o_machineid = NULL;
    string_t o_syncgroup = NULL;
    string_t o_hashcode = NULL;
    bool_t o_issinglefile = false;
    string_t o_suffix = NULL;

    u8_t o_mpidx = 0;
    u8_t o_p1 = 0;
    u8_t o_p2 = 0;
    u32_t o_tidx = 0;
    u64_t o_fcreatetime = 0l;
    u32_t o_rand = 0;
    u64_t o_begin = 0l;
    u64_t o_realsize = 0l;
    u64_t o_totalsize = 0l;
    u32_t o_ver = 0;
    u32_t o_opver = 0;
    u64_t o_lastmodifytime = 0l;
    bool_t o_has_suffix = false;
    if(0 != ( err = ydb_storage_dio_parser_fileid(jc->log,dc->rfid,
                    &(o_groupname),&(o_machineid),&(o_syncgroup),
                    &(o_issinglefile),&(o_mpidx),&(o_p1),
                    &(o_p2),&(o_tidx),&(o_fcreatetime),
                    &(o_rand),&(o_begin),&(o_realsize),
                    &(o_totalsize),&(o_ver),&(o_opver),
                    &(o_lastmodifytime),&(o_hashcode),
                    &(o_has_suffix),&(o_suffix)))){

        SpxLog2(dc->log,SpxLogError,err,\
                "parser fid is fail.");
        goto r1;
    }

    o_fname = ydb_storage_dio_make_filename(dc->log,
            o_issinglefile,c->mountpoints,
            o_mpidx,o_p1,o_p2,
            o_machineid,o_tidx,o_fcreatetime,
            o_rand,o_suffix,&err);
    if(NULL == o_fname){
        SpxLog2(dc->log,SpxLogError,err,\
                "make filename is fail.");
        goto r1;
    }

    if(0 != (err = ydb_storage_dio_upload_to_singlefile(dc))){
        SpxLog2(dc->log,SpxLogError,err,
                "modify context convert upload is fail.");
        goto r1;
    }

    if(0 != (err = ydb_storage_modify_after(dc))){
        SpxLog2(dc->log,SpxLogError,err,
                "make response for modify is fail.");
        goto r1;
    }

    bool_t is_ofile_exist = SpxFileExist(o_fname);

    //delete old context
    if(o_issinglefile){
        if(is_ofile_exist
                && (0 != remove(o_fname))){
            SpxLogFmt2(dc->log,SpxLogError,err,
                    "delete single file:%s is fail.",o_fname);
            goto r2;
        }
    }else {
        if(!is_ofile_exist){
            goto r2;//no file no modify metadata
        }
        u32_t unit = (int) o_begin / c->pagesize;
        u64_t begin = unit * c->pagesize;
        u64_t offset = o_begin - begin;
        u64_t len = offset + o_totalsize;

        int fd = open(o_fname,
                O_RDWR|O_APPEND|O_CREAT,SpxFileMode);
        if(0 == fd){
            err = errno;
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "open chunkfile:%s is fail.",
                    o_fname);
            goto r2;
        }

        char *mptr = mmap(NULL,\
                len,PROT_READ | PROT_WRITE ,\
                MAP_SHARED,fd,begin);
        if(MAP_FAILED == mptr){
            err = errno;
            SpxClose(fd);
            SpxLogFmt2(dc->log,SpxLogError,err,\
                    "mmap chunkfile:%s is fail.",
                    o_fname);
            goto r2;
        }

        struct spx_msg *ioctx = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,&err);
        if(NULL == ioctx){
            SpxLog2(dc->log,SpxLogError,err,\
                    "alloc io ctx is fail.");
            SpxClose(fd);
            munmap(mptr,len);
            goto r2;
        }

        if(0 != (err = spx_msg_pack_ubytes(ioctx,
                        ((ubyte_t *) (mptr+ offset)),
                        YDB_CHUNKFILE_MEMADATA_SIZE))){
            SpxLog2(dc->log,SpxLogError,err,\
                    "pack io ctx is fail.");
            SpxMsgFree(ioctx);
            SpxClose(fd);
            munmap(mptr,len);
            goto r2;
        }
        spx_msg_seek(ioctx,0,SpxMsgSeekSet);
        bool_t io_isdelete = false;
        u32_t io_opver = 0;
        u32_t io_ver = 0;
        u64_t io_createtime = 0;
        u64_t io_lastmodifytime = 0;
        u64_t io_totalsize = 0;
        u64_t io_realsize = 0;
        string_t io_suffix = NULL;
        string_t io_hashcode = NULL;

        err = ydb_storage_dio_parser_metadata(dc->log,ioctx,
                &io_isdelete,&io_opver,
                &io_ver,&io_createtime,
                &io_lastmodifytime,&io_totalsize,&io_realsize,
                &io_suffix,&io_hashcode);
        if(0 != err){
            SpxLog2(dc->log,SpxLogError,err,\
                    "unpack io ctx is fail.");
            SpxMsgFree(ioctx);
            SpxClose(fd);
            munmap(mptr,len);
            if(NULL != io_suffix){
                SpxStringFree(io_suffix);
            }
            if(NULL != io_hashcode){
                SpxStringFree(io_hashcode);
            }
            goto r2;
        }

        if(o_opver != io_opver || o_ver != io_ver
                || o_lastmodifytime != io_lastmodifytime
                || o_totalsize != io_totalsize
                || o_realsize != io_realsize){
            SpxLog2(dc->log,SpxLogError,err,\
                    "the file is not same as want to delete-file.");
            SpxMsgFree(ioctx);
            SpxClose(fd);
            munmap(mptr,len);
            if(NULL != io_suffix){
                SpxStringFree(io_suffix);
            }
            if(NULL != io_hashcode){
                SpxStringFree(io_hashcode);
            }
            goto r2;
        }

        spx_msg_seek(ioctx,0,SpxMsgSeekSet);
        spx_msg_pack_true(ioctx);//isdelete
        spx_msg_pack_u32(ioctx,dc->opver + 1);
        spx_msg_pack_u32(ioctx,dc->ver);
        spx_msg_seek(ioctx,sizeof(u64_t),SpxMsgSeekCurrent);//jump createtime
        spx_msg_pack_u64(ioctx,spx_now());
        spx_msg_pack_u64(ioctx,dc->totalsize);

        memcpy(mptr + offset,
                ioctx->buf,YDB_CHUNKFILE_MEMADATA_SIZE);
        SpxMsgFree(ioctx);
        SpxClose(fd);
        munmap(mptr,len);
        if(NULL != io_suffix){
            SpxStringFree(io_suffix);
        }
        if(NULL != io_hashcode){
            SpxStringFree(io_hashcode);
        }
        goto r2;
    }
r1:

    if(NULL != o_groupname){
        SpxStringFree(o_groupname);
    }
    if(NULL != o_machineid){
        SpxStringFree(o_machineid);
    }
    if(NULL != o_syncgroup){
        SpxStringFree(o_syncgroup);
    }
    if(NULL != o_hashcode){
        SpxStringFree(o_hashcode);
    }
    if(NULL != o_suffix){
        SpxStringFree(o_suffix);
    }
    if(NULL != o_fname){
        SpxStringFree(o_fname);
    }
    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    if(NULL == jc->writer_header) {
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
    jc->writer_header->protocol = YDB_C2S_MODIFY;
    jc->writer_header->bodylen = 0;
    jc->writer_header->version = YDB_VERSION;
    jc->writer_header->err = err;

    jc->moore = SpxNioMooreResponse;
    size_t i = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext_err =
        spx_get_thread(g_spx_network_module,i);
    jc->tc = threadcontext_err;
    //    err = spx_module_dispatch(threadcontext_err,
    //            spx_network_module_wakeup_handler,jc);
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);
    return;
r2:
    YdbStorageBinlogModifyWriter(dc->fid,dc->rfid);
    if(NULL != o_groupname){
        SpxStringFree(o_groupname);
    }
    if(NULL != o_machineid){
        SpxStringFree(o_machineid);
    }
    if(NULL != o_syncgroup){
        SpxStringFree(o_syncgroup);
    }
    if(NULL != o_hashcode){
        SpxStringFree(o_hashcode);
    }
    if(NULL != o_suffix){
        SpxStringFree(o_suffix);
    }
    if(NULL != o_fname){
        SpxStringFree(o_fname);
    }

    spx_task_pool_push(g_spx_task_pool,dc->tc);
    ydb_storage_dio_pool_push(g_ydb_storage_dio_pool,dc);

    jc->moore = SpxNioMooreResponse;
    size_t idx = spx_network_module_wakeup_idx(jc);
    struct spx_thread_context *threadcontext = spx_get_thread(g_spx_network_module,idx);
    jc->tc = threadcontext;
    //    err = spx_module_dispatch(threadcontext,
    //            spx_network_module_wakeup_handler,jc);
    SpxModuleDispatch(spx_network_module_wakeup_handler,jc);

    return;
}/*}}}*/

spx_private err_t ydb_storage_modify_after(
        struct ydb_storage_dio_context *dc){/*{{{*/
    err_t err = 0;
    struct spx_job_context *jc = dc->jc;
    struct ydb_storage_configurtion *c = jc->config;
    //    struct ydb_storage_storefile *cf = dc->storefile;

    size_t len = 0;

    dc->fid =  ydb_storage_dio_make_fileid(c->log,
            c->groupname,c->machineid,c->syncgroup,
            dc->issinglefile,dc->mp_idx,dc->p1,dc->p2,
            dc->tidx,dc->file_createtime,dc->rand,
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
    h->protocol = YDB_C2S_MODIFY;
    h->bodylen = len;
    h->version = YDB_VERSION;
    h->offset = len;
    jc->is_sendfile = false;
    return err;
}/*}}}*/


