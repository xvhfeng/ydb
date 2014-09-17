/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_dio.c
 *        Created:  2014/08/14 17时27分44秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>

#include "spx_types.h"
#include "spx_defs.h"
#include "spx_string.h"
#include "spx_list.h"
#include "spx_path.h"
#include "spx_message.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_dio_context.h"
#include "ydb_storage_storefile.h"

err_t ydb_storage_dio_mountpoint_init(struct ydb_storage_configurtion *c){/*{{{*/
    err_t err = 0;
    int i = 0;
    string_t path = spx_string_newlen(NULL,SpxPathSize,&err);
    if(NULL == path){
        SpxLog2(c->log,SpxLogError,err,\
                "new path is fail.");
        return err;
    }
    printf("make store path:\n");
    for( ; i< YDB_STORAGE_MOUNTPOINT_COUNT; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)){
            int j = 0;
            for( ; j < c->storerooms; j++){
                int z = 0;
                for( ; z < c->storerooms; z++){
                    string_t newpath = NULL;
                    if(SpxStringEndWith(mp->path,SpxPathDlmt)){
                        newpath = spx_string_cat_printf(&err,path,"%sdata/%02X/%02X/",
                                mp->path,j,z);
                    } else {
                        newpath = spx_string_cat_printf(&err,path,"%s/data/%02X/%02X/",
                                mp->path,j,z);
                    }

                    if(NULL == newpath){
                        SpxLog2(c->log,SpxLogError,err,
                                "cat store path is fail.");
                        goto r1;
                    }
                    if(!spx_is_dir(newpath,&err)){
                        if(0 != (err = spx_mkdir(c->log,newpath,SpxPathMode))){
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "create store path:%s is fail.",
                                    newpath);
                            goto r1;
                        }
                        printf("mountpoint idz:%02X,store path:%02X/%02X/ is created.\n",
                                i,j,z);
                    }
                    spx_string_clear(path);
                }
            }
        }
    }
r1:
    spx_string_free(path);
    return err;
}/*}}}*/


string_t ydb_storage_dio_make_filename(SpxLogDelegate *log,\
        struct spx_list *mps,\
        u8_t mpidx,string_t machineid,u32_t tidx,\
        u64_t createtime, u32_t rand,string_t suffix,err_t *err){/*{{{*/
    string_t filename = spx_string_newlen(NULL,SpxPathSize,err);
    if(NULL == filename){
        SpxLog2(log,SpxLogError,*err,\
                "new filename is fail.");
        return NULL;
    }
    struct ydb_storage_mountpoint *mp = spx_list_get(mps,mpidx);
    if(SpxStringEndWith(mp->path,SpxPathDlmt)){
        spx_string_cat_printf(err,filename, "%s%s_%d_%lld_%d%s%s",\
                machineid,tidx,createtime,rand,\
                NULL == suffix ? "" : ".",\
                NULL == suffix ? "" : suffix);
    }else{
        spx_string_cat_printf(err,filename,"%s/%s_%d_%lld_%d%s%s",\
                machineid,tidx,createtime,rand,\
                NULL == suffix ? "" : ".",\
                NULL == suffix ? "" : suffix);
    }
    return filename;
}/*}}}*/

string_t ydb_storage_dio_make_fileid(struct ydb_storage_configurtion *c,\
        struct ydb_storage_dio_context *dc,struct ydb_storage_storefile *cf,\
        size_t *len,err_t *err){/*{{{*/

string_t fid = spx_string_newlen(NULL,SpxFileNameSize,err);
    if(NULL == fid){
        SpxLog2(dc->log,SpxLogError,*err,\
                "new file is fail.");
        return NULL;
    }
    spx_string_cat_printf(err,fid,\
            "%s:%s:%d:%d:%d:%d:%d:%ulld:%ud:%ulld:%ulld:%ulld:%ud:%ud:%ulld:%s%s%s",
            c->groupname,c->machineid,dc->issignalfile,dc->mp_idx,dc->p1,dc->p2,\
            cf->tidx,dc->file_createtime,dc->rand,dc->begin,dc->realsize,dc->totalsize,\
            dc->ver,dc->opver,dc->lastmodifytime,NULL == dc->hashcode ? "" : dc->hashcode,\
            dc->has_suffix ? "." : "",dc->has_suffix ? dc->suffix : "");

   *len = spx_string_len(fid);
    return fid;
}/*}}}*/

err_t ydb_storage_dio_parser_fileid(struct spx_msg* ctx,\
        size_t fid_len,struct ydb_storage_dio_context *dc){/*{{{*/
    string_t fid = spx_msg_unpack_string(ctx,0 == fid_len ? ctx->s : fid_len,&(dc->err));
    if(NULL == fid){
        SpxLog2(dc->log,SpxLogError,dc->err,\
                "alloc file id for parser is fail.");
        return dc->err;
    }
    int count = 0;
    string_t *fids = spx_string_splitlen(fid,\
            spx_string_len(fid),":",strlen(":"),\
            &count,&(dc->err));
    if(NULL == fids || 0 == count){
        SpxLogFmt2(dc->log,SpxLogError,dc->err,\
                "split fileid:%s is fail.",fid);
        return dc->err;
    }

    int i = 0;
    for( ; i < count; i++){
        switch (i){
            case 0://groupname
                {
                    dc->groupname = spx_string_dup(*(fids + count),&(dc->err));
                    if(NULL == dc->groupname){
                        SpxLog2(dc->log,SpxLogError,dc->err,\
                                "dup groupname is fail.");
                    }
                    break;
                }
            case 1://machineid
                {
                    dc->machineid = spx_string_dup(*(fids + count),&(dc->err));
                    if(NULL == dc->machineid){
                        SpxLog2(dc->log,SpxLogError,dc->err,\
                                "dup machineid is fail.");
                    }
                    break;
                }
            case 2://issignalfile
                {
                    dc->issignalfile = (bool_t) atoi(*(fids + count));
                    break;
                }
            case 3://mpidx
                {
                    dc->mp_idx = (u8_t) atoi(*(fids + count));
                    break;
                }
            case 4://p1
                {
                    dc->p1 = (u8_t) atoi(*(fids + count));
                    break;
                }
            case 5://p2
                {
                    dc->p2 = (u8_t) atoi(*(fids + count));
                    break;
                }
            case 6://tidx
                {
                    dc->tidx = (u32_t) atol(*(fids + count));
                    break;
                }
            case 7://file_createtime
                {
                    dc->file_createtime = (u64_t) strtoul(*(fids + count),NULL,10);
                    break;
                }
            case 8://rand
                {
                    dc->rand = (u32_t) atol(*(fids + count));
                    break;
                }
            case 9://begin
                {
                    dc->begin = (u64_t) strtoul(*(fids + count),NULL,10);
                    break;
                }
            case 10://realsize
                {
                    dc->realsize = (u64_t) strtoul(*(fids + count),NULL,10);
                    break;
                }
            case 11://totalsize
                {
                    dc->totalsize = (u64_t) strtoul(*(fids + count),NULL,10);
                    break;
                }
            case 12://ver
                {
                    dc->ver = (u32_t) atol(*(fids + count));
                    break;
                }
            case 13://opver
                {
                    dc->opver = (u32_t) atol(*fids + count);
                    break;
                }
            case 14://lastmodifytime
                {
                    dc->lastmodifytime = (u64_t) strtoul(*(fids + count),NULL,10);
                    break;
                }
            case 15://hashcode
                {
                    dc->hashcode = spx_string_dup(*(fids + count),&(dc->err));
                    if(NULL == dc->hashcode){
                        SpxLog2(dc->log,SpxLogError,dc->err,\
                                "dup hashcode is fail.");
                    }
                    break;
                }
            case 16://suffix
                {
                    dc->suffix = spx_string_dup(((*(fids + count)) + 1),&(dc->err));//remove suffix sper "."
                    if(NULL != dc->suffix){
                        SpxLog2(dc->log,SpxLogError,dc->err,\
                                "dup suffix is fail.");
                    }
                    break;
                }
            default:{
                        break;
                    }
        }
    }

    return 0;
}/*}}}*/

struct spx_msg *ydb_storage_dio_make_metadata(SpxLogDelegate *log,
        bool_t isdelete,u32_t opver,u32_t ver,u64_t createtime,u64_t lastmodifytime,\
        u64_t totalsize,u64_t realsize,string_t suffix,string_t hashcode,err_t *err){/*{{{*/

    struct spx_msg *md = spx_msg_new(YDB_CHUNKFILE_MEMADATA_SIZE,err);

    if(NULL == md){
        SpxLog2(log,SpxLogError,*err,\
                "alloc md for chunkfile is fail.");
        return NULL;
    }

    if(isdelete){
        spx_msg_pack_true(md);
    }else {
        spx_msg_pack_false(md);//isdelete
    }
    spx_msg_pack_u32(md,opver);
    spx_msg_pack_u32(md,ver);
    spx_msg_pack_u64(md,createtime);
    spx_msg_pack_u64(md,lastmodifytime);
    spx_msg_pack_u64(md,totalsize);
    spx_msg_pack_u64(md,realsize);
    if(NULL == suffix){
        spx_msg_align(md,YDB_FILENAME_SUFFIX_SIZE);
    } else {
        spx_msg_pack_fixed_string(md,suffix,YDB_FILENAME_SUFFIX_SIZE);
    }

    if(NULL == hashcode){
        spx_msg_align(md,YDB_HASHCODE_SIZE);
    } else {
        spx_msg_pack_fixed_string(md,hashcode,YDB_HASHCODE_SIZE);
    }
    return md;
}/*}}}*/

err_t ydb_storage_dio_parser_metadata(SpxLogDelegate *log,struct spx_msg *md,\
        bool_t *isdelete,u32_t *opver,u32_t *ver,u64_t *createtime,u64_t *lastmodify,\
        u64_t *totalsize,u64_t *realsize,string_t *suffix,string_t *hashcode){
    err_t err = 0;
    *isdelete = spx_msg_unpack_bool(md);
    *opver = spx_msg_unpack_u32(md);
    *ver = spx_msg_unpack_u32(md);
    *createtime = spx_msg_unpack_u64(md);
    *lastmodify = spx_msg_unpack_u64(md);
    *totalsize = spx_msg_unpack_u64(md);
    *realsize = spx_msg_unpack_u64(md);
    *suffix = spx_msg_unpack_string(md,YDB_FILENAME_SUFFIX_SIZE,&err);
    if(0 != err){
        SpxLog2(log,SpxLogError,err,\
                "parser suffix is fail.");
        return err;
    }
    *hashcode = spx_msg_unpack_string(md,YDB_HASHCODE_SIZE,&err);
    if(0 != err){
        SpxLog2(log,SpxLogError,err,\
                "parser hashcode is fail.");
        return err;
    }
    return err;
}

