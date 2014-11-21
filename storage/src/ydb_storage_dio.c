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
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "spx_types.h"
#include "spx_defs.h"
#include "spx_string.h"
#include "spx_list.h"
#include "spx_path.h"
#include "spx_message.h"
#include "spx_atomic.h"
#include "spx_time.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_dio_context.h"
#include "ydb_storage_storefile.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_dio.h"

err_t ydb_storage_dio_mountpoint_init(struct ydb_storage_configurtion *c){/*{{{*/
    err_t err = 0;
    int i = 0;
    string_t path = spx_string_emptylen(SpxPathSize,&err);
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
                    path = newpath;
                    if(!spx_is_dir(path,&err)){
                        if(0 != (err = spx_mkdir(c->log,path,SpxPathMode))){
                            SpxLogFmt2(c->log,SpxLogError,err,
                                    "create store path:%s is fail.",
                                    path);
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
    SpxStringFree(path);
    return err;
}/*}}}*/


string_t ydb_storage_dio_make_filename(SpxLogDelegate *log,\
        bool_t issinglefile,
        struct spx_list *mps,u8_t mpidx,
        u8_t p1,u8_t p2,
        string_t machineid,u32_t tidx,\
        u64_t createtime, u32_t rand,string_t suffix,err_t *err){/*{{{*/
    string_t filename = spx_string_newlen(NULL,SpxPathSize,err);
    if(NULL == filename){
        SpxLog2(log,SpxLogError,*err,\
                "new filename is fail.");
        return NULL;
    }

    struct ydb_storage_mountpoint *mp = spx_list_get(mps,mpidx);
    if(issinglefile){
        if(SpxStringEndWith(mp->path,SpxPathDlmt)){
            spx_string_cat_printf(err,filename, "%sdata%s%02X%s%02X%s%s_%d_%lld_%d%s%s",
                    mp->path,SpxPathDlmtString,
                    p1,SpxPathDlmtString,p2,SpxPathDlmtString,
                    machineid,tidx,createtime,rand,
                    NULL == suffix ? "" : ".",
                    NULL == suffix ? "" : suffix);
        }else{
            spx_string_cat_printf(err,filename,"%s%sdata%s%02X%s%02X%s%s_%d_%lld_%d%s%s",
                    mp->path,SpxPathDlmtString,SpxPathDlmtString,
                    p1,SpxPathDlmtString,p2,SpxPathDlmtString,
                    machineid,tidx,createtime,rand,
                    NULL == suffix ? "" : ".",
                    NULL == suffix ? "" : suffix);
        }
    } else {
        if(SpxStringEndWith(mp->path,SpxPathDlmt)){
            spx_string_cat_printf(err,filename, "%sdata%s%02X%s%02X%s%s_%d_%lld_%d",
                    mp->path,SpxPathDlmtString,
                    p1,SpxPathDlmtString,p2,SpxPathDlmtString,
                    machineid,tidx,createtime,rand);
        }else{
            spx_string_cat_printf(err,filename,"%s%sdata%s%02X%s%02X%s%s_%d_%lld_%d",
                    mp->path,SpxPathDlmtString,SpxPathDlmtString,
                    p1,SpxPathDlmtString,p2,SpxPathDlmtString,
                    machineid,tidx,createtime,rand);
        }

    }
    return filename;
}/*}}}*/

string_t ydb_storage_dio_make_fileid(SpxLogDelegate *log,\
        string_t groupname,string_t machineid,string_t syncgroup,
        bool_t issinglefile,u8_t mpidx,u8_t p1,u8_t p2,
        u32_t tidx,u64_t fcreatetime,u32_t rand,u64_t begin,u64_t realsize,
        u64_t totalsize,u32_t ver,u32_t opver,u64_t lastmodifytime,
        string_t hashcode,bool_t has_suffix,string_t suffix,
        size_t *len,err_t *err){/*{{{*/

    string_t fid = spx_string_newlen(NULL,SpxFileNameSize,err);
    if(NULL == fid){
        SpxLog2(log,SpxLogError,*err,\
                "new file is fail.");
        return NULL;
    }
    spx_string_cat_printf(err,fid,\
            "%s:%s:%s:%d:%d:%d:%d:%d:%lld:%d:%lld:%lld:%lld:%d:%d:%lld:%s%s%s",
            groupname,machineid,syncgroup,issinglefile,mpidx,p1,p2,
            tidx,fcreatetime,rand,begin,realsize,totalsize,
            ver,opver,lastmodifytime,NULL == hashcode ? "" : hashcode,
            has_suffix ? ":." : "" , has_suffix ? suffix : "");

    *len = spx_string_len(fid);
    return fid;
}/*}}}*/

err_t ydb_storage_dio_parser_fileid(SpxLogDelegate *log,
        string_t fid,
        string_t *groupname,string_t *machineid,string_t *syncgroup,
        bool_t *issinglefile,u8_t *mpidx,u8_t *p1,u8_t *p2,
        u32_t *tidx,u64_t *fcreatetime,u32_t *rand,u64_t *begin,u64_t *realsize,
        u64_t *totalsize,u32_t *ver,u32_t *opver,u64_t *lastmodifytime,
        string_t *hashcode,bool_t *has_suffix,string_t *suffix){/*{{{*/
    err_t err = 0;
    int count = 0;
    string_t *fids = spx_string_splitlen(fid,\
            spx_string_len(fid),":",strlen(":"),\
            &count,&(err));
    if(NULL == fids || 0 == count){
        SpxLogFmt2(log,SpxLogError,err,\
                "split fileid:%s is fail.",fid);
        return err;
    }

    int i = 0;
    for( ; i < count; i++){
        switch (i){
            case 0://groupname
                {
                    *groupname = spx_string_dup(*(fids + i),&(err));
                    if(NULL == *groupname){
                        SpxLog2(log,SpxLogError,err,\
                                "dup groupname is fail.");
                        goto r1;
                    }
                    break;
                }
            case 1://machineid
                {
                    *machineid = spx_string_dup(*(fids + i),&(err));
                    if(NULL == *machineid){
                        SpxLog2(log,SpxLogError,err,\
                                "dup machineid is fail.");
                        goto r1;
                    }
                    break;
                }
            case 2://syncgroupname
                {
                    *syncgroup = spx_string_dup(*(fids + i),&(err));
                    if(NULL == *syncgroup){
                        SpxLog2(log,SpxLogError,err,\
                                "dup syncgroup is fail.");
                        goto r1;
                    }
                    //unuseful
                    break;
                }
            case 3://issignalfile
                {
                    *issinglefile = (bool_t) atoi(*(fids + i));
                    break;
                }
            case 4://mpidx
                {
                    *mpidx = (u8_t) atoi(*(fids + i));
                    break;
                }
            case 5://p1
                {
                    *p1 = (u8_t) atoi(*(fids + i));
                    break;
                }
            case 6://p2
                {
                    *p2 = (u8_t) atoi(*(fids + i));
                    break;
                }
            case 7://tidx
                {
                    *tidx = (u32_t) atol(*(fids + i));
                    break;
                }
            case 8://file_createtime
                {
                    *fcreatetime = (u64_t) strtoul(*(fids + i),NULL,10);
                    break;
                }
            case 9://rand
                {
                    *rand = (u32_t) atol(*(fids + i));
                    break;
                }
            case 10://begin
                {
                    *begin = (u64_t) strtoul(*(fids + i),NULL,10);
                    break;
                }
            case 11://realsize
                {
                    *realsize = (u64_t) strtoul(*(fids + i),NULL,10);
                    break;
                }
            case 12://totalsize
                {
                    *totalsize = (u64_t) strtoul(*(fids + i),NULL,10);
                    break;
                }
            case 13://ver
                {
                    *ver = (u32_t) atol(*(fids + i));
                    break;
                }
            case 14://opver
                {
                    *opver = (u32_t) atol(*(fids + i));
                    break;
                }
            case 15://lastmodifytime
                {
                    *lastmodifytime = (u64_t) strtoul(*(fids + i),NULL,10);
                    break;
                }
            case 16://hashcode
                {
                    if(!SpxStringIsEmpty(*(fids + i))){
                        *hashcode = spx_string_dup(*(fids + i),&(err));
                        if(NULL == *hashcode){
                            SpxLog2(log,SpxLogError,err,\
                                    "dup hashcode is fail.");
                            goto r1;
                        }
                    }
                    break;
                }
            case 17://suffix
                {
                    *suffix = spx_string_new(((*(fids + i)) + 1),&(err));//remove suffix sper "."
                    //                    *suffix = spx_string_dup(((*(fids + i)) + 1),&(*err));//remove suffix sper "."
                    if(NULL == *suffix){
                        SpxLog2(log,SpxLogError,err,\
                                "dup suffix is fail.");
                        goto r1;
                    }
                    *has_suffix = true;
                    break;
                }
            default:{
                        break;
                    }
        }
    }

    return 0;
r1:
    if(NULL != *groupname){
        SpxStringFree(*groupname);
    }
    if(NULL != *machineid){
        SpxStringFree(*machineid);
    }
    if(NULL != *syncgroup){
        SpxStringFree(*syncgroup);
    }
    if(NULL != *hashcode){
        SpxStringFree(*hashcode);
    }
    if(NULL != *suffix){
        SpxStringFree(*suffix);
    }
    return err;
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
        u64_t *totalsize,u64_t *realsize,string_t *suffix,string_t *hashcode){/*{{{*/
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
}/*}}}*/


err_t ydb_storage_dio_get_path(
        struct ydb_storage_configurtion *c,
        struct ydb_storage_runtime *rt,
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
                    *mpidx = ydb_storage_get_mountpoint_with_maxsize(
                            c,rt,&err);
                    if(0 != err){
                        return err;
                    }
                    break;
                }
            case YDB_STORAGE_MOUNTPOINT_MASTER:
                {
                    *mpidx = ydb_storage_get_mountpoint_with_master(
                            c,rt,&err);
                    if(0 != err){
                        return err;
                    }
                    break;
                }
            case YDB_STORAGE_MOUNTPOINT_LOOP:
            default:
                {
                    *mpidx = ydb_storage_get_mountpoint_with_loop(
                            c,rt,true,&err);
                    if(0 != err){
                        return err;
                    }
                    break;
                }
        }
    }/*}}}*/
    return err;
}/*}}}*/

u8_t ydb_storage_get_mountpoint_with_loop(
        struct ydb_storage_configurtion *c,\
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
        u64_t size = spx_mountpoint_availsize(mp->path,err);
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

u8_t ydb_storage_get_mountpoint_with_turn(
        struct ydb_storage_configurtion *c,\
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
        u64_t size = spx_mountpoint_availsize(mp->path,err);
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

u8_t ydb_storage_get_mountpoint_with_maxsize(
        struct ydb_storage_configurtion *c,\
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
        u64_t size = spx_mountpoint_availsize(mp->path,err);
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

u8_t ydb_storage_get_mountpoint_with_master(
        struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err){/*{{{*/
    struct ydb_storage_mountpoint *mp = NULL;
    mp = spx_list_get(c->mountpoints,c->master);
    if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)
            && c->freedisk < spx_mountpoint_availsize(mp->path,err)){
        return c->master;
    }
    return ydb_storage_get_mountpoint_with_loop(c,rt,false,err);
}/*}}}*/


err_t ydb_storage_upload_check_and_open_chunkfile(
        struct ydb_storage_configurtion *c,\
        struct ydb_storage_dio_context *dc,\
        struct ydb_storage_storefile *cf){/*{{{*/
    err_t err = 0;
    while(true) {
        if(0 == cf->chunkfile.fd){
            cf->chunkfile.fcreatetime = spx_now();
            int count = SpxAtomicIncr(&(
                        g_ydb_storage_runtime->chunkfile_count));
            SpxAtomicCas(&(g_ydb_storage_runtime->chunkfile_count),10000,0);
            cf->chunkfile.rand = count;
            ydb_storage_dio_get_path(c,g_ydb_storage_runtime,\
                    &(cf->chunkfile.mpidx),\
                    &(cf->chunkfile.p1),&(cf->chunkfile.p2));

            cf->chunkfile.filename = ydb_storage_dio_make_filename(\
                    dc->log,dc->issinglefile,c->mountpoints,
                    g_ydb_storage_runtime->mpidx,
                    cf->chunkfile.p1,cf->chunkfile.p2,
                    c->machineid,cf->tidx,cf->chunkfile.fcreatetime,
                    cf->chunkfile.rand,\
                    dc->suffix,&err);

            SpxLogFmt1(c->log,SpxLogDebug,"reopen :%s."
                    ,cf->chunkfile.filename);
            SpxAtomicVIncr(g_ydb_storage_runtime->storecount);

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
                SpxStringFree(cf->chunkfile.filename);
                break;
            }
            if(0 != (err = ftruncate(cf->chunkfile.fd,c->chunksize))){
                SpxLogFmt2(c->log,SpxLogError,err,\
                        "truncate chunkfile:%s to size:%lld is fail.",
                        cf->chunkfile.filename,c->chunksize);
                SpxStringFree(cf->chunkfile.filename);
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
                SpxStringFree(cf->chunkfile.filename);
                SpxClose(cf->chunkfile.fd);
                break;
            }
        }

        if(dc->totalsize + cf->chunkfile.offset  > c->chunksize){
            SpxLogFmt1(c->log,SpxLogDebug,
                    "total:%lld,chunksize:%lld,offset:%lld.",dc->totalsize,
                    c->chunksize,cf->chunkfile.offset);
            SpxLogFmt1(c->log,SpxLogDebug,"close the file %s",
                    cf->chunkfile.filename);
            munmap(cf->chunkfile.mptr,c->chunksize);
            cf->chunkfile.mptr = NULL;
            SpxClose(cf->chunkfile.fd);
            SpxStringFree(cf->chunkfile.filename);
            cf->chunkfile.offset = 0;
            continue;//reopen a new file
        }
        break;
    }
    SpxLog1(c->log,SpxLogDebug,"return");
    return err;
}/*}}}*/


