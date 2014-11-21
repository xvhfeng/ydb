/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_dio.h
 *        Created:  2014/08/14 17时27分46秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#ifndef _YDB_STORAGE_DIO_H_
#define _YDB_STORAGE_DIO_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>

#include "spx_types.h"
#include "spx_string.h"
#include "spx_list.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_dio_context.h"
#include "ydb_storage_storefile.h"
#include "ydb_storage_runtime.h"


    struct ydb_storage_fid{
        bool_t issinglefile;
        string_t groupname;
        string_t machineid;
        string_t syncgroup;
        u8_t mpidx;
        u8_t p1;
        u8_t p2;
        u32_t tidx;
        u64_t fcreatetime;
        u32_t rand;
        u64_t begin;
        u64_t realsize;
        u64_t totalsize;
        u32_t ver;
        u32_t opver;
        u64_t lastmodifytime;
        string_t hashcode;
        bool_t has_suffix;
        string_t suffix;
    };

    struct ydb_storage_fname{
        string_t machineid;
        bool_t issinglefile;
        u8_t mpidx;
        u8_t p1;
        u8_t p2;
        u32_t tidx;
        u64_t createtime;
        u32_t rand;
        string_t suffix;
    };

    struct ydb_storage_metadata{
        bool_t isdelete;
        u32_t opver;
        u32_t ver;
        u64_t createtime;
        u64_t lastmodifytime;
        u64_t totalsize;
        u64_t realsize;
        string_t suffix;
        string_t hashcode;
    };

err_t ydb_storage_dio_mountpoint_init(struct ydb_storage_configurtion *c);

string_t ydb_storage_dio_make_filename(SpxLogDelegate *log,\
        bool_t issinglefile,
        struct spx_list *mps,u8_t mpidx,
        u8_t p1,u8_t p2,
        string_t machineid,u32_t tidx,\
        u64_t createtime, u32_t rand,string_t suffix,err_t *err);

string_t ydb_storage_dio_make_fileid(SpxLogDelegate *log,\
        string_t groupname,string_t machineid,string_t syncgroup,
        bool_t issinglefile,u8_t mpidx,u8_t p1,u8_t p2,
        u32_t tidx,u64_t fcreatetime,u32_t rand,u64_t begin,u64_t realsize,
        u64_t totalsize,u32_t ver,u32_t opver,u64_t lastmodifytime,
        string_t hashcode,bool_t has_suffix,string_t suffix,
        size_t *len,err_t *err);

err_t ydb_storage_dio_parser_fileid(SpxLogDelegate *log,
        string_t fid,
        string_t *groupname,string_t *machineid,string_t *syncgroup,
        bool_t *issinglefile,u8_t *mpidx,u8_t *p1,u8_t *p2,
        u32_t *tidx,u64_t *fcreatetime,u32_t *rand,u64_t *begin,u64_t *realsize,
        u64_t *totalsize,u32_t *ver,u32_t *opver,u64_t *lastmodifytime,
        string_t *hashcode,bool_t *has_suffix,string_t *suffix);

struct spx_msg *ydb_storage_dio_make_metadata(SpxLogDelegate *log,
        bool_t isdelete,u32_t opver,u32_t ver,u64_t createtime,u64_t lastmodifytime,\
        u64_t totalsize,u64_t realsize,string_t suffix,string_t hashcode,err_t *err);

err_t ydb_storage_dio_parser_metadata(SpxLogDelegate *log,struct spx_msg *md,\
        bool_t *isdelete,u32_t *opver,u32_t *ver,u64_t *createtime,u64_t *lastmodify,\
        u64_t *totalsize,u64_t *realsize,string_t *suffix,string_t *hashcode);

 err_t ydb_storage_dio_get_path(struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,\
        u8_t *mpidx,u8_t *pq,u8_t *p2);
 u8_t ydb_storage_get_mountpoint_with_loop(
        struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,bool_t isfirst,err_t *err);
 u8_t ydb_storage_get_mountpoint_with_turn(
        struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err);
 u8_t ydb_storage_get_mountpoint_with_maxsize(
        struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err);
 u8_t ydb_storage_get_mountpoint_with_master(
        struct ydb_storage_configurtion *c,\
        struct ydb_storage_runtime *rt,err_t *err);

err_t ydb_storage_upload_check_and_open_chunkfile(
        struct ydb_storage_configurtion *c,\
        struct ydb_storage_dio_context *dc,\
        struct ydb_storage_storefile *cf);



#ifdef __cplusplus
}
#endif
#endif
