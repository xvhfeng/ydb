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

err_t ydb_storage_dio_mountpoint_init(struct ydb_storage_configurtion *c);

string_t ydb_storage_dio_make_filename(SpxLogDelegate *log,\
        bool_t issinglefile,
        struct spx_list *mps,u8_t mpidx,
        u8_t p1,u8_t p2,
        string_t machineid,u32_t tidx,\
        u64_t createtime, u32_t rand,string_t suffix,err_t *err);

string_t ydb_storage_dio_make_fileid(struct ydb_storage_configurtion *c,\
        struct ydb_storage_dio_context *dc,struct ydb_storage_storefile *cf,\
        size_t *len,err_t *err);

err_t ydb_storage_dio_parser_fileid(struct spx_msg* ctx,\
        size_t fid_len,struct ydb_storage_dio_context *dc);

struct spx_msg *ydb_storage_dio_make_metadata(SpxLogDelegate *log,
        bool_t isdelete,u32_t opver,u32_t ver,u64_t createtime,u64_t lastmodifytime,\
        u64_t totalsize,u64_t realsize,string_t suffix,string_t hashcode,err_t *err);

err_t ydb_storage_dio_parser_metadata(SpxLogDelegate *log,struct spx_msg *md,\
        bool_t *isdelete,u32_t *opver,u32_t *ver,u64_t *createtime,u64_t *lastmodify,\
        u64_t *totalsize,u64_t *realsize,string_t *suffix,string_t *hashcode);

#ifdef __cplusplus
}
#endif
#endif
