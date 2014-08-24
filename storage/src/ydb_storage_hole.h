/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_hole.h
 *        Created:  2014/07/15 10时55分33秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_HOLE_H_
#define _YDB_STORAGE_HOLE_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>

#include "include/spx_types.h"

#include "ydb_protocol.h"

extern struct spx_skiplist *hole_idx;

#define YDB_STORAGE_HOLE_IDX_NODE_LENGTH \
            YDB_MACHINEID_LEN + sizeof(u8_t) + sizeof(u32_t) + sizeof(u64_t) \
            + sizeof(i32_t) + sizeof(u64_t) + sizeof(u64_t)

    struct ydb_storage_hole_idx_node{
        string_t machineid;
        u8_t mp_idx;
        u32_t dio_idx;
        u64_t file_createtime;
        i32_t rand_number;
        u64_t begin;
        u64_t length;
    };

#ifdef __cplusplus
}
#endif
#endif
