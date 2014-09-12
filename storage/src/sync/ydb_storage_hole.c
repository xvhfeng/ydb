/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_hole.c
 *        Created:  2014/07/15 10时55分30秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "include/spx_types.h"
#include "include/spx_properties.h"
#include "include/spx_defs.h"
#include "include/spx_io.h"
#include "include/spx_time.h"
#include "include/spx_path.h"
#include "include/spx_list.h"
#include "include/spx_string.h"
#include "include/spx_skiplist.h"
#include "include/spx_collection.h"
#include "include/spx_alloc.h"
#include "include/spx_message.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_hole.h"

struct spx_skiplist *hole_idx = NULL;
spx_private struct ev_loop *loop = NULL;

spx_private string_t ydb_storage_hole_idx_filename(\
        struct ydb_storage_configurtion *c,err_t *err){

    string_t filename = spx_string_newlen(NULL,SpxPathSize,err);
    if(NULL  == filename){
        SpxLog2(c->log,SpxLogError,*err,\
                "alloc hole idx filename is fail.");
        return NULL;
    }

    string_t new_filename = NULL;

    if(SpxStringEndWith(c->basepath,SpxPathDlmt)){
        new_filename = spx_string_cat_printf(err,filename,\
                "%s%s-ydb-storag-hole.idx",c->basepath,c->machineid);
    } else {
        new_filename = spx_string_cat_printf(err,filename,\
                "%s/%s-ydb-storage-hole.idx",c->basepath,c->machineid);
    }
    if(NULL == new_filename){
        spx_string_free(filename);
    }
    return new_filename;
}


spx_private err_t ydb_storage_hole_idx_vfree(void **v){
}

spx_private int skiplist_range_cmper(void *k1,u64_t l1,\
        void *k2,u64_t l2){
    u64_t *i1 = (u64_t *) k1;
    u64_t *i2 = (u64_t *) k2;
    u64_t r = *i2 - *i1;
    u64_t min = 1;
    u64_t max = min + 3;
    if( min <= r && max >= r) return 0;
    if( min > r) return 1;
    else return -1;

}


err_t ydb_storage_recover_hold_idx(struct ydb_storage_configurtion *c){
    err_t err = 0;
    string_t filename = ydb_storage_hole_idx_filename(c,&err);
    if(NULL == filename){
        SpxLog2(c->log,SpxLogError,err,\
                "get hole idx filename is fail.");
        return err;
    }

    struct spx_msg *ctx = spx_msg_new(YDB_STORAGE_HOLE_IDX_NODE_LENGTH,&err);
    if(NULL == ctx){
        SpxLog2(c->log,SpxLogError,err,\
                "alloc msg ctx for hole idx file is fail.");
        goto r1;
    }

    hole_idx = spx_skiplist_new(c->log,\
            SPX_SKIPLIST_IDX_U64,\
            SPX_SKIPLIST_LEVEL_DEFAULT,\
            true,\
            spx_collection_u64_default_cmper,\
            NULL,\
            spx_collection_u64_default_printf,\
            NULL,\
            ydb_storage_hole_idx_vfree,\
            &err);
    if(NULL == hole_idx){
        SpxLog2(c->log,SpxLogError,err,\
                "alloc the hole idx by skiplist is fail.");
        goto r2;
    }

    if(!SpxFileExist(filename)){
        goto r2;
    }

    struct stat buf;
    SpxZero(buf);
    stat(filename,&buf);
    u64_t filesize = buf.st_size;

    int fd = 0;
    fd = open(filename,O_RDWR);
    if(0 >= fd){
        err = errno;
        SpxLog2(c->log,SpxLogError,err,\
                "open the hole idx is fail.");
        goto r2;
    }

    ubyte_t *ptr =(ubyte_t *) mmap(NULL,filesize,PROT_READ,MAP_PRIVATE,fd,0);
    if(NULL == ptr){
        err = errno;
        SpxLog2(c->log,SpxLogError,err,\
                "mmap the hole idx file is fail.");
        goto r3;
    }
    u64_t numbs = 0;
    for( ; numbs <= filesize; numbs += YDB_STORAGE_HOLE_IDX_NODE_LENGTH){
        err =  spx_msg_pack_ubytes( ctx,ptr + numbs,YDB_STORAGE_HOLE_IDX_NODE_LENGTH);
        struct ydb_storage_hole_idx_node *n = \
                                              (struct ydb_storage_hole_idx_node *) \
                                              spx_alloc_alone(sizeof(*n),&err);
        if(NULL == n){
            SpxLog2(c->log,SpxLogWarn,err,\
                    "alloc the node for hole idx is fail.");
            break;
        }

        n->machineid = spx_msg_unpack_string(ctx,YDB_STORAGE_HOLE_IDX_NODE_LENGTH,&err);
        if(NULL == n->machineid){
            SpxLog2(c->log,SpxLogError,err,\
                    "unpack machineid from ctx is fail.");
            SpxFree(n);
            break;
        }

        n->mp_idx = spx_msg_unpack_u8(ctx);
        n->file_createtime = spx_msg_unpack_u64(ctx);
        n->dio_idx = spx_msg_unpack_u32(ctx);
        n->rand_number = spx_msg_unpack_i32(ctx);
        n->begin = spx_msg_unpack_u64(ctx);
        n->length = spx_msg_unpack_u64(ctx);

        spx_skiplist_insert(hole_idx,&(n->length),sizeof(&(n->length)),n,sizeof(n),0);
        spx_msg_clear(ctx);
    }

r3:
    SpxClose(fd);
r2:
    spx_msg_free(&ctx);
r1:
    if(NULL != filename){
        spx_string_free(filename);
    }
    return err;
}

err_t ydb_storage_hole_idx_writer(){

}

spx_private void ydb_storage_hole_idx_writer_handler(struct ev_loop *loop,\
        ev_timer *w,int revents){

}
