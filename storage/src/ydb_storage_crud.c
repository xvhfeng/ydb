/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_crud.c
 *        Created:  2014/07/14 14时39分56秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>


#include "include/spx_types.h"
#include "include/spx_nio_context.h"
#include "include/spx_message.h"
#include "include/spx_io.h"

#include "ydb_storage_configurtion.h"

err_t ydb_storage_upload(int fd,struct spx_nio_context *nio_context){
    //body buffer:
    //1:have suffix-is_having_suffix + suffix length + context
    //2:have no suffix-is_having_suffix + context
    err_t err = 0;

    return err;
}

err_t ydb_storage_delete(int fd,struct spx_nio_context *nio_context){
    //body buffer:file-id
    err_t err = 0;

    return err;
}

err_t ydb_storage_modify(int fd,struct spx_nio_context *nio_context){
    //body buffer:
    //1:have suffix-is_having_suffix + suffix length + old file-id + context
    //2:have no suffix-is_having_suffix + old file-id + context

    err_t err = 0;

    return err;
}

err_t ydb_storage_find(int fd,struct spx_nio_context *nio_context){
    //body buffer:file-id
    err_t err = 0;

    return err;
}


