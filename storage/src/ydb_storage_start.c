/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_start.c
 *        Created:  2014/07/08 16时16分43秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>

#include "include/spx_types.h"
#include "include/spx_properties.h"
#include "include/spx_defs.h"
#include "include/spx_io.h"
#include "include/spx_time.h"
#include "include/spx_path.h"
#include "include/spx_list.h"
#include "include/spx_string.h"

#include "ydb_storage_configurtion.h"


err_t ydb_storage_mountpoint_init(struct ydb_storage_configurtion *c){
    err_t err = 0;

    string_t path = spx_string_newlen(NULL,SpxStringRealSize(SpxPathSize),&err);
    if(NULL == path){
        return err;
    }
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_COUNT; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)){
            int out = 0;
            for( ; out < c->storerooms; out++){
                int in = 0;
                for( ; in < c->storerooms; in++){
                    string_t new_path = spx_string_cat_printf(&err,path,"%s%s%02X/%02X/",\
                            mp->path,SpxStringEndWith(mp->path,SpxPathDlmt) \
                            ? "" : SpxPathDlmtString,\
                            out,in);
                    if(NULL == new_path){
                        SpxLogFmt1(c->log,SpxLogError,\
                                "create new path is fail.path:%02X/&02X",\
                                out,in);
                        continue;
                    }
                    if(!spx_is_dir(new_path,&err)){
                        err = spx_mkdir(c->log,new_path,SpxPathMode);
                        SpxLogFmt2(c->log,SpxLogError,err,\
                                "mkdir %s is fail.",\
                                new_path);
                    }
                    spx_string_clear(path);
                }
            }
        }
    }
    spx_string_free(path);
    return err;
}


