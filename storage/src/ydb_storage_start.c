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

#include "spx_types.h"
#include "spx_properties.h"
#include "spx_defs.h"
#include "spx_io.h"
#include "spx_time.h"
#include "spx_path.h"
#include "spx_list.h"
#include "spx_string.h"

#include "ydb_storage_configurtion.h"

spx_private err_t ydb_storage_mountpoint_status(struct ydb_storage_configurtion *c,
        struct ydb_storage_mountpoint *mp);

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
            if(!spx_is_dir(mp->path,&err)){
                err = spx_mkdir(c->log,mp->path,SpxPathMode);
                if(0 != err){
                    SpxLogFmt2(c->log,SpxLogError,err,\
                            "mkdir for mountpoint:%s is fail.",
                            mp->path);
                    break;
                }
            }

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


            if(0 != (err = ydb_storage_mountpoint_status(c,mp))){
                break;
            }
        }
    }
    SpxStringFree(path);
    return err;
}

spx_private err_t ydb_storage_mountpoint_status(struct ydb_storage_configurtion *c,
        struct ydb_storage_mountpoint *mp){
    err_t err = 0;
    mp->freesize = spx_mountpoint_availsize(mp->path,&err);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "mp:%s get freesize is fail.",
                mp->path);
        return err;
    }
    mp->disksize = spx_mountpoint_totalsize(mp->path,&err);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "mp:%s get disksize is fail.",
                mp->path);
        return err;
    }
    return err;
}


