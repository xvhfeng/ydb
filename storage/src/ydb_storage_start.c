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
#include "spx_limits.h"

#include "ydb_storage_configurtion.h"

spx_private err_t ydb_storage_mountpoint_status(struct ydb_storage_configurtion *c,
        struct ydb_storage_mountpoint *mp);
spx_private err_t ydb_storage_mountpoint_initfile(struct ydb_storage_configurtion *c,
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

            if(0 != (err  = ydb_storage_mountpoint_initfile(c,mp))){
                break;
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

spx_private err_t ydb_storage_mountpoint_initfile(struct ydb_storage_configurtion *c,
        struct ydb_storage_mountpoint *mp){
    err_t err = 0;
    string_t new_initfile = NULL;
    string_t initfile = spx_string_newlen(NULL,SpxFileNameSize,&err)
    if(NULL == initfile){
        SpxLogFmt2(c->log,SpxLogError,err,
                "new initfile name for mountpoint:%s is fail.",
                mp->path);
        return err;
    }

    if(SpxStringEndWith(c->basepath,SpxPathDlmt)){
        new_initfile = spx_string_cat_printf(&err,initfile,
                "%s.%s-%s-mprtf.spx",c->basepath,
                c->groupname,c->machineid);
    } else {
        new_initfile = spx_string_cat_printf(&err,initfile,
                "%s%c.%s-%s-mprtf.spx",c->basepath,SpxPathDlmt,
                c->groupname,c->machineid);
    }
    if(NULL == new_initfile){
        SpxLogFmt2(c->log,SpxLogError,err,
                "make init filename for mountpoint is fail.",
                mp->path);
        goto r1;
    }
    initfile = new_initfile;
    if(SpxFileExist(initfile)){
        FILE *fp = fopen(initfile,"r");
        if(NULL == fp){
            err = 0 == errno ? EACCES : errno;
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open init file :%s is fail.",
                    intfile);
            goto r1;
        }
        char line[SpxU64MaxLength] = {0};
        if(NULL != fgets(line,SpxU64MaxLength,fp)){
            mp->init_timespan = atol(line);
        }
        fclose(fp);
    } else {
        mp->need_dsync = true;
        mp->init_timespan = c->start_timespan;
        char line[SpxU64MaxLength] = {0};
        sprintf(line,"%ld",mp->init_timespan);
        FILE *fp = fopen(initfile,"w");
        if(NULL == fp){
            err = 0 == errno ? EACCES : errno;
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open init file :%s is fail.",
                    intfile);
            goto r1;
        }
        fwrite(line,strlen(line),sizeof(char),fp);
        fclose(fp);
    }
r1:
    SpxStringFree(initfile);
    return err;
}

err_t ydb_storage_mountpoint_initfile_writer(struct ydb_storage_configurtion *c,
        struct ydb_storage_mountpoint *mp){
    err_t err = 0;
    string_t new_initfile = NULL;
    string_t initfile = spx_string_newlen(NULL,SpxFileNameSize,&err)
        if(NULL == initfile){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "new initfile name for mountpoint:%s is fail.",
                    mp->path);
            return err;
        }

    if(SpxStringEndWith(mp->path,SpxPathDlmt)){
        new_initfile = spx_string_cat_printf(&err,initfile,
                "%smp.initf",mp->path);
    } else {
        new_initfile = spx_string_cat_printf(&err,initfile,
                "%s.mp,initf",mp->path);
    }
    if(NULL == new_initfile){
        SpxLogFmt2(c->log,SpxLogError,err,
                "make init filename for mountpoint is fail.",
                mp->path);
        goto r1;
    }
    initfile = new_initfile;
    char line[SpxU64MaxLength] = {0};
    sprintf(line,"%ld",mp->init_timespan);
    FILE *fp = fopen(initfile,"w");
    if(NULL == fp){
        err = 0 == errno ? EACCES : errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "open init file :%s is fail.",
                intfile);
        goto r1;
    }
    fwrite(line,strlen(line),sizeof(char),fp);
    fclose(fp);
r1:
    SpxStringFree(initfile);
    return err;
}

