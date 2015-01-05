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
#include "spx_alloc.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_mountpoint.h"

spx_private err_t ydb_storage_mountpoint_getsize(struct ydb_storage_configurtion *c,
        struct ydb_storage_mountpoint *mp);
spx_private err_t ydb_storage_mountpoint_initfile(struct ydb_storage_configurtion *c,
        struct ydb_storage_mountpoint *mp);

spx_private err_t ydb_storage_mprtf_line_parser(struct ydb_storage_configurtion *c,
        string_t line);

err_t ydb_storage_mountpoint_init(struct ydb_storage_configurtion *c){/*{{{*/
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
                                "create new path is fail.path:%02X/%02X",\
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

            if(0 != (err = ydb_storage_mountpoint_getsize(c,mp))){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "get mp:%s disksize and freesize is fail.",
                        mp->path);
                break;
            }
        }
    }
    SpxStringFree(path);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_mountpoint_getsize(struct ydb_storage_configurtion *c,
        struct ydb_storage_mountpoint *mp){/*{{{*/
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
}/*}}}*/

/*
 * the line means a mountpoint
 * and the struct of line is
 * mpidx:no_disksync_force:init_time:last_modify_time:last_freesize:path
 * if you  delete the file in the offline,and you konw the things you do,
 * you maybe must set no-disksync-force to 1 to jump the step for disksync
 * the mprtf in the basepath and named by .groupname-machineid-mprtf.spx
 */
err_t ydb_storage_mprtf_reader(struct ydb_storage_configurtion *c){/*{{{*/
    err_t err = 0;
    string_t new_initfile = NULL;
    string_t line = NULL;
    string_t initfile = spx_string_newlen(NULL,SpxFileNameSize,&err);
        if(NULL == initfile){
            SpxLog2(c->log,SpxLogError,err,
                    "new initfile name for mountpoint is fail.");
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
        SpxLog2(c->log,SpxLogError,err,
                "make init filename for mountpoint:%s is fail.");
        goto r1;
    }
    initfile = new_initfile;
    line = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),&err);
    if(NULL == line){
        SpxLog2(c->log,SpxLogError,err,
                "new line for mprtf reader is fail.");
        goto r1;
    }
    if(SpxFileExist(initfile)){
        FILE *fp = fopen(initfile,"r");
        if(NULL == fp){
            err = 0 == errno ? EACCES : errno;
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open init file :%s is fail.",
                    initfile);
            goto r1;
        }

        while(NULL != (fgets(line,SpxLineSize,fp))){
            spx_string_updatelen(line);
            spx_string_strip_linefeed(line);
            if('#' == *line){
                spx_string_clear(line);
                continue;
            }
            if(0 != (err = ydb_storage_mprtf_line_parser(c,line))){
                fclose(fp);
                goto r1;
            }
            spx_string_clear(line);
        }
        fclose(fp);
    }
r1:
    SpxStringFree(initfile);
    SpxStringFree(line);
    return err;
}/*}}}*/

spx_private err_t ydb_storage_mprtf_line_parser(struct ydb_storage_configurtion *c,
        string_t line){/*{{{*/
    struct ydb_storage_mountpoint *mp = NULL;
    err_t err = 0;
    int count = 0;
    int spelen = strlen(":");
    string_t *context = spx_string_split(line,":",spelen,&count,&err);
    if(NULL == context){
        SpxLogFmt2(c->log,SpxLogError,err,
                "split mprtf line is fail."
                "line:%s.",
                line);
        return err;
    }

    int i = 0;
    for( ; i < count ; i++){
        switch (i){
            case 0:{
                       if(SpxStringIsNullOrEmpty(*(context + i))){
                           SpxLogFmt2(c->log,SpxLogError,err,
                                   "the mpidx is null or empty."
                                   "line:%s.",
                                   line);
                           goto r1;
                       }
                       int idx = atoi(*(context + i));
                       mp = spx_list_get(c->mountpoints,idx);
                       if(NULL == mp){
                           mp = (struct ydb_storage_mountpoint *) spx_alloc_alone(sizeof(*mp),&err);
                           if(NULL == mp){
                               SpxLogFmt2(c->log,SpxLogError,err,
                                       "new mp for idx:%d is fail.",
                                       idx);
                               goto r1;
                           }
                           mp->idx = idx;
                           mp->isusing = false;
                           spx_list_set(c->mountpoints,idx,mp);
                       }
                       break;
                   }
            case 1:{
                       if(SpxStringIsNullOrEmpty(*(context +i))){
                           SpxLogFmt2(c->log,SpxLogError,err,
                                   "the no_disksync_force operator "
                                   "for mp:%d is null or empty",
                                   mp->idx);
                           goto r1;
                       }
                       int op = atoi(*(context + i));
                       if(0 != op){
                           mp->dsync_force = true;
                       }
                       break;
                   }
            case 2:{
                       if(SpxStringIsNullOrEmpty(*(context +i))){
                           SpxLogFmt2(c->log,SpxLogError,err,
                                   "the init_timespan operator "
                                   "for mp:%d is null or empty",
                                   mp->idx);
                           goto r1;
                       }
                       u64_t op = atoll(*(context + i));
                       mp->init_timespan = op;
                       break;
                   }
            case 3:{
                       if(SpxStringIsNullOrEmpty(*(context +i))){
                           SpxLogFmt2(c->log,SpxLogError,err,
                                   "the last_modify_time operator "
                                   "for mp:%d is null or empty",
                                   mp->idx);
                           goto r1;
                       }
                       u64_t op = atoll(*(context + i));
                       SpxSSet(mp,last_modify_time,op);
//                       mp->last_modify_time = op;
                       break;
                   }
            case 4:{
                       if(SpxStringIsNullOrEmpty(*(context +i))){
                           SpxLogFmt2(c->log,SpxLogError,err,
                                   "the last_freesize operator "
                                   "for mp:%d is null or empty",
                                   mp->idx);
                           goto r1;
                       }
                       u64_t op = atoll(*(context + i));
                       mp->last_freesize = op;
                       break;
                   }
            case 5:{
                       if(SpxStringIsNullOrEmpty(*(context +i))){
                           SpxLogFmt2(c->log,SpxLogError,err,
                                   "the path operator "
                                   "for mp:%d is null or empty",
                                   mp->idx);
                           goto r1;
                       }
                       if(NULL == mp->path){
                           mp->path = spx_string_dup(*(context + i),&err);
                           if(NULL == mp->path){
                               SpxLogFmt2(c->log,SpxLogError,err,
                                       "dup mp path:%s tomp:%d is fail.",
                                       *(context +i),mp->idx);
                               goto r1;
                           }
                       }
                       break;
                   }
            default:{
                        SpxLogFmt1(c->log,SpxLogWarn,
                                "no the context(idx:%d) for mprtf line.",
                                i);
                        goto r1;
                        break;
                    }
        }
    }
r1:
    spx_string_free_splitres(context,count);
    return err;
}/*}}}*/


err_t ydb_storage_mprtf_writer(struct ydb_storage_configurtion *c){/*{{{*/
    err_t err = 0;
    string_t context = NULL;
    string_t new_context = NULL;
    string_t new_initfile = NULL;
    string_t initfile = spx_string_newlen(NULL,SpxFileNameSize,&err);
    if(NULL == initfile){
        SpxLog2(c->log,SpxLogError,err,
                "new initfile name for mountpoint:%s is fail.");
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
        SpxLog2(c->log,SpxLogError,err,
                "make init filename for mountpoint is fail.");
        goto r1;
    }
    initfile = new_initfile;

    int i = 0;
    context = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == context){
        SpxLog2(c->log,SpxLogError,err,
                "new context fot mprtf is fail.");
        goto r1;
    }

    new_context = spx_string_cat_printf(&err,context,"%s\n",
            "# mpidx:disksync_force:init_time:last_modify_time:last_freesize:path");
    if(NULL == new_context){
        SpxLog2(c->log,SpxLogError,err,
                "make context for mprtf is fail.");
        goto r1;
    } else {
        context = new_context;
        new_context = NULL;
    }

    // mpidx:disksync_force:init_time:last_modify_time:last_freesize:path
    for( ; i < YDB_STORAGE_MOUNTPOINT_MAXSIZE; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path)){
            new_context = spx_string_cat_printf(&err,context,
                    "%d:%d:%lld:%lld:%lld:%s%s",
                    mp->idx,0,mp->init_timespan,
                    mp->last_modify_time,mp->freesize,
                    mp->path,SpxLineEndDlmtString);
            if(NULL == new_context){
                SpxLogFmt2(c->log,SpxLogError,err,
                        "make context for mprtf is fail."
                        "mpidx:%d",
                        mp->idx);
                goto r1;
            } else {
                context = new_context;
                new_context = NULL;
            }
        }
    }

    size_t size = spx_string_len(context);
    size_t len = 0;
    FILE *fp = SpxFWriteOpen(initfile,true);
    if(NULL == fp){
        err = 0 == errno ? EACCES : errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "open init file :%s is fail.",
                initfile);
        goto r1;
    }
    err = spx_fwrite_string(fp,context,size,&len);
    if(size != len){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write mprtf is fail."
                "size:%ld,len:%ld",
                size,len);
    }
    fclose(fp);
r1:
    SpxStringFree(initfile);
    SpxStringFree(context);
    return err;
}/*}}}*/

