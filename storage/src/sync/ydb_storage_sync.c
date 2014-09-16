/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_sync.c
 *        Created:  2014/08/14 15时46分46秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>

#include "include/spx_types.h"
#include "include/spx_defs.h"
#include "include/spx_string.h"
#include "include/spx_path.h"
#include "include/spx_map.h"
#include "include/spx_alloc.h"
#include "include/spx_collection.h"
#include "include/spx_io.h"
#include "include/spx_time.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_sync.h"

struct spx_map *g_ydb_storage_slaves = NULL;


spx_private err_t  ydb_storage_key_free(void **arg){
    if(NULL != *arg){
        string_t s = (string_t ) *arg;
        spx_string_free(s);
    }
    return 0;
}

spx_private err_t ydb_storage_free(void **arg){
    if(NULL != *arg){
        struct ydb_storage_slave **s = (struct ydb_storage_slave **) arg;
        if(NULL != (*s)->machineid){
            spx_string_free((*s)->machineid);
        }
        if(NULL != (*s)->host.ip){
            spx_string_free((*s)->host.ip);
        }
        SpxFree(*s);
    }
    return 0;
}


err_t ydb_storage_sync_init(struct ydb_storage_configurtion *c){
    err_t err = 0;
    string_t filename = NULL;
    FILE *fp = NULL;
    string_t line = NULL;

    g_ydb_storage_slaves = spx_map_new(c->log,
            spx_pjw,
            spx_collection_string_default_cmper,
            NULL,
            ydb_storage_key_free,
            ydb_storage_free,
            &err);

    if(NULL == g_ydb_storage_slaves){
        SpxLog2(c->log,SpxLogError,err,
                "new map for storage slaves is fail.");
        goto r1;
    }

    filename = ydb_storage_make_marklog_filename(c,&err);
    if(NULL == filename){
        SpxLog2(c->log,SpxLogError,err,\
                "make makelog filename is fail.");
        return err;
    }
    if(!SpxFileExist(filename)){
        SpxLogFmt1(c->log,SpxLogWarn,
                "marklog file:%s is not exist/",
                filename);
        goto r1;
    }
    line = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == line){
        SpxLog2(c->log,SpxLogError,err,
                "alloc line for marklog is fail.");
        goto r1;
    }

    fp = fopen(filename,"r");
    if(NULL == fp){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "open marklog:%s is fail.",
                filename);
        goto r1;
    }
    //line format machineid:ip:port:read-binlog-idx:offset:timespan
    while(NULL != (fgets(line,SpxLineSize,fp))){
        spx_string_updatelen(line);
        if(SpxStringBeginWith(line,'#')){
            continue;
        }
        int count = 0;
        string_t *split_line = spx_string_split(line,":",sizeof(":"),&count,&err);
        if(NULL == split_line){
            SpxLogFmt2(c->log,SpxLogError,err,\
                    "split line:%s is fail.",
                    line);
            continue;
        }
        int i = 0;
        struct ydb_storage_slave *s = NULL;
        for( ; i < count; i++){
            string_t context = *(split_line + i);
            switch (i){
                case 0:
                    {
                        if(SpxStringIsNullOrEmpty(context)){
                            SpxLogFmt1(c->log,SpxLogError,
                                    "machineid from line:%s is null or empty",
                                    line);
                            spx_string_free_splitres(split_line,count);
                            goto r1;
                        }
                        spx_map_get(g_ydb_storage_slaves,context,spx_string_len(context),(void **) &s,NULL);
                        if(NULL == s){
                            s = spx_alloc_alone(sizeof(*s),&err);
                            if(NULL == s){
                                SpxLog2(c->log,SpxLogError,err,
                                        "new slave storage is fail.");
                                spx_string_free_splitres(split_line,count);
                                goto r1;
                            }
                            s->c = c;
                            s->machineid = spx_string_dup(context,&err);
                            if(NULL == s->machineid){
                                SpxLogFmt2(c->log,SpxLogError,err,
                                        "dup machineid:%s from line:%s is fail.",
                                        context,line);
                                spx_string_free_splitres(split_line,count);
                                SpxFree(s);
                                goto r1;
                            }
                            spx_map_insert(g_ydb_storage_slaves,\
                                    s->machineid,spx_string_len(line),(void **) &s,sizeof(s));
                        }
                        break;
                    }
                case 1:
                    {
                        if(!SpxStringIsNullOrEmpty(context)){
                            s->host.ip = spx_string_dup(context,&err);
                            if(NULL == s->host.ip){
                                SpxLogFmt2(c->log,SpxLogError,err,
                                        "dup ip:%s from line:%s is fail.",
                                        context,line);
                                spx_string_free_splitres(split_line,count);
                                goto r1;
                            }
                        }
                        break;
                    }
                case 2:
                    {
                        if(!SpxStringIsNullOrEmpty(context)){
                            s->host.port = atoi(context);
                        }
                        break;
                    }
                case 3:
                    {
                        if(!SpxStringIsNullOrEmpty(context)){
                            u64_t timespan = strtoul(context,NULL,10);
                            spx_get_date((time_t *) &timespan,&(s->read_binlog_date));
                        }
                        break;
                    }
                case 4:
                    {
                        if(!SpxStringIsNullOrEmpty(context)){
                            s->read_binlog_offset = atol(context);
                        }
                        break;
                    }
                case 5:
                    {
                        if(!SpxStringIsNullOrEmpty(context)){
                            s->timespan = atol(context);
                        }
                        break;
                    }
                default:{
                            break;
                        }
            }
        }
        spx_string_clear(line);
        spx_string_free_splitres(split_line,count);
    }

r1:
    spx_string_free(filename);
    if(NULL != fp){
        fclose(fp);
        fp = NULL;
    }
    spx_string_free(line);
    return err;
}

string_t ydb_storage_make_marklog_filename(struct ydb_storage_configurtion *c,err_t *err){
    string_t filename = spx_string_newlen(NULL,SpxFileNameSize,err);
    if(NULL == filename){
        SpxLog2(c->log,SpxLogError,*err,
                "alloc marklog filename is fail.");
        return NULL;
    }
    string_t filename_new = spx_string_cat_printf(err,filename,"%s%ssync/%s.marklog",
            c->basepath,SpxStringEndWith(c->basepath,SpxPathDlmt) ? "" : SpxPathDlmtString,
            c->machineid);
    if(NULL == filename_new){
        SpxLog2(c->log,SpxLogError,*err,
                "print marklog filename is fail.");
        return NULL;
    }
    return filename_new;
}

err_t ydb_storage_marklog_flush(struct ydb_storage_configurtion *c){
    if(NULL == g_ydb_storage_slaves || 0 == g_ydb_storage_slaves->numbs){
        return 0;
    }

    err_t err = 0;
    struct spx_map_iter *iter = NULL;
    string_t context = NULL;
    string_t filename = NULL;

    context = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == context){
        SpxLog2(c->log,SpxLogError,err,
                "new context for marklog file is fail.");
        return err;
    }

    spx_string_cat(context,
            "#format machineid:ip:port:read-binlog-idx:"
            "read-binlog-offset:lastmodify-timespan \n",
            &err);
    if(0 != err){
        SpxLog2(c->log,SpxLogError,err,
                "cat format is fail.");
        goto r1;
    }
    iter = spx_map_iter_new(g_ydb_storage_slaves,&err);
    if(NULL == iter || 0 != err){
        SpxLog2(c->log,SpxLogError,err,
                "new iter for slave storage is fail.");
        goto r1;
    }
    struct ydb_storage_slave *slave = NULL;
    struct spx_map_node *n = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&err))){
        if(NULL == (slave = n->v)){
            continue;
        }
        string_t new_ctx = spx_string_cat_printf(&err,context,"%s:%s:%d:%ulld:%ulld:%ulld\n",
                slave->machineid,slave->host.ip,slave->host.port,
                slave->read_binlog_date,slave->read_binlog_offset,
                slave->timespan);
        if(NULL == new_ctx){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "cat slave:%s is fail.",
                    slave->machineid);
            break;
        }
        context = new_ctx;
    }
    spx_map_iter_free(&iter);
    if(0 != err) goto r1;

    filename = ydb_storage_make_marklog_filename(c,&err);
    if(NULL == filename){
        SpxLog2(c->log,SpxLogError,err,\
                "make makelog filename is fail.");
        return err;
    }

    FILE *fp = fopen(filename,"a");
    if(NULL == fp){
        err = errno;
        SpxLogFmt2(c->log,SpxLogError,err,
                "open marklog:%s is fail.",
                filename);
        goto r1;
    }

    size_t len = 0;
    err = spx_fwrite_string(fp,context,spx_string_len(context),&len);
    if(0 != err || len != spx_string_len(context)){
        SpxLogFmt2(c->log,SpxLogError,err,
                "write the marklog is fail.context:%s.",
                context);
    }
    fclose(fp);

r1:
    spx_string_free(context);
    spx_string_free(filename);
    return err;
}
