/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_startpoint.c
 *        Created:  2014/07/10 00时07分54秒
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

#include "ydb_storage_configurtion.h"

err_t ydb_storage_startpoint_load(SpxLogDelegate *log,struct spx_properties *p){
    err_t err = 0;
    string_t basepath = NULL;
    string_t groupname = NULL;
    string_t machineid = NULL;
    err = spx_properties_get(p,ydb_storage_config_basepath_key,(void **) &basepath,NULL);
    if(SpxStringIsNullOrEmpty(basepath)){
        SpxLog2(log,SpxLogError,err,"get the basepath is empty.");
        return err;
    }

    err = spx_properties_get(p,ydb_storage_config_groupname_key,(void **) &groupname,NULL);
    if(SpxStringIsNullOrEmpty(groupname)){
        SpxLog2(log,SpxLogError,err,"get the groupname is empty.");
        return err;
    }

    err = spx_properties_get(p,ydb_storage_config_machineid_key,(void **) &machineid,NULL);
    if(SpxStringIsNullOrEmpty(machineid)){
        SpxLog2(log,SpxLogError,err,"get the machineid is empty.");
        return err;
    }


    string_t new_basepath = spx_string_dup(basepath,&err);
    if(NULL == new_basepath){
        SpxLog2(log,SpxLogError,err,"dup the basepath is fail.");
        return err;
    }

    string_t filename = NULL;
    string_t line = NULL;
    if(SpxStringEndWith(new_basepath,SpxPathDlmt)){
        filename = spx_string_cat_printf(&err,new_basepath,"%s%s-%s-ydb-storage.mid",new_basepath,groupname,machineid);
    } else {
        filename = spx_string_cat_printf(&err,new_basepath,"%s/%s-%s-ydb-storage.mid",new_basepath,groupname,machineid);
    }
    if(NULL == filename){
        SpxLog2(log,SpxLogError,err,"get storage mid filename is fail.");
        goto r1;
    }

    if(SpxFileExist(filename)){
        FILE *fp = fopen(filename,"a+");
        if(NULL == fp) {
            err = errno;
            SpxLogFmt2(log,SpxLogError,err,"open the mid file is fail.filename:&s.",filename);
            goto r1;
        }
        line = spx_string_newlen(NULL,SpxStringRealSize(SpxLineSize),&err);
        if(NULL == line){
            fclose(fp);
            SpxLog2(log,SpxLogError,err,"alloc string for mid file line is fail.");
            goto r1;
        }
        line = fgets(line,SpxLineSize,fp);
        if(NULL == line){
            ydb_storage_first_start = spx_now();
            string_t time = spx_string_from_i64(ydb_storage_first_start,&err);
            if(NULL == time){
                SpxLog2(log,SpxLogError,err,"convect u64_t to string is fail.");
                fclose(fp);
                goto r1;
            }
            fwrite(time,sizeof(char),spx_string_len(time),fp);
            spx_string_free(time);
        } else {
            ydb_storage_first_start = (u64_t)  strtoul(line,NULL,10);
        }
        fclose(fp);
    } else {
        FILE *fp = fopen(filename,"a+");
        if(NULL == fp) {
            err = errno;
            SpxLogFmt2(log,SpxLogError,err,"create and open the mid file is fail.filename:&s.",filename);
            goto r1;
        }
        ydb_storage_first_start = spx_now();
        string_t time = spx_string_from_i64(ydb_storage_first_start,&err);
        if(NULL == time){
            SpxLog2(log,SpxLogError,err,"convect u64_t to string is fail.");
            fclose(fp);
            goto r1;
        }
        fwrite(time,sizeof(char),spx_string_len(time),fp);
        spx_string_free(time);
        fclose(fp);
    }
r1:
    spx_string_free(new_basepath);
    spx_string_free(line);
    return err;
}


err_t ydb_storage_startpoint_reset(SpxLogDelegate *log,struct spx_properties *p){
    err_t err = 0;
    string_t basepath = NULL;
    string_t groupname = NULL;
    string_t machineid = NULL;
    err = spx_properties_get(p,ydb_storage_config_basepath_key,(void **) &basepath,NULL);
    if(SpxStringIsNullOrEmpty(basepath)){
        SpxLog2(log,SpxLogError,err,"get the basepath is empty.");
        return err;
    }

    err = spx_properties_get(p,ydb_storage_config_groupname_key,(void **) &groupname,NULL);
    if(SpxStringIsNullOrEmpty(groupname)){
        SpxLog2(log,SpxLogError,err,"get the groupname is empty.");
        return err;
    }

    err = spx_properties_get(p,ydb_storage_config_machineid_key,(void **) &machineid,NULL);
    if(SpxStringIsNullOrEmpty(machineid)){
        SpxLog2(log,SpxLogError,err,"get the machineid is empty.");
        return err;
    }


    string_t new_basepath = spx_string_dup(basepath,&err);
    if(NULL == new_basepath){
        SpxLog2(log,SpxLogError,err,"dup the basepath is fail.");
        return err;
    }

    if(!spx_is_dir(new_basepath,&err)){
        if(0 != (err = spx_mkdir(log,new_basepath,SpxPathMode))){
            SpxLogFmt2(log,SpxLogError,err,"mkdir dir:%s is fail.",new_basepath);
            goto r1;
        }
    }
    string_t filename = NULL;
    if(SpxStringEndWith(new_basepath,SpxPathDlmt)){
        filename = spx_string_cat_printf(&err,new_basepath,"%s%s-%s-ydb-storage.mid",new_basepath,groupname,machineid);
    } else {
        filename = spx_string_cat_printf(&err,new_basepath,"%s/%s-%s-ydb-storage.mid",new_basepath,groupname,machineid);
    }
    if(NULL == filename){
        SpxLog2(log,SpxLogError,err,"get storage mid filename is fail.");
        goto r1;
    }

    FILE *fp = fopen(filename,"w+");
    if(NULL == fp) {
        err = errno;
        SpxLogFmt2(log,SpxLogError,err,"create and open the mid file is fail.filename:&s.",filename);
        goto r1;
    }
    ydb_storage_first_start = spx_now();
    string_t time = spx_string_from_i64(ydb_storage_first_start,&err);
    if(NULL == time){
        SpxLog2(log,SpxLogError,err,"convect u64_t to string is fail.");
        fclose(fp);
        goto r1;
    }
    fwrite(time,sizeof(char),spx_string_len(time),fp);
    spx_string_free(time);
    fclose(fp);
r1:
    spx_string_free(new_basepath);
    return err;
}
