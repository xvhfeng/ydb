/*
 * =====================================================================================
 *
 *       Filename:  ydb_storage_configurtion.c
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/07/01 15时40分40秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include <string.h>

#include "include/spx_types.h"
#include "include/spx_defs.h"
#include "include/spx_string.h"
#include "include/spx_properties.h"
#include "include/spx_alloc.h"
#include "include/spx_list.h"
#include "include/spx_path.h"
#include "include/spx_socket.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"

string_t ydb_storage_config_ip_key = NULL;
string_t ydb_storage_config_port_key = NULL;
string_t ydb_storage_config_timeout_key = NULL;
string_t ydb_storage_config_basepath_key = NULL;
string_t ydb_storage_config_logpath_key = NULL;
string_t ydb_storage_config_logprefix_key = NULL;
string_t ydb_storage_config_logsize_key = NULL;
string_t ydb_storage_config_loglevel_key = NULL;
string_t ydb_storage_config_balance_key = NULL;
string_t ydb_storage_config_master_key = NULL;
string_t ydb_storage_config_heartbeat_key = NULL;
string_t ydb_storage_config_daemon_key  = NULL;
string_t ydb_storage_config_niosize_key = NULL;
string_t ydb_storage_config_siosize_key = NULL;
string_t ydb_storage_config_stacksize_key = NULL;
string_t ydb_storage_config_groupname_key = NULL;
string_t ydb_storage_config_machineid_key = NULL;
string_t ydb_storage_config_diosize_key = NULL;
string_t ydb_storage_config_freedisk_key = NULL;
string_t ydb_storage_config_mountpoint_key = NULL;
string_t ydb_storage_config_tracker_key = NULL;
string_t ydb_storage_config_storepaths_key = NULL;
string_t ydb_storage_config_storemode_key = NULL;
string_t ydb_storage_config_storecount_key = NULL;
string_t ydb_storage_config_hole_key = NULL;
string_t ydb_storage_config_conpress_key = NULL;
string_t ydb_storage_config_chunkfile_key = NULL;
string_t ydb_storage_config_chunksize_key = NULL;
string_t ydb_storage_config_overload_key = NULL;
string_t ydb_storage_config_oversize_key = NULL;
 string_t ydb_storage_config_overmode_key = NULL;//this operator is not config and from the oversize

int ydb_storage_status = YDB_STORAGE_NORMAL;
u64_t ydb_storage_first_start = 0;

spx_private string_t ydb_storage_config_ip_default = NULL;
spx_private int ydb_storage_config_port_default = 8175;
spx_private int ydb_storage_config_timeout_default = 30;
spx_private string_t ydb_storage_config_basepath_default = NULL;
spx_private string_t ydb_storage_config_logpath_default = NULL;
spx_private string_t ydb_storage_config_logprefix_default = NULL;
spx_private u64_t ydb_storage_config_logsize_default = 10;
spx_private int ydb_storage_config_loglevel_default = SpxLogInfo;
spx_private int ydb_storage_config_balance_default = YDB_STORAGE_MOUNTPOINT_LOOP;
spx_private string_t ydb_storage_config_master_default = NULL;
spx_private int ydb_storage_config_heartbeat_default = 30;
spx_private bool_t ydb_storage_config_daemon_default = true;
spx_private int ydb_storage_config_niosize_default = 16;
spx_private int ydb_storage_config_siosize_default = 16;
spx_private u64_t ydb_storage_config_stacksize_default = 256;
spx_private string_t ydb_storage_config_groupname_default = NULL;
spx_private string_t ydb_storage_config_machineid_default = NULL;
spx_private int ydb_storage_config_diosize_default = 8;
spx_private u64_t ydb_storage_config_freedisk_default = 4;
spx_private struct spx_list *ydb_storage_config_mountpoints_default = NULL;
spx_private struct spx_vector *ydb_storage_config_trackers_default = NULL;
spx_private i32_t ydb_storage_config_storepaths_default = 256;
spx_private i32_t ydb_storage_config_storemode_default = YDB_STORAGE_STOREMODE_LOOP;
spx_private i32_t ydb_storage_config_storecount_default = 4096;
spx_private bool_t ydb_storage_config_hole_default = true;
spx_private bool_t ydb_storage_config_compress_default = false;
spx_private bool_t ydb_storage_config_chunkfile_default = true;
spx_private u64_t ydb_storage_config_chunksize_default = 64;
spx_private bool_t ydb_storage_config_overload_default = true;
spx_private u64_t ydb_storage_config_oversize_default = 10;
spx_private i32_t ydb_storage_config_overmode_default = YDB_STORAGE_OVERMODE_RELATIVE;


spx_private void *ydb_mountpoint_new(size_t i,void *arg,err_t *err);
spx_private err_t ydb_mountpoint_free(void **arg);
spx_private err_t ydb_tracker_free(void **arg);

spx_private void *ydb_mountpoint_new(size_t i,void *arg,err_t *err){
    struct ydb_storage_mouintpoint *mp = spx_alloc_alone(sizeof(*mp),err);
    if(NULL == mp){
        return NULL;
    }
    string_t path = (string_t) arg;
    mp->path = path;
    mp->freesize = spx_mountpoint_freesize(path,err);
    if(0 != *err){
        SpxFree(mp);
        return NULL;
    }
    mp->disksize = spx_mountpoint_size(path,err);
    if(0 != *err){
        SpxFree(mp);
        return NULL;
    }
    mp->idx = i;
    return mp;
}

spx_private err_t ydb_mountpoint_free(void **arg){
    if(NULL == arg || NULL == *arg){
        return 0;
    }
    struct ydb_storage_mountpoint **mp = (struct ydb_storage_mountpoint **)arg;
    SpxFree(*mp);
    return 0;
}

spx_private err_t ydb_tracker_free(void **arg){
    struct spx_host **host = (struct spx_host **) arg;
    SpxFree(*host);
    return 0;
}

err_t ydb_storage_config_parser_before_handle(struct spx_properties *p){
    err_t err = 0;
    ydb_storage_config_ip_default = spx_string_empty(&err);
    ydb_storage_config_port_default = 8175;
    ydb_storage_config_timeout_default = 30;
    ydb_storage_config_logpath_default = spx_string_new("/opt/ydb/log/storage/",&err);
    ydb_storage_config_logprefix_default = spx_string_new("ydb-storage",&err);
    ydb_storage_config_logsize_default = 10;
    ydb_storage_config_loglevel_default = SpxLogInfo;
    ydb_storage_config_balance_default = YDB_STORAGE_MOUNTPOINT_LOOP;
    ydb_storage_config_master_default = NULL;
    ydb_storage_config_heartbeat_default = 30;
    ydb_storage_config_daemon_default = true;
    ydb_storage_config_niosize_default = 16;
    ydb_storage_config_siosize_default = 16;
    ydb_storage_config_stacksize_default = 256 ;
    ydb_storage_config_groupname_default = NULL;
    ydb_storage_config_machineid_default = NULL;
    ydb_storage_config_diosize_default = 8;
    ydb_storage_config_freedisk_default = 4;
    if(NULL == ydb_storage_config_mountpoints_default){
        ydb_storage_config_mountpoints_default = spx_list_new(p->log,YDB_STORAGE_MOUNTPOINT_COUNT,ydb_mountpoint_free,&err);
    }
    if(NULL == ydb_storage_config_trackers_default) {
        ydb_storage_config_trackers_default = spx_vector_init(p->log,ydb_tracker_free,&err);
    }
    ydb_storage_config_storepaths_default = 256;
    ydb_storage_config_storemode_default = YDB_STORAGE_STOREMODE_LOOP;
    ydb_storage_config_storecount_default = 4096;
    ydb_storage_config_hole_default = true;
    ydb_storage_config_compress_default = false;
    ydb_storage_config_chunkfile_default = true;
    ydb_storage_config_chunksize_default = 64;
    ydb_storage_config_overload_default = true;
    ydb_storage_config_oversize_default = 10;
    ydb_storage_config_overmode_default = YDB_STORAGE_OVERMODE_RELATIVE;

    ydb_storage_config_ip_key = spx_string_new("ip",&err);
    ydb_storage_config_port_key = spx_string_new("port",&err);
    ydb_storage_config_timeout_key = spx_string_new("timeout",&err);
    ydb_storage_config_basepath_key = spx_string_new("basepath",&err);
    ydb_storage_config_logpath_key = spx_string_new("logpath",&err);
    ydb_storage_config_logprefix_key = spx_string_new("logprefix",&err);
    ydb_storage_config_logsize_key = spx_string_new("logsize",&err);
    ydb_storage_config_loglevel_key = spx_string_new("loglevel",&err);
    ydb_storage_config_balance_key = spx_string_new("balance",&err);
    ydb_storage_config_master_key = spx_string_new("master",&err);
    ydb_storage_config_heartbeat_key = spx_string_new("heartbeat",&err);
    ydb_storage_config_daemon_key = spx_string_new("daemon",&err);
    ydb_storage_config_siosize_key = spx_string_new("siosize",&err);
    ydb_storage_config_niosize_key = spx_string_new("niosize",&err);
    ydb_storage_config_stacksize_key = spx_string_new("stacksize",&err);
    ydb_storage_config_groupname_key = spx_string_new("groupname",&err);
    ydb_storage_config_machineid_key = spx_string_new("machineid",&err);
    ydb_storage_config_diosize_key = spx_string_new("diosize",&err);
    ydb_storage_config_freedisk_key = spx_string_new("freedisk",&err);
    ydb_storage_config_mountpoint_key = spx_string_new("mp",&err);
    ydb_storage_config_tracker_key = spx_string_new("tracker",&err);
    ydb_storage_config_storepaths_key = spx_string_new("storepaths",&err);
    ydb_storage_config_storemode_key = spx_string_new("storemode",&err);
    ydb_storage_config_storecount_key = spx_string_new("storecount",&err);
    ydb_storage_config_hole_key = spx_string_new("hole",&err);
    ydb_storage_config_compress_key = spx_string_new("compress",&err);
    ydb_storage_config_chunkfile_key = spx_string_new("chunkfile",&err);
    ydb_storage_config_chunksize_key = spx_string_new("chunksize",&err);
    ydb_storage_config_overload_key = spx_string_new("overload",&err);
    ydb_storage_config_oversize_key = spx_string_new("oversize",&err);
    ydb_storage_config_overmode_key = spx_string_new("overmode",&err);
    return err;
}



void ydb_storage_config_line_deserialize(string_t line,struct spx_properties *p,err_t *err){
    int count = 0;
    string_t *kv = spx_string_splitlen(line,spx_string_len(line),"=",sizeof("="),&count,err);
    if(NULL == kv){
        return;
    }

    spx_string_trim(*kv," ");
    if(2 == count){
        spx_string_trim(*(kv + 1)," ");
    }

    //ip
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_ip_key)){
        if(2 == count){
            *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        } else{
            SpxLog1(p->log,SpxLogWarn,"use the default ip.");
            spx_string_free_splitres(kv,count);
        }
        return;
    }

    //port
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_port_key)){
        if(2 != count){
            *err = EINVAL;
            SpxLog1(p->log,SpxLogError,"bad the configurtion item of port.the port is empty.");
            goto r1;
        }
        int *port = spx_alloc_alone(sizeof(int),err);
        if(NULL == port) goto r1;
        *port = strtol(*(kv + 1),NULL,10);
        if(ERANGE == *port) {
            SpxLog1(p->log,SpxLogError,"bad the configurtion item of port.");
            SpxFree(port);
            goto r1;
        }
        *err = spx_properties_set(p,ydb_storage_config_port_key, port,sizeof(int));
        if(0 != *err){
            SpxFree(port);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //timeout
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_timeout_key)){
        int *timeout = spx_alloc_alone(sizeof(int),err);
        if(NULL == timeout) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the timeout default is 30s.");
            *timeout = ydb_storage_config_timeout_default;
        } else {
            *timeout = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *timeout) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of timeout.");
                SpxFree(timeout);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_timeout_key, timeout,sizeof(int));
        if(0 != *err){
            SpxFree(timeout);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //daemon
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_daemon_key)){
        bool_t *is_daemon = spx_alloc_alone(sizeof(bool_t),err);
        if(NULL == is_daemon) goto r1;
        if(1 == count){
            *is_daemon = ydb_storage_config_daemon_default;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"false")){
                *is_daemon = false;
            } else if(0 == spx_string_casecmp(s,"true")){
                *is_daemon = true;
            } else {
                *is_daemon = false;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_daemon_key, is_daemon,sizeof(bool_t));
        if(0 != *err){
            SpxFree(is_daemon);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //stacksize
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_stacksize_key)){
        u64_t *size = spx_alloc_alone(sizeof(u64_t),err);
        if(NULL == size) goto r1;
        if(1 == count){
            *size = ydb_storage_config_stacksize_default * SpxKB;
        } else {
            *size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == *size) {
                SpxFree(size);
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),-2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                SpxFree(size);
                goto r1;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                *size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                *size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                *size *= SpxKB;
            }else {
                *size *= SpxMB;
            }
            spx_string_free(unit);
        }
        *err = spx_properties_set(p,ydb_storage_config_stacksize_key, size,sizeof(u64_t));
        if(0 != *err){
            SpxFree(size);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //niosize
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_niosize_key)){
        int *niosize = spx_alloc_alone(sizeof(int),err);
        if(NULL == niosize) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the niosize default is 16.");
            *niosize = ydb_storage_config_niosize_default;
        } else {
            *niosize = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *niosize) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of niosize.");
                SpxFree(niosize);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_niosize_key, niosize,sizeof(int));
        if(0 != *err){
            SpxFree(niosize);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //siosize
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_siosize_key)){
        int *siosize = spx_alloc_alone(sizeof(int),err);
        if(NULL == siosize) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the siosize default is 16.");
            *siosize = ydb_storage_config_siosize_default;
        } else {
            *siosize = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *siosize) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of siosize.");
                SpxFree(siosize);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_siosize_key, siosize,sizeof(int));
        if(0 != *err){
            SpxFree(siosize);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //heartbeat
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_heartbeat_key)){
        int *heartbeat = spx_alloc_alone(sizeof(int),err);
        if(NULL == heartbeat) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the heartbeat default is 30s.");
            *heartbeat = ydb_storage_config_heartbeat_default;
        } else {
            *heartbeat = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *heartbeat) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of heartbeat.");
                SpxFree(heartbeat);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_heartbeat_key, heartbeat,sizeof(int));
        if(0 != *err){
            SpxFree(heartbeat);
        }
        spx_string_free_splitres(kv,count);
        return;
    }
    //basepath
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_basepath_key)){
        if(1 == count){
            *err = EINVAL;
            SpxLog1(p->log,SpxLogError,"bad the configurtion item of basepath.and basepath is empty.");
            goto r1;
        }
        *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        return;
    }

    //logpath
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_logpath_key)){
        if(1 == count){
            *err = spx_properties_set(p,ydb_storage_config_logpath_key,\
                    ydb_storage_config_logpath_default,spx_string_len(ydb_storage_config_logpath_default));
            spx_string_free_splitres(kv,count);
        }else {
            *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        }
        return;
    }

    //logprefix
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_logprefix_key)){
        if( 1 == count){
            *err = spx_properties_set(p,ydb_storage_config_logprefix_key,\
                    ydb_storage_config_logprefix_default,spx_string_len(ydb_storage_config_logprefix_default));
            spx_string_free_splitres(kv,count);
        } else {
            *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        }
        return;
    }

    //logsize
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_logsize_key)){
        u64_t *size = spx_alloc_alone(sizeof(u64_t),err);
        if(NULL == size) goto r1;
        if(1 == count){
            *size = ydb_storage_config_logsize_default * SpxMB;
        } else {
            *size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == *size) {
                SpxFree(size);
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),-2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                SpxFree(size);
                goto r1;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                *size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                *size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                *size *= SpxKB;
            }else {
                *size *= SpxMB;
            }
            spx_string_free(unit);
        }
        *err = spx_properties_set(p,ydb_storage_config_logsize_key, size,sizeof(u64_t));
        if(0 != *err){
            SpxFree(size);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //loglevel
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_loglevel_key)){
        int *level = spx_alloc_alone(sizeof(int),err);
        if(NULL == level) goto r1;
        if(1 == count){
            *level = ydb_storage_config_loglevel_default;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"debug")){
                *level = SpxLogDebug;
            } else if(0 == spx_string_casecmp(s,"info")){
                *level = SpxLogInfo;
            }else if(0 == spx_string_casecmp(s,"warn")){
                *level = SpxLogWarn;
            }else if(0 == spx_string_casecmp(s,"error")){
                *level = SpxLogError;
            } else {
                *level = ydb_storage_config_loglevel_default;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_loglevel_key, level,sizeof(int));
        if(0 != *err){
            SpxFree(level);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //balance
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_balance_key)){
        int *balance = spx_alloc_alone(sizeof(int),err);
        if(NULL == balance) goto r1;
        if(1 == count){
            *balance = YDB_STORAGE_MOUNTPOINT_LOOP;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"loop")){
                *balance = YDB_STORAGE_MOUNTPOINT_LOOP;
            } else if(0 == spx_string_casecmp(s,"maxdisk")){
                *balance = YDB_STORAGE_MOUNTPOINT_MAXSIZE;
            }else if(0 == spx_string_casecmp(s,"turn")){
                *balance = YDB_STORAGE_MOUNTPOINT_TRUN;
            }else if(0 == spx_string_casecmp(s,"master")){
                *balance = YDB_STORAGE_MOUNTPOINT_MASTER;
            } else {
                *balance = YDB_STORAGE_MOUNTPOINT_LOOP;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_balance_key, balance,sizeof(int));
        if(0 != *err){
            SpxFree(balance);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //master
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_master_key)){
        if(1 == count){
            goto r1;
        }
        *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        return;
    }

    //groupname
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_groupname_key)){
        if(1 == count){
            *err = EINVAL;
            SpxLog1(p->log,SpxLogError,"bad the configurtion item of groupname.and groupname is empty.");
            goto r1;
        }
        *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        return;
    }

    //machineid
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_machineid_key)){
        if(1 == count){
            *err = EINVAL;
            SpxLog1(p->log,SpxLogError,"bad the configurtion item of machineid.and machineid is empty.");
            goto r1;
        }
        *err = spx_properties_set(p,*kv, *(kv + 1),spx_string_len((string_t) *(kv + 1)));
        return;
    }

    //diosize
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_diosize_key)){
        int *diosize = spx_alloc_alone(sizeof(int),err);
        if(NULL == diosize) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the diosize default is 16.");
            *diosize = ydb_storage_config_diosize_default;
        } else {
            *diosize = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *diosize) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of diosize.");
                SpxFree(diosize);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_diosize_key, diosize,sizeof(int));
        if(0 != *err){
            SpxFree(diosize);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //freedisk
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_freedisk_key)){
        u64_t *size = spx_alloc_alone(sizeof(u64_t),err);
        if(NULL == size) goto r1;
        if(1 == count){
            *size = ydb_storage_config_freedisk_default * SpxGB;
        } else {
            *size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == *size) {
                SpxFree(size);
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),-2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                SpxFree(size);
                goto r1;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                *size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                *size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                *size *= SpxKB;
            }else {
                *size *= SpxMB;
            }
            spx_string_free(unit);
        }
        *err = spx_properties_set(p,ydb_storage_config_freedisk_key, size,sizeof(u64_t));
        if(0 != *err){
            SpxFree(size);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //mount point
    if(spx_string_begin_casewith_string(*kv,ydb_storage_config_mountpoint_key)){
        int idx = strtol((*kv) + spx_string_len(ydb_storage_config_mountpoint_key),NULL,16);
        struct spx_list *mountpoints = NULL;
        spx_properties_get(p,ydb_storage_config_mountpoint_key,(void **) &mountpoints,NULL);
        if(NULL == mountpoints){
            *err = spx_properties_set(p,ydb_storage_config_mountpoint_key,\
                    ydb_storage_config_mountpoints_default,sizeof(ydb_storage_config_mountpoints_default));
            if(0 != *err){
                return;
            }
        }

        struct ydb_storage_mouintpoint *mp = (struct ydb_storage_mouintpoint *) ydb_mountpoint_new(idx,*(kv+1),err);
        if(NULL == mp){
            return;
        }
        spx_list_insert(ydb_storage_config_mountpoints_default,idx,mp);
        return;//memory overflow the kv is not free.it mostly 256
    }

    //tracker
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_tracker_key)){
        struct spx_vector *trackers = NULL;
        spx_properties_get(p,ydb_storage_config_tracker_key,(void **) &trackers,NULL);
        if(NULL == trackers){
            *err = spx_properties_set(p,ydb_storage_config_tracker_key,\
                    ydb_storage_config_trackers_default,sizeof(ydb_storage_config_trackers_default));
            if(0 != *err){
                return;
            }
        }
        int sum = 0;
        string_t *kv1 = spx_string_splitlen(*(kv + 1),spx_string_len(*(kv + 1)),":",sizeof(":"),&sum,err);
        struct spx_host *tracker = spx_alloc_alone(sizeof(*tracker),err);
        if(NULL == tracker){
            spx_string_free_splitres(kv1,sum);
            goto r1;
        }
        tracker->ip = *kv1;
        tracker->port = strtol(*(kv1 + 1),NULL,10);
        spx_vector_add(ydb_storage_config_trackers_default,tracker);
        return;//memory overflow the port string of the kv
    }

    //storepaths
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_storepaths_key)){
        i32_t *storepaths = spx_alloc_alone(sizeof(i32_t),err);
        if(NULL == storepaths) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the storepaths default is 256.");
            *storepaths = ydb_storage_config_storepaths_default;
        } else {
            *storepaths = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *storepaths) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of storepaths.");
                SpxFree(storepaths);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_storepaths_key, storepaths,sizeof(i32_t));
        if(0 != *err){
            SpxFree(storepaths);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //storemode
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_storemode_key)){
        i32_t *storemode = spx_alloc_alone(sizeof(i32_t),err);
        if(NULL == storemode) goto r1;
        if(1 == count){
            *storemode = YDB_STORAGE_STOREMODE_LOOP;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"loop")){
                *storemode = YDB_STORAGE_STOREMODE_LOOP;
            }else if(0 == spx_string_casecmp(s,"turn")){
                *storemode = YDB_STORAGE_STOREMODE_TURN;
            } else {
                *storemode = YDB_STORAGE_STOREMODE_LOOP;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_storemode_key, storemode,sizeof(int));
        if(0 != *err){
            SpxFree(storemode);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //storecount
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_storecount_key)){
        i32_t *storecount = spx_alloc_alone(sizeof(i32_t),err);
        if(NULL == storecount) goto r1;
        if(1 == count){
            SpxLog1(p->log,SpxLogInfo,"the storecount default is 4096.");
            *storecount = ydb_storage_config_storecount_default;
        } else {
            *storecount = strtol(*(kv + 1),NULL,10);
            if(ERANGE == *storecount) {
                SpxLog1(p->log,SpxLogError,"bad the configurtion item of storecount.");
                SpxFree(storecount);
                goto r1;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_storecount_key, storecount,sizeof(i32_t));
        if(0 != *err){
            SpxFree(storecount);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //hole
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_hole_key)){
        bool_t *is_hole = spx_alloc_alone(sizeof(bool_t),err);
        if(NULL == is_hole) goto r1;
        if(1 == count){
            *is_hole = ydb_storage_config_hole_default;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"false")){
                *is_hole = false;
            } else if(0 == spx_string_casecmp(s,"true")){
                *is_hole = true;
            } else {
                *is_hole = false;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_hole_key, is_hole,sizeof(bool_t));
        if(0 != *err){
            SpxFree(is_hole);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //compress
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_compress_key)){
        bool_t *is_compress = spx_alloc_alone(sizeof(bool_t),err);
        if(NULL == is_compress) goto r1;
        if(1 == count){
            *is_compress = ydb_storage_config_compress_default;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"false")){
                *is_compress = false;
            } else if(0 == spx_string_casecmp(s,"true")){
                *is_compress = true;
            } else {
                *is_compress = false;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_compress_key, is_compress,sizeof(bool_t));
        if(0 != *err){
            SpxFree(is_compress);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //chunkfile
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_chunkfile_key)){
        bool_t *is_chunkfile = spx_alloc_alone(sizeof(bool_t),err);
        if(NULL == is_chunkfile) goto r1;
        if(1 == count){
            *is_chunkfile = ydb_storage_config_chunkfile_default;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"false")){
                *is_chunkfile = false;
            } else if(0 == spx_string_casecmp(s,"true")){
                *is_chunkfile = true;
            } else {
                *is_chunkfile = false;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_chunkfile_key, is_chunkfile,sizeof(bool_t));
        if(0 != *err){
            SpxFree(is_chunkfile);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //chunksize
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_chunksize_key)){
        u64_t *size = spx_alloc_alone(sizeof(u64_t),err);
        if(NULL == size) goto r1;
        if(1 == count){
            *size = ydb_storage_config_chunksize_default * SpxMB;
        } else {
            *size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == *size) {
                SpxFree(size);
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),-2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                SpxFree(size);
                goto r1;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                *size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                *size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                *size *= SpxKB;
            }else {
                *size *= SpxMB;
            }
            spx_string_free(unit);
        }
        *err = spx_properties_set(p,ydb_storage_config_chunksize_key, size,sizeof(u64_t));
        if(0 != *err){
            SpxFree(size);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //overload
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_overload_key)){
        bool_t *is_overload = spx_alloc_alone(sizeof(bool_t),err);
        if(NULL == is_overload) goto r1;
        if(1 == count){
            *is_overload = ydb_storage_config_overload_default;
        } else {
            string_t s = *(kv + 1);
            if(0 == spx_string_casecmp(s,"false")){
                *is_overload = false;
            } else if(0 == spx_string_casecmp(s,"true")){
                *is_overload = true;
            } else {
                *is_overload = false;
            }
        }
        *err = spx_properties_set(p,ydb_storage_config_overload_key, is_overload,sizeof(bool_t));
        if(0 != *err){
            SpxFree(is_overload);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

    //oversize & overmode
    if(0 == spx_string_casecmp_string(*kv,ydb_storage_config_oversize_key)){
        u64_t *size = spx_alloc_alone(sizeof(u64_t),err);
        i32_t *mode = spx_alloc_alone(sizeof(i32_t),err);
        if(NULL == size ) goto r1;
        if(NULL == mode) {
            SpxFree(size);
            goto r1;
        }
        if(1 == count){
            *size = ydb_storage_config_oversize_default;
            *mode = ydb_storage_config_overmode_default;
        } else {
            if(SpxStringEndWith(*(kv + 1),'%')){
                *mode = YDB_STORAGE_OVERMODE_RELATIVE;
            } else {
                *mode = YDB_STORAGE_OVERMODE_ABSSOLUTE;
            }

            *size = strtoul(*(kv + 1),NULL,10);
            if(ERANGE == *size) {
                SpxFree(size);
                goto r1;
            }
            string_t unit = spx_string_range_new(*(kv + 1),-2,spx_string_len(*(kv + 1)),err);
            if(NULL == unit){
                SpxFree(size);
                goto r1;
            }
            if(0 == spx_string_casecmp(unit,"GB")){
                *size *= SpxGB;
            }
            else if( 0 == spx_string_casecmp(unit,"MB")){
                *size *= SpxMB;
            }else if(0 == spx_string_casecmp(unit,"KB")){
                *size *= SpxKB;
            }
            spx_string_free(unit);
        }
        *err = spx_properties_set(p,ydb_storage_config_oversize_key, size,sizeof(u64_t));
        *err = spx_properties_set(p,ydb_storage_config_overmode_key,mode,sizeof(i32_t));
        if(0 != *err){
            SpxFree(size);
        }
        spx_string_free_splitres(kv,count);
        return;
    }

r1:
    spx_string_free_splitres(kv,count);
    return;
}
