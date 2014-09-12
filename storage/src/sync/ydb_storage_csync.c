//                   _ooOoo_
//                  o8888888o
//                  88" . "88
//                  (| -_- |)
//                  O\  =  /O
//               ____/`---'\____
//             .'  \\|     |//  `.
//            /  \\|||  :  |||//  \
//           /  _||||| -:- |||||-  \
//           |   | \\\  -  /// |   |
//           | \_|  ''\---/''  |   |
//           \  .-\__  `-`  ___/-. /
//         ___`. .'  /--.--\  `. . __
//      ."" '<  `.___\_<|>_/___.'  >'"".
//     | | :  `- \`.;`\ _ /`;.`/ - ` : | |
//     \  \ `-.   \_ __\ /__ _/   .-` /  /
//======`-.____`-.___\_____/___.-`____.-'======
//                   `=---='
//^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//         佛祖保佑       永无BUG

/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_csync.c
 *        Created:  2014/08/29 14时06分33秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <ev.h>

#include "include/spx_socket.h"
#include "include/spx_types.h"
#include "include/spx_defs.h"
#include "include/spx_alloc.h"
#include "include/spx_threadpool.h"
#include "include/spx_time.h"
#include "include/spx_path.h"
#include "include/spx_io.h"
#include "include/spx_message.h"
#include "include/spx_socket.h"


#include "ydb_protocol.h"
#include "ydb_storage_sync.h"
#include "ydb_storage_csync.h"
#include "ydb_storage_binlog.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_dio.h"

spx_private ev_timer *csync_timer = NULL;
spx_private struct ev_loop *csyncloop = NULL;
spx_private struct spx_threadpool *sync_threadpool = NULL;

spx_private void *ydb_storage_csync();
spx_private void ydb_storage_csync_handler(struct ev_loop *loop,\
        ev_timer *w,int revents);
spx_private void ydb_storage_csync_do(void *arg);

spx_private void *ydb_storage_csync(){/*{{{*/
    ev_timer_init(csync_timer,ydb_storage_csync_handler,(double) 30,(double) 1);
    ev_run(csyncloop,0);
    return NULL;
}/*}}}*/

pthread_t ydb_storage_csync_start(struct ydb_storage_configurtion *c,
        err_t *err){/*{{{*/
    csyncloop = ev_loop_new(0);
    if(NULL == csyncloop){
        *err = errno;
        SpxLog2(c->log,SpxLogError,*err,\
                "new event loop for heartbeat is fail.");
    }
    csync_timer = spx_alloc_alone(sizeof(*csync_timer),err);
    if(NULL == csync_timer){
        SpxLog2(c->log,SpxLogError,*err,"alloc heartbeat timer is fail.");
    }
    sync_threadpool = spx_threadpool_new(c->log,c->sync_threads,c->stacksize,err);
    if(NULL == sync_threadpool){
        SpxLog2(c->log,SpxLogError,*err,
                "alloc sync threadpool is fail.");
    }
    pthread_t tid = 0;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t ostack_size = 0;
    pthread_attr_getstacksize(&attr, &ostack_size);
    do{
        if (ostack_size != c->stacksize
                && (0 != (*err = pthread_attr_setstacksize(&attr,c->stacksize)))){
            pthread_attr_destroy(&attr);
            SpxLog2(c->log,SpxLogError,*err,\
                    "set thread stack size is fail.");
        }
        if (0 !=(*err =  pthread_create(&(tid), &attr, ydb_storage_csync,
                        NULL))){
            pthread_attr_destroy(&attr);
            SpxLog2(c->log,SpxLogError,*err,\
                    "create heartbeat thread is fail.");
        }
    }while(false);
    pthread_attr_destroy(&attr);
    return tid;
}/*}}}*/


spx_private void ydb_storage_csync_handler(struct ev_loop *loop,\
        ev_timer *w,int revents){
    err_t err = 0;
    struct spx_map_iter *iter = spx_map_iter_new(g_ydb_storage_slaves,&err);
    if(NULL == iter){
    }
    struct spx_map_node *n = NULL;
    while(NULL != (n = spx_map_iter_next(iter,&err))){
        struct ydb_storage_slave *slave = n->v;
        if(NULL == slave){
            continue;
        }
        if(YDB_STORAGE_RUNNING == slave->state
                && !slave->is_syncing){
            spx_threadpool_execute(sync_threadpool,ydb_storage_csync_do,slave);
        }
    }
    spx_map_iter_free(&iter);
}


spx_private void ydb_storage_csync_do(void *arg){
    err_t err = 0;
    struct ydb_storage_slave *slave = (struct ydb_storage_slave *) arg;
    if(0 == slave->read_binlog_date.year){//first sync
        spx_get_date((time_t *) &(g_ydb_storage_runtime->first_start_time),
                &(slave->read_binlog_date));
    }
    slave->is_syncing = true;
    struct ydb_storage_configurtion *c = slave->c;
    string_t line = NULL;
    while(true){
        if(spx_date_is_after(&(slave->read_binlog_date))){
            SpxLogFmt2(c->log,SpxLogError,err,
                    "sync is overload the date.syncing date:%04-%02-%02"
                    "slave machineid:%s.",
                    slave->read_binlog_date.year,slave->read_binlog_date.month,
                    slave->read_binlog_date.day,slave->machineid);
            break;
        }

        string_t rbinlog_filename = ydb_storage_binlog_make_filename(c->log,
                c->basepath,c->machineid,slave->read_binlog_date.year,
                slave->read_binlog_date.month,slave->read_binlog_date.day,&err);
        if(SpxStringIsNullOrEmpty(rbinlog_filename)){
            SpxLog2(c->log,SpxLogError,err,
                    "make binlog filenam is fail.");
            break;
        }

        if(!SpxFileExist(rbinlog_filename)){// no the binlog
            SpxLogFmt1(c->log,SpxLogWarn,
                    "the binlog:%s is not exist.",
                    rbinlog_filename);
            spx_date_add(&(slave->read_binlog_date),1);
            spx_string_free(rbinlog_filename);
            continue;
        }

        struct stat buf;
        SpxZero(buf);
        lstat(rbinlog_filename,&buf);
        if((u64_t) buf.st_size ==(u64_t) slave->read_binlog_offset){// this binlog is syncing over
            SpxLogFmt1(c->log,SpxLogWarn,
                    "the binlog:%s is syncing over,offset:%ulld,filesize:%ulld.",
                    rbinlog_filename,slave->read_binlog_offset,buf.st_size);
            slave->read_binlog_offset = 0;
            spx_date_add(&(slave->read_binlog_date),1);
            spx_string_free(rbinlog_filename);
            continue;
        }

        line = spx_string_newlen(NULL,SpxLineSize,&err);
        if(NULL == line){
            SpxLog2(c->log,SpxLogError,err,
                    "alloc line for sync is fail.");
            break;
        }

        FILE *fp = NULL;
        fp = fopen(rbinlog_filename,"r");
        if(NULL == fp){
            err = errno;
            SpxLogFmt2(c->log,SpxLogError,err,
                    "open binlog:%s is fail.",
                    rbinlog_filename);
            break;
        }

        while(NULL != (fgets(line,SpxLineSize,fp))){
            spx_string_updatelen(line);

        }

    }

    if(NULL != line){
        spx_string_free(line);
    }
    slave->is_syncing = false;
    return;
}

spx_private err_t ydb_storage_csync_onefile(struct ydb_storage_slave *slave,
        struct ydb_storage_configurtion *c,string_t line){
    err_t err = 0;
    i32_t op  = 0;
    bool_t issinglefile = 0;
    string_t mid = NULL;
    u32_t ver = 0;
    u32_t opver = 0;
    u64_t fcreatetime = 0;
    u64_t createtime = 0;
    u64_t lastmodifytime = 0;
    i32_t mpidx = 0;
    i32_t p1 = 0;
    i32_t p2 = 0;
    u32_t tid = 0;
    u32_t rand = 0;
    u64_t begin = 0;
    u64_t totalsize = 0;
    u64_t realsize = 0;
    string_t suffix = NULL;
    err =  ydb_storage_binlog_context_parser(c->log,line,&op,&issinglefile,
            &mid,&ver,&opver,&fcreatetime,&createtime,&lastmodifytime,
            &mpidx,&p1,&p2,&tid,&rand,&begin,&totalsize,&realsize,&suffix);
    if(0 != err){
        SpxLogFmt2(c->log,SpxLogError,err,
                "parser line of binlog is fail.line:%s.",
                line);
        goto r1;
    }
    string_t filename = ydb_storage_dio_make_filename(c->log,c->mountpoints,
            (u8_t) mpidx,mid,tid,fcreatetime,rand,suffix,&err);
    if(NULL == filename){
        SpxLog2(c->log,SpxLogError,err,
                "make filename is fail.");
        goto r1;
    }
    if(YDB_BINLOG_DELETE == op){
        ydb_storage_csync_delete();
    } else {
        ydb_storage_csync_file();
    }

r1:
    if(NULL != mid){
        spx_string_free(mid);
    }
    if(NULL != suffix){
        spx_string_free(suffix);
    }
    return err;
}

spx_private err_t  ydb_storage_csync_delete(struct ydb_storage_slave *slave,
            string_t filename,
            char op,bool_t issinglefile,\
            u32_t ver,u32_t opver,\
            string_t mid,u64_t fcreatetime,u64_t createtime,\
            u64_t lastmodifytime,u8_t mpidx,u8_t p1,u8_t p2, int tid,\
            u32_t rand,u64_t begin,u64_t totalsize,u64_t realsize,string_t suffix){

    struct ydb_storage_configurtion *c = slave->c;

    err_t err = 0;
    struct spx_msg_context request;
    struct spx_msg_context response;
    SpxZero(request);
    SpxZero(response);

    struct spx_msg_header *request_header = spx_alloc_alone(sizeof(*request_header),&err);
    if(NULL == request_header){

    }
    request.header = request_header;
    //issingfile + begin + totalsize + filename
    request_header->protocol = YDB_STORAGE_SYNC_DELETE;
    request_header->bodylen = sizeof(char) + sizeof(u64_t) * 2 + spx_string_len(filename);

    struct spx_msg *ctx = spx_msg_new(request_header->bodylen,&err);
    if(NULL == ctx){

    }
    request.body = ctx;
    if(issinglefile){
        spx_msg_pack_true(ctx);
    } else {
        spx_msg_pack_false(ctx);
    }
    spx_msg_pack_u64(ctx,begin);
    spx_msg_pack_u64(ctx,totalsize);
    spx_msg_pack_string(ctx,filename);
    int sock = 0;
    sock = spx_socket_new(&err);
    if(0 >= sock || 0 != err){

    }
    spx_socket_set(sock,\
                    true,c->timeout,\
                    3,c->timeout,\
                    false,0,\
                    true,\
                    true,c->timeout);
    spx_set_nb(sock);
    spx_socket_connect(sock,slave->host.ip,slave->host.port);
    err = spx_write_context_nb(c->log,sock,&request);
    if(0 != err){

    }

    struct spx_msg_header *pheader = spx_read_header_nb(c->log,sock,&err);
    if(NULL == pheader || 0 != err){

    }
    close(sock);
    return err;
}


spx_private err_t  ydb_storage_csync_file(struct ydb_storage_slave *slave,
            string_t filename,
            char op,bool_t issinglefile,\
            u32_t ver,u32_t opver,\
            string_t mid,u64_t fcreatetime,u64_t createtime,\
            u64_t lastmodifytime,u8_t mpidx,u8_t p1,u8_t p2, int tid,\
            u32_t rand,u64_t begin,u64_t totalsize,u64_t realsize,string_t suffix){
    if(!SpxFileExist(filename)){

    }

    if(issinglefile){

    } else {

    }
}
