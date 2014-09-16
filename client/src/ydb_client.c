/*************************************************************
 *                     _ooOoo_
 *                    o8888888o
 *                    88" . "88
 *                    (| -_- |)
 *                    O\  =  /O
 *                 ____/`---'\____
 *               .'  \\|     |//  `.
 *              /  \\|||  :  |||//  \
 *             /  _||||| -:- |||||-  \
 *             |   | \\\  -  /// |   |
 *             | \_|  ''\---/''  |   |
 *             \  .-\__  `-`  ___/-. /
 *           ___`. .'  /--.--\  `. . __
 *        ."" '<  `.___\_<|>_/___.'  >'"".
 *       | | :  `- \`.;`\ _ /`;.`/ - ` : | |
 *       \  \ `-.   \_ __\ /__ _/   .-` /  /
 *  ======`-.____`-.___\_____/___.-`____.-'======
 *                     `=---='
 *  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 *           佛祖保佑       永无BUG
 *
 * ==========================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_client.c
 *        Created:  2014/09/09 09时37分30秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 ***********************************************************************/

#include <stdlib.h>
#include <stdio.h>


#include "spx_types.h"
#include "spx_string.h"
#include "spx_alloc.h"
#include "spx_socket.h"
#include "spx_message.h"
#include "spx_defs.h"
#include "spx_io.h"
#include "spx_list.h"

#include "ydb_protocol.h"

#include "ydb_client.h"

spx_private int g_ydb_client_trackers_idx = 0;

spx_private struct spx_list *ydb_client_parser_trackers(char *hosts,err_t *err);

spx_private err_t ydb_client_parser_fileid(string_t fileid,string_t *groupname,
        string_t *machineid,string_t *syncgroup);

spx_private struct spx_host *ydb_client_query_storage_for_upload(
        char *groupname,struct spx_list *trackers,err_t *err);

spx_private struct spx_host *ydb_client_query_storage_for_operator(
        int protocol,
        string_t groupname,string_t machineid,string_t syncgroup,
        struct spx_list *trackers,err_t *err);

spx_private err_t ydb_client_free_tracker(void **arg);

spx_private string_t ydb_client_uplaod_do(struct spx_host *s,
        byte_t *buff,size_t len,char *suffix,err_t *err);

spx_private byte_t *ydb_client_find_do(struct spx_host *s,
        string_t fileid,err_t *err);

spx_private struct spx_host *ydb_client_query_storage_for_upload(
        char *groupname,struct spx_list *trackers,err_t *err){/*{{{*/
    if(NULL == groupname || NULL == trackers){
        *err = EINVAL;
        return NULL;
    }

    struct spx_host *s = NULL;

    struct spx_msg_context *qctx = spx_alloc_alone(sizeof(*qctx),err);
    if(NULL == qctx || 0 != err){
        return NULL;
    }

    struct spx_msg_header * qheader = spx_alloc_alone(sizeof(*qheader),err);
    if(NULL == qheader){
        goto r1;
    }
    qctx->header = qheader;
    qheader->protocol = YDB_TRACKER_QUERY_UPLOAD_STORAGE;
    qheader->version = YDB_CLIENT_VERSION;
    qheader->bodylen = YDB_GROUPNAME_LEN;

    struct spx_msg *qbody = spx_msg_new(YDB_GROUPNAME_LEN,err);
    if(NULL == qbody || 0 != err){
        goto r1;
    }
    qctx->body = qbody;
    spx_msg_pack_ubytes(qbody,(ubyte_t *)groupname,SpxMin(YDB_GROUPNAME_LEN,strlen(groupname)));

    struct spx_host *t = NULL;
    size_t i = 0;
    int idx = 0;
    for(; i< trackers->curr_size; i++){
        idx =(size_t) g_ydb_client_trackers_idx == trackers->curr_size
            ? 0 : g_ydb_client_trackers_idx;
        t = (struct spx_host *)spx_list_get(trackers,idx);
        if(NULL == t){
            continue;
        }

        int fd = 0;
        fd = spx_socket_new(err);
        if(0 >= fd || 0 != err){
            continue;
        }
        spx_set_nb(fd);
        *err =  spx_socket_set(fd,true,30,3,30,false,0,true,true,30);
        if(0 != *err){
            SpxClose(fd);
            continue;
        }
        *err = spx_socket_connect(fd,t->ip,t->port);
        if(0 != *err){
            SpxClose(fd);
            continue;
        }
        *err = spx_write_context(NULL,fd,qctx);
        if(0 != *err){
            SpxClose(fd);
            continue;
        }

        struct spx_msg_header *pheader = spx_read_header(NULL,fd,err);
        if(NULL == pheader){
            SpxClose(fd);
            continue;
        }
        if(0 != *err || 0 != pheader->err || 0 == pheader->bodylen){
            *err = pheader->err;
            SpxFree(pheader);
            SpxClose(fd);
            continue;
        }
        struct spx_msg *pbody = spx_read_body_nb(NULL,fd,pheader->bodylen,err);
        SpxClose(fd);
        if(NULL == pbody){
            SpxFree(pheader);
           continue;
        }
        if(0 != *err || 0 == spx_msg_len(pbody)){
            SpxFree(pheader);
            SpxFree(pbody);
            continue;
        }
        s = spx_alloc_alone(sizeof(*s),err);
        if(NULL == s){
            SpxFree(pheader);
            SpxFree(pbody);
            continue;
        }
        s->ip = spx_msg_unpack_string(pbody,SpxIpv4Size,err);
        if(0 != *err){
            SpxFree(pheader);
            SpxFree(pbody);
            SpxFree(s);
            continue;
        }
        s->port = spx_msg_unpack_i32(pbody);
        SpxFree(pheader);
        SpxFree(pbody);
        break;
    }
r1:
    if(NULL != qctx){
        if(NULL != qctx->header){
            SpxFree(qctx->header);
        }
        if(NULL != qctx->body){
            SpxFree(qctx->body);
        }
        SpxFree(qctx);
    }
    return s;
}/*}}}*/


spx_private struct spx_host *ydb_client_query_storage_for_operator(
        int protocol,
        string_t groupname,string_t machineid,string_t syncgroup,
        struct spx_list *trackers,err_t *err){/*{{{*/

    if(SpxStringIsNullOrEmpty(groupname)
            || SpxStringIsNullOrEmpty(machineid)
            || SpxStringIsNullOrEmpty(syncgroup)
            || NULL == trackers){
        *err = EINVAL;
        return NULL;
    }

    struct spx_host *s = NULL;

    struct spx_msg_context *qctx = spx_alloc_alone(sizeof(*qctx),err);
    if(NULL == qctx || 0 != err){
        return NULL;
    }

    struct spx_msg_header * qheader = spx_alloc_alone(sizeof(*qheader),err);
    if(NULL == qheader){
        goto r1;
    }
    qctx->header = qheader;
    qheader->protocol = protocol;
    qheader->version = YDB_CLIENT_VERSION;
    qheader->bodylen = YDB_GROUPNAME_LEN + YDB_MACHINEID_LEN + YDB_SYNCGROUP_LEN;

    struct spx_msg *qbody = spx_msg_new(qheader->bodylen,err);
    if(NULL == qbody || 0 != err){
        goto r1;
    }
    qctx->body = qbody;
    spx_msg_pack_ubytes(qbody,(ubyte_t *)groupname,SpxMin(YDB_GROUPNAME_LEN,spx_string_len(groupname)));
    spx_msg_pack_ubytes(qbody,(ubyte_t *)machineid,SpxMin(YDB_MACHINEID_LEN,spx_string_len(machineid)));
    spx_msg_pack_ubytes(qbody,(ubyte_t *)syncgroup,SpxMin(YDB_SYNCGROUP_LEN,spx_string_len(syncgroup)));

    struct spx_host *t = NULL;
    size_t i = 0;
    int idx = 0;
    for(; i< trackers->curr_size; i++){
        idx =(size_t) g_ydb_client_trackers_idx == trackers->curr_size
            ? 0 : g_ydb_client_trackers_idx;
        t = (struct spx_host *)spx_list_get(trackers,idx);
        if(NULL == t){
            continue;
        }

        int fd = 0;
        fd = spx_socket_new(err);
        if(0 >= fd || 0 != err){
            continue;
        }
        spx_set_nb(fd);
        *err =  spx_socket_set(fd,true,30,3,30,false,0,true,true,30);
        if(0 != *err){
            SpxClose(fd);
            continue;
        }
        *err = spx_socket_connect(fd,t->ip,t->port);
        if(0 != *err){
            SpxClose(fd);
            continue;
        }
        *err = spx_write_context(NULL,fd,qctx);
        if(0 != *err){
            SpxClose(fd);
            continue;
        }

        struct spx_msg_header *pheader = spx_read_header(NULL,fd,err);
        if(NULL == pheader){
            SpxClose(fd);
            continue;
        }
        if(0 != *err || 0 != pheader->err || 0 == pheader->bodylen){
            *err = pheader->err;
            SpxFree(pheader);
            SpxClose(fd);
            continue;
        }
        struct spx_msg *pbody = spx_read_body_nb(NULL,fd,pheader->bodylen,err);
        SpxClose(fd);
        if(NULL == pbody){
            SpxFree(pheader);
           continue;
        }
        if(0 != *err || 0 == spx_msg_len(pbody)){
            SpxFree(pheader);
            SpxFree(pbody);
            continue;
        }
        s = spx_alloc_alone(sizeof(*s),err);
        if(NULL == s){
            SpxFree(pheader);
            SpxFree(pbody);
            continue;
        }
        s->ip = spx_msg_unpack_string(pbody,SpxIpv4Size,err);
        if(0 != *err){
            SpxFree(pheader);
            SpxFree(pbody);
            SpxFree(s);
            continue;
        }
        s->port = spx_msg_unpack_i32(pbody);
        SpxFree(pheader);
        SpxFree(pbody);
        break;
    }
r1:
    if(NULL != qctx){
        if(NULL != qctx->header){
            SpxFree(qctx->header);
        }
        if(NULL != qctx->body){
            SpxFree(qctx->body);
        }
        SpxFree(qctx);
    }
    return s;
}/*}}}*/

spx_private struct spx_list *ydb_client_parser_trackers(char *hosts,err_t *err){/*{{{*/
    string_t shosts = spx_string_new(hosts,err);
    if(NULL == shosts || 0 != *err){
        return NULL;
    }
    int count = 0;
    string_t *ctx = spx_string_split(shosts,";",sizeof(";"),&count,err);
    if(0 != *err || NULL == ctx){
        spx_string_free(shosts);
        return NULL;
    }
    struct spx_list *trackers =  spx_list_new(NULL,0,ydb_client_free_tracker,err);

    if(NULL == trackers || 0 != *err){
        spx_string_free_splitres(ctx,count);
        spx_string_free(shosts);
        return NULL;
    }
    int i = 0;
    int len = 0;
    for( ; i< count; i++){
        string_t *host = spx_string_split(*(ctx + i),":",sizeof(":"),&len,err);
        if(0 != err || 2 != len){
            continue;
        }
        int j = 0;
        struct spx_host *h = spx_alloc_alone(sizeof(*h),err);
        if(NULL == h || 0 != *err){
            spx_string_free_splitres(host,len);
            continue;
        }
        for( ; j < len; j++){
            switch(j){
                case 0:{
                           h->ip = spx_string_dup(*(host + j),err);
                           if(NULL == h->ip || 0 != *err){
                               SpxFree(h);
                               break;
                           }
                       }
                case 1:{
                           h->port = atoi(*(host + j));
                           if(0 == h->port){
                               *err = errno;
                               if(NULL != h->ip){
                                   spx_string_free(h->ip);
                               }
                               SpxFree(h);
                               break;
                           }
                       }
            }
        }
        if(NULL != h){
            if(0 != spx_list_add(trackers,h)){
                if(NULL != h->ip){
                    spx_string_free(h->ip);
                }
                SpxFree(h);
            }
        }
        spx_string_free_splitres(host,len);
    }
    spx_string_free_splitres(ctx,count);
    spx_string_free(shosts);
    return trackers;
}/*}}}*/

spx_private err_t ydb_client_parser_fileid(string_t fileid,string_t *groupname,
        string_t *machineid,string_t *syncgroup){/*{{{*/
    err_t err = 0;
    if(NULL == fileid || 0 == strlen(fileid)){
        return EINVAL;
    }
    int count = 0;
    int info_count = 3;
    string_t *ctx = spx_string_split(fileid,":",sizeof(":"),&count,&err);
    if(NULL == ctx){
        return err;
    }
    if(count < info_count){
        err =  EINVAL;
        goto r1;
    }
    int i = 0;
    for( ; i < info_count; i++){
        switch(i){
            case 0:{
                        *groupname = spx_string_dup(*(ctx + i),&err);
                        if(0 != err || NULL == *groupname){
                            break;
                        }
                   }
            case 1:{
                        *machineid = spx_string_dup(*(ctx + i),&err);
                        if(0 != err || NULL == *machineid){
                            spx_string_free(*groupname);
                            break;
                        }
                   }
            case 2:{
                        *syncgroup = spx_string_dup(*(ctx + i),&err);
                        if(0 != err || NULL == *syncgroup){
                            spx_string_free(*groupname);
                            spx_string_free(*machineid);
                            break;
                        }
                   }
        }
    }
r1:
    spx_string_free_splitres(ctx,count);
    return err;
}/*}}}*/

spx_private err_t ydb_client_free_tracker(void **arg){/*{{{*/
    if(NULL == arg || NULL == *arg){
        return 0;
    }
    struct spx_host **t = (struct spx_host **) arg;
    if(NULL != ((*t)->ip)){
        spx_string_free((*t)->ip);
    }
    SpxFree(*t);
    return 0;
}/*}}}*/


string_t ydb_client_upload(char *groupname,char *hosts,
        byte_t *buff,size_t len,char *suffix,err_t *err){/*{{{*/
    if(NULL == groupname || 0 == strlen(groupname)){
        *err = EINVAL;
        return NULL;
    }
    if(NULL == hosts || 0 == strlen(hosts)){
        *err = EINVAL;
        return NULL;
    }
    if(NULL == buff || 0 == len){
        *err = EINVAL;
        return NULL;
    }
    string_t test_fileid = spx_string_new("g001/m001/s001/00/00/01/23434523242/82323232/234234/2342342/13344/9343",err);
    return test_fileid;

    struct spx_list *trackers = NULL;
    struct spx_host *s = NULL;
    string_t fileid = NULL;

    trackers = ydb_client_parser_trackers(hosts,err);
    if(NULL == trackers){
        goto r1;
    }

    s = ydb_client_query_storage_for_upload(
            groupname,trackers,err);
    if(NULL == s){
        goto r1;
    }

    fileid = ydb_client_uplaod_do(s,buff,len,suffix,err);
    if(NULL == fileid){
        goto r1;
    }

r1:
    if(NULL != trackers){
        spx_list_free(&trackers);
    }
    if(NULL != s){
        if(NULL != s->ip){
            spx_string_free(s->ip);
        }
        SpxFree(s);
    }
    return fileid;
}/*}}}*/

spx_private string_t ydb_client_uplaod_do(struct spx_host *s,
        byte_t *buff,size_t len,char *suffix,err_t *err){/*{{{*/
    struct spx_msg_context *qctx = spx_alloc_alone(sizeof(*qctx),err);
    if(NULL == qctx || 0 != err){
        return NULL;
    }

    string_t fileid = NULL;
    int fd = 0;
    struct spx_msg_header *pheader = NULL;
    struct spx_msg *pbody = NULL;

    struct spx_msg_header *qheader = spx_alloc_alone(sizeof(*qheader),err);
    if(NULL == qheader || 0 != err){
        goto r1;
    }
    qctx->header = qheader;

    if(NULL == suffix || 0 == strlen(suffix)){
        qheader->bodylen = SpxBoolTransportSize + YDB_FILENAME_SUFFIX_SIZE + len;
        qheader->offset = SpxBoolTransportSize + YDB_FILENAME_SUFFIX_SIZE;
    } else {
        qheader->bodylen = SpxBoolTransportSize + len;
        qheader->offset = SpxBoolTransportSize;
    }
    struct spx_msg *qbody = spx_msg_new(qheader->bodylen,err);
    if(NULL == qbody || 0 != err){
        goto r1;
    }
    qctx->body = qbody;

    qheader->protocol = YDB_STORAGE_UPLOAD;
    qheader->version = YDB_CLIENT_VERSION;
    if(NULL == suffix || 0 == strlen(suffix)){
        spx_msg_pack_true(qbody);
        spx_msg_pack_fixed_string(qbody,suffix,YDB_FILENAME_SUFFIX_SIZE);
    } else {
        spx_msg_pack_false(qbody);
    }

    spx_msg_pack_ubytes(qbody,(ubyte_t *)buff,len);

    fd = spx_socket_new(err);
    if(0 >= fd || 0 != err){
    }
    spx_set_nb(fd);
    *err =  spx_socket_set(fd,true,30,3,30,false,0,true,true,30);
    if(0 != *err){
        goto r1;
    }
    *err = spx_socket_connect(fd,s->ip,s->port);
    if(0 != *err){
        goto r1;
    }
    *err = spx_write_context(NULL,fd,qctx);
    if(0 != *err){
        goto r1;
    }

    pheader = spx_read_header(NULL,fd,err);
    if(NULL == pheader){
        goto r1;
    }
    if(0 != pheader->err || 0 == pheader->bodylen){
        goto r1;
    }
    pbody = spx_read_body_nb(NULL,fd,pheader->bodylen,err);
    if(NULL == pbody){
        goto r1;
    }
    if(0 == spx_msg_len(pbody)){
        goto r1;
    }
    fileid = spx_msg_unpack_string(pbody,pheader->bodylen,err);
r1:
    if(NULL != qctx){
        if(NULL != qctx->header){
            SpxFree(qctx->header);
        }
        if(NULL != qctx->body){
            SpxFree(qctx->body);
        }
        SpxFree(qctx);
    }
    if(0 < fd){
        SpxClose(fd);
    }
    if(NULL != pheader){
        SpxFree(pheader);
    }
    if(NULL != pbody){
        SpxFree(pbody);
    }

   return fileid;
}/*}}}*/


byte_t *ydb_client_find(char *hosts,char *fileid,err_t *err){/*{{{*/
    if(NULL == hosts || 0 == strlen(hosts)){
        *err = EINVAL;
        return NULL;
    }
    if(NULL == fileid || 0 == strlen(fileid)){
        *err = EINVAL;
        return NULL;
    }

    byte_t *test_buff = spx_alloc_alone(1024,err);
    return test_buff;

    struct spx_list *trackers = NULL;
    struct spx_host *s = NULL;
    string_t groupname = NULL;
    string_t machineid = NULL;
    string_t syncgroup = NULL;
    byte_t *buff = NULL;

    trackers = ydb_client_parser_trackers(hosts,err);
    if(NULL == trackers){
        goto r1;
    }

    *err =  ydb_client_parser_fileid(fileid,&groupname,&machineid,&syncgroup);
    if(0 != *err){
        goto r1;
    }

    s = ydb_client_query_storage_for_operator(
            YDB_TRACKER_QUERY_SELECT_STORAGE,
            groupname,machineid,syncgroup,
            trackers,err);
    if(NULL == s){
        goto r1;
    }

    buff = ydb_client_find_do(s,fileid,err);
r1:
    if(NULL != trackers){
        spx_list_free(&trackers);
    }
    if(NULL != s){
        if(NULL != s->ip){
            spx_string_free(s->ip);
        }
        SpxFree(s);
    }
    if(NULL != groupname){
        spx_string_free(groupname);
    }
    if(NULL != machineid){
        spx_string_free(machineid);
    }
    if(NULL != syncgroup){
        spx_string_free(syncgroup);
    }
    return buff;
}/*}}}*/


spx_private byte_t *ydb_client_find_do(struct spx_host *s,
        string_t fileid,err_t *err){/*{{{*/
    struct spx_msg_context *qctx = spx_alloc_alone(sizeof(*qctx),err);
    if(NULL == qctx || 0 != err){
        return NULL;
    }

    int fd = 0;
    struct spx_msg_header *pheader = NULL;
    struct spx_msg *pbody = NULL;
    byte_t *buff = NULL;

    struct spx_msg_header *qheader = spx_alloc_alone(sizeof(*qheader),err);
    if(NULL == qheader || 0 != err){
        goto r1;
    }
    qctx->header = qheader;

    struct spx_msg *qbody = spx_msg_new(spx_string_len(fileid),err);
    if(NULL == qbody || 0 != err){
        goto r1;
    }
    qctx->body = qbody;

    qheader->protocol = YDB_STORAGE_FIND;
    qheader->version = YDB_CLIENT_VERSION;
    qheader->bodylen = spx_string_len(fileid);
    qheader->offset = 0;
    spx_msg_pack_string(qbody,fileid);

    fd = spx_socket_new(err);
    if(0 >= fd || 0 != err){
    }
    spx_set_nb(fd);
    *err =  spx_socket_set(fd,true,30,3,30,false,0,true,true,30);
    if(0 != *err){
        goto r1;
    }
    *err = spx_socket_connect(fd,s->ip,s->port);
    if(0 != *err){
        goto r1;
    }
    *err = spx_write_context(NULL,fd,qctx);
    if(0 != *err){
        goto r1;
    }

    pheader = spx_read_header(NULL,fd,err);
    if(NULL == pheader){
        goto r1;
    }
    if(0 != pheader->err || 0 == pheader->bodylen){
        goto r1;
    }
    pbody = spx_read_body_nb(NULL,fd,pheader->bodylen,err);
    if(NULL == pbody){
        goto r1;
    }
    if(0 == spx_msg_len(pbody)){
        goto r1;
    }
    buff = spx_msg_unpack_bytes(pbody,pheader->bodylen,err);
r1:
    if(NULL != qctx){
        if(NULL != qctx->header){
            SpxFree(qctx->header);
        }
        if(NULL != qctx->body){
            SpxFree(qctx->body);
        }
        SpxFree(qctx);
    }
    if(0 < fd){
        SpxClose(fd);
    }
    if(NULL != pheader){
        SpxFree(pheader);
    }
    if(NULL != pbody){
        SpxFree(pbody);
    }

   return buff;
}/*}}}*/


/*
bool_t ydb_client_delete(char *groupname,char *hosts,char *fileid){
return true;
}

string_t ydb_client_update(char *groupname,char *hosts,
        byte_t *buff,size_t len,char *suffix){
    return NULL;
}
*/
void ydb_client_upload_free(string_t fileid){/*{{{*/
    if(NULL == fileid) return;
    spx_string_free(fileid);
}/*}}}*/

void ydb_client_find_free(byte_t **buff){/*{{{*/
    if(NULL != buff) return;
    if(NULL != *buff){
        SpxFree(*buff);
    }
}/*}}}*/

