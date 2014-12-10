#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "spx_types.h"
#include "spx_string.h"
#include "spx_alloc.h"
#include "spx_io.h"
#include "spx_defs.h"
#include "spx_time.h"

#include "ydb_client.h"

spx_private byte_t * ydb_client_read_local(
        char *fname,
        size_t *len,
        err_t *err
        );

spx_private err_t ydb_client_write_local(
        byte_t *buf,
        size_t size,
        char *fpath,
        char *suffix
        );

err_t ydb_client_upload_test(
        char *groupname,
        char *tracker,
        char *local_suffix,
        char *local_fname,
        char *write_fpath,
        u32_t timeout
        ){/*{{{*/
    size_t local_fsize = 0;
    err_t err = 0;
    byte_t *buff = NULL;
    string_t fid = NULL;
    byte_t *fbuff = NULL;

    buff = ydb_client_read_local(local_fname,&local_fsize,&err);
    if(NULL == buff){
        printf("read local file:%s is fail.",
                local_fname);
        return err;
    }
    fid = ydb_client_upload(groupname,tracker,buff,local_fsize,local_suffix,timeout,&err);
    size_t ffsize = 0;
    fbuff = ydb_client_find(tracker,fid,&ffsize,timeout,&err);
    if(NULL == fbuff){
        printf("find file:%s from remote is fail.",
                fid);
        goto r1;
    }
    if(0 != (err = ydb_client_write_local(fbuff,ffsize,write_fpath,local_suffix))){
        printf("fid:%s write  to local is fail.",
                fid);
        goto r1;
    }
r1:
    SpxFree(buff);
    SpxStringFree(fid);
    SpxFree(fbuff);
    return err;
}/*}}}*/

err_t ydb_client_delete_test(
        char *groupname,
        char *tracker,
        char *local_suffix,
        char *local_fname,
        u32_t timeout
        ){/*{{{*/
    size_t local_fsize = 0;
    err_t err = 0;
    byte_t *buff = NULL;
    string_t fid = NULL;

    buff = ydb_client_read_local(local_fname,&local_fsize,&err);
    if(NULL == buff){
        printf("read local file:%s is fail.",
                local_fname);
        return err;
    }
    fid = ydb_client_upload(groupname,tracker,buff,local_fsize,local_suffix,timeout,&err);
    if(0 != (err = ydb_client_delete(tracker,fid,timeout))){
        printf("delete file:%s from remote is fail.",
                fid);
        goto r1;
    }
r1:
    SpxFree(buff);
    SpxStringFree(fid);
    return err;
}/*}}}*/

err_t ydb_client_modify_c2c_test(
        char *groupname,
        char *tracker,
        char *local_suffix,
        char *local_fname,
        char *write_fpath,
        char *modify_suffix,
        char *modify_local_fname,
        char *modify_write_fpath,
        u32_t timeout
        ){/*{{{*/
    size_t local_fsize = 0;
    err_t err = 0;
    byte_t *buff = NULL;
    string_t fid = NULL;
    byte_t *fbuff = NULL;

    byte_t *mbuff = NULL;
    string_t mfid = NULL;
    byte_t *mfbuff = NULL;

    buff = ydb_client_read_local(local_fname,&local_fsize,&err);
    if(NULL == buff){
        printf("read local file:%s is fail.",
                local_fname);
        return err;
    }
    fid = ydb_client_upload(groupname,tracker,buff,local_fsize,local_suffix,timeout,&err);
    size_t ffsize = 0;
    fbuff = ydb_client_find(tracker,fid,&ffsize,timeout,&err);
    if(NULL == fbuff){
        printf("find file:%s from remote is fail.",
                fid);
        goto r1;
    }
    if(0 != (err = ydb_client_write_local(fbuff,ffsize,write_fpath,local_suffix))){
        printf("fid:%s write  to local is fail.",
                fid);
        goto r1;
    }

    size_t mfsize = 0;
    size_t mffsize = 0;
    mbuff = ydb_client_read_local(modify_local_fname,&mfsize,&err);
    if(NULL == mbuff){
        printf("read modify file:%s is fail.",
                modify_local_fname);
        goto r1;
    }
    mfid = ydb_client_modify(tracker,fid,mbuff,mfsize,modify_suffix,timeout,&err);
    if(NULL == mfid){
        printf("modify file:%s is fail.",
                fid);
        goto r1;
    }
    mfbuff = ydb_client_find(tracker,mfid,&mffsize,timeout,&err);
    if(NULL == mfbuff){
        printf("find file:%s from remote is fail.",
                mfid);
        goto r1;
    }

    if(0 != (err = ydb_client_write_local(mfbuff,mffsize,
                    modify_write_fpath,modify_suffix))){
        printf("write modify file:%s to local is fail.",
                mfid);
        goto r1;
    }
r1:
    SpxFree(buff);
    SpxStringFree(fid);
    SpxFree(fbuff);
    SpxFree(mbuff);
    SpxStringFree(mfid);
    SpxFree(mfbuff);
    return err;
}/*}}}*/


spx_private byte_t *ydb_client_read_local(
        char *fname,
        size_t *len,
        err_t *err
        ){/*{{{*/
    byte_t *buff = NULL;
    int fd = 0;
    struct stat buf;
    SpxZero(buf);
    lstat(fname,&buf);
    size_t filesize = buf.st_size;

    buff = spx_alloc_alone(filesize,err);
    if(NULL == buff){
        printf("error!!.");
        return NULL;
    }
    fd = open(fname,O_RDWR);
    if(0 >= fd){
        *err = errno;
        printf("open file error.");
        goto r1;
    }
    size_t recvbytes = 0;
    *err = spx_read(fd,buff,filesize,&recvbytes);
    if(0 != *err || recvbytes != filesize){
        printf("read file bytes is fail.");
        goto r1;
    }
    *len = filesize;
    goto r2;
r1:
    if(NULL != buff){
        SpxFree(buff);
    }
r2:
    if(0 < fd) {
        close(fd);
    }
    return buff;
}/*}}}*/

spx_private err_t ydb_client_write_local(
        byte_t *buf,
        size_t size,
        char *fpath,
        char *suffix
        ){/*{{{*/
    err_t err = 0;
    char fname[1024] = {0};
    time_t now = spx_now();
    if(NULL == suffix) {
        snprintf(fname,1023,"%s/%ld",fpath,now);
    } else {
        snprintf(fname,1023,"%s/%ld.%s",fpath,now,suffix);
    }
    int fd = open(fname,O_RDWR|O_CREAT);
    if(0 > fd){
        err = errno;
        return err;
    }
    size_t len = 0;
    err = spx_write(fd,buf,size,&len);
    if(size != len || 0 != err){
        printf("write file:%s is fail."
                "errno:%d,info:%s."
                "size:%ld,len:%ld.",
                fname,err,strerror(err),size,len);
        close(fd);
        return err;
    }
    close(fd);
    return 0;
}/*}}}*/

