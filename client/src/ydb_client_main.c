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
 *       Filename:  ydb_client_main.c
 *        Created:  2014/09/10 13时50分28秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 ***********************************************************************/

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

#include "ydb_client.h"

int main(int argc,char **argv){
    err_t err = 0;

    string_t tfileid = NULL;
    byte_t *tbuf_rc = NULL;
    size_t tlen = 0;

    string_t ifileid = NULL;
    byte_t *ibuf_rc = NULL;
    size_t ilen = 0;

    char *suffix = "png";
    char *groupname = "g001";
    char *host = "10.97.19.31:4150";
    char *filename = "1.png";
    u32_t timeout = 30;

    struct stat buf;
    SpxZero(buf);
    lstat("1.png",&buf);
    size_t filesize = buf.st_size;

    byte_t *buff = spx_alloc_alone(filesize,&err);
    if(NULL == buff){
        printf("error!!.");
        return 0;
    }
    int fd = open(filename,O_RDWR);
    if(0 >= fd){
        printf("open file error.");
        return 0;
    }
    size_t recvbytes = 0;
    err = spx_read(fd,buff,filesize,&recvbytes);
    if(0 != err || recvbytes != filesize){
        printf("read file bytes is fail.");
        return 0;
    }
    close(fd);

    int i = 0;
    char iname[256] = {0};
    char tname[256] = {0};
    for (i = 0; i < 1000; ++i) {
        ifileid = ydb_client_upload(groupname,host,buff,filesize,suffix,timeout,&err);
        ibuf_rc = ydb_client_find(host,ifileid,&ilen,timeout,&err);
        printf("%s \n",ifileid);
        snprintf(iname,256,"./img1/%d",i);
        int ifd = open(iname,O_RDWR|O_CREAT);
        if(0 > ifd){
            err = errno;
        }
        spx_write(ifd,ibuf_rc,ilen,&ilen);
        close(ifd);
        memset(iname,0,256);

        tfileid = ydb_client_upload(groupname,host,buff,filesize,NULL,timeout,&err);
        tbuf_rc = ydb_client_find(host,tfileid,&tlen,timeout,&err);
        printf("%s \n",tfileid);
        snprintf(tname,256,"./img2/%d",i);
        int tfd = open(tname,O_RDWR|O_CREAT);
        if(0 > tfd){
            err = errno;
        }
        spx_write(tfd,tbuf_rc,tlen,&tlen);
        close(tfd);
        memset(tname,0,256);


        if(NULL != tfileid){
            spx_string_free(tfileid);
        }
        if(NULL != tbuf_rc){
            SpxFree(tbuf_rc);
        }

        if(NULL != ifileid){
            spx_string_free(ifileid);
        }
        if(NULL != ibuf_rc){
            SpxFree(ibuf_rc);
        }
    }

    return 0;
}
