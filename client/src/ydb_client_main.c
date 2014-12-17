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
#include "ydb_client_test.h"

int main(int argc,char **argv){
    err_t err = 0;

    char *png = "png";
    char *jpg = "jpg";
    char *jpg105 = "105.jpg";
    char *png200 = "200.png";
    char *jpg216 = "216.jpg";
    char *jpg250 = "250.jpg";
    char *jpg254 = "254.jpg";
    char *jpg44 = "44.jpg";
    char *wfpath = "img/";

    char *groupname = "g001";
    char *tracker = "10.97.19.31:4150";
    u32_t timeout = 300;

    /*
       ydb_client_upload_test(
       groupname,
       tracker,
       jpg,
       jpg105,
       wfpath,
       timeout,true);

       ydb_client_upload_test(
       groupname,
       tracker,
       jpg,
       jpg254,
       wfpath,
       timeout,true);

       ydb_client_delete_test(
       groupname,
       tracker,
       jpg,
       jpg44,
       timeout,true);

       ydb_client_delete_test(
       groupname,
       tracker,
       jpg,
       jpg105,
       timeout,true);
       ydb_client_delete_test(
       groupname,
       tracker,
       jpg,
       jpg254,
       timeout,true);
       */

    ydb_client_modify_test(
            groupname,
            tracker,
            jpg,
            jpg254,
            wfpath,
            jpg,
            jpg250,
            wfpath,
            timeout,true);

    ydb_client_modify_test(
            groupname,
            tracker,
            jpg,
            jpg250,
            wfpath,
            jpg,
            jpg254,
            wfpath,
            timeout,true);

    ydb_client_modify_test(
            groupname,
            tracker,
            jpg,
            jpg44,
            wfpath,
            jpg,
            jpg250,
            wfpath,
            timeout,true);

    ydb_client_modify_test(
            groupname,
            tracker,
            jpg,
            jpg250,
            wfpath,
            jpg,
            jpg44,
            wfpath,
            timeout,true);

    ydb_client_modify_test(
            groupname,
            tracker,
            jpg,
            jpg44,
            wfpath,
            jpg,
            jpg105,
            wfpath,
            timeout,true);

    ydb_client_modify_test(
            groupname,
            tracker,
            jpg,
            jpg105,
            wfpath,
            jpg,
            jpg44,
            wfpath,
            timeout,true);

    return 0;
}

