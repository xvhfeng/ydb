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

    char *jpg = "jpg";
    char *png = "png"
    char *groupname = "g001";
    char *tracker = "10.97.19.31:4150";
    u32_t timeout = 300;

err_t ydb_client_upload_test(
        char *groupname,
        char *tracker,
        char *local_suffix,
        char *local_fname,
        char *write_fpath,
        u32_t timeout
        );






}

