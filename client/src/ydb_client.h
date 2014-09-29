/***********************************************************************
 *                              _ooOoo_
 *                             o8888888o
 *                             88" . "88
 *                             (| -_- |)
 *                              O\ = /O
 *                          ____/`---'\____
 *                        .   ' \\| |// `.
 *                         / \\||| : |||// \
 *                       / _||||| -:- |||||- \
 *                         | | \\\ - /// | |
 *                       | \_| ''\---/'' | |
 *                        \ .-\__ `-` ___/-. /
 *                     ___`. .' /--.--\ `. . __
 *                  ."" '< `.___\_<|>_/___.' >'"".
 *                 | | : `- \`.;`\ _ /`;.`/ - ` : | |
 *                   \ \ `-. \_ __\ /__ _/ .-` / /
 *           ======`-.____`-.___\_____/___.-`____.-'======
 *                              `=---='
 *           .............................................
 *                    佛祖镇楼                  BUG辟易
 *            佛曰:
 *                    写字楼里写字间，写字间里程序员；
 *                    程序人员写程序，又拿程序换酒钱。
 *                    酒醒只在网上坐，酒醉还来网下眠；
 *                    酒醉酒醒日复日，网上网下年复年。
 *                    但愿老死电脑间，不愿鞠躬老板前；
 *                    奔驰宝马贵者趣，公交自行程序员。
 *                    别人笑我忒疯癫，我笑自己命太贱；
 *                    不见满街漂亮妹，哪个归得程序员？
 * ==========================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_client.h
 *        Created:  2014/09/09 09时43分17秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 ****************************************************************************/
#ifndef _YDB_CLIENT_H_
#define _YDB_CLIENT_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>

#include "spx_types.h"

#define YDB_CLIENT_VERSION 1

string_t ydb_client_upload(char *groupname,char *hosts,
        byte_t *buff,size_t len,char *suffix,u32_t timeout,err_t *err);

bool_t ydb_client_delete(char *groupname,char *hosts,char *fileid,u32_t timeout,err_t *err);
byte_t *ydb_client_find(char *hosts,char *fileid,size_t *len,u32_t timeout,err_t *err);
string_t ydb_client_modify(char *old_fileid,char *hosts,
        byte_t *buff,size_t len,char *suffix,u32_t timeout,err_t *err);

void ydb_client_upload_free(string_t fileid);
void ydb_client_find_free(byte_t **buff);


#ifdef __cplusplus
}
#endif
#endif
