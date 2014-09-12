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
 *       Filename:  ydb_storage_dsync.c
 *        Created:  2014/09/01 17时17分09秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 ***********************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <ev.h>
#include <pthread.h>

#include "include/spx_alloc.h"
#include "include/spx_types.h"
#include "include/spx_defs.h"
#include "include/spx_io.h"
#include "include/spx_list.h"
#include "include/spx_string.h"
#include "include/spx_path.h"

#include "ydb_storage_dsync.h"
#include "ydb_storage_sync.h"
#include "ydb_storage_runtime.h"
#include "ydb_protocol.h"

err_t ydb_storage_dsync_start(struct ydb_storage_configurtion *c){
    g_ydb_storage_runtime->status = YDB_STORAGE_DSYNCING;
    SpxLog1(c->log,SpxLogMark,"begin disk sync,and storage state run to DSYNCING.");
    err_t err = 0;
    struct spx_list *mps = c->mountpoints;
    if(NULL == mps || 0 == mps->size){
        return 0;
    }
    string_t mpmarklog = spx_string_newlen(NULL,SpxLineSize,&err);
    if(NULL == mpmarklog){
        SpxLog2(c->log,SpxLogError,err,
                "alloc marklog filename for mp is fail.");
        return err;
    }
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_MAXSIZE; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(mps,i);
        if(!SpxStringIsNullOrEmpty(mp->path)){
            string_t newlog = spx_string_cat_printf(&err,mpmarklog,"%s%s.%02X.mark",
                    mp->path,SpxStringEndWith(mp->path,SpxPathDlmt) ? "" : SpxPathDlmtString,
                    mp->idx);
            if(NULL == newlog){
                SpxLog2(c->log,SpxLogError,err,
                        "build mark file for mp is fail.");
            }else{
                mpmarklog = newlog;
                if(!SpxFileExist(mpmarklog)){
                }
            }
            spx_string_clear(mpmarklog);
        }
    }
    spx_string_free(mpmarklog);
}



