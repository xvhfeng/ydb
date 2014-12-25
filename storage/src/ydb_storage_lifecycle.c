/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_start.c
 *        Created:  2014/07/08 16时16分43秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>

#include "spx_types.h"
#include "spx_properties.h"
#include "spx_defs.h"
#include "spx_io.h"
#include "spx_time.h"
#include "spx_path.h"
#include "spx_list.h"
#include "spx_string.h"
#include "spx_limits.h"
#include "spx_threadpool.h"
#include "spx_periodic.h"

#include "ydb_protocol.h"

#include "ydb_storage_configurtion.h"
#include "ydb_storage_disksync.h"
#include "ydb_storage_mountpoint.h"
#include "ydb_storage_runtime.h"
#include "ydb_storage_sync.h"
#include "ydb_storage_lifecycle.h"

#define YDB_STORAGE_NORMAL 0
#define YDB_STORAGE_INITING 1
#define YDB_STORAGE_ACCEPTING 2
#define YDB_STORAGE_DSYNCING 3
#define YDB_STORAGE_DSYNCED 4
#define YDB_STORAGE_CSYNCING 5
#define YDB_STORAGE_RUNNING 6
#define YDB_STORAGE_CLOSING 7
#define YDB_STORAGE_CLOSED 8



//write status file:runtime,mountpoint,sync runtime
void *ydb_storage_startup_runtime_flush(
        void *arg){/*{{{*/
    if(NULL == arg){
        return NULL;
    }
    SpxTypeConvert2(struct ydb_storage_runtime,srt,arg);
    SpxTypeConvert2(struct ydb_storage_configurtion,c,srt->c);
    if(YDB_STORAGE_RUNNING != srt->status){
        if( ydb_storage_sync_consistency(c)){
            srt->status = YDB_STORAGE_RUNNING;
        }
    }
    SpxLogFmt1(c->log,SpxLogMark,
            "now storage state:%s.",
            ydb_state_desc[srt->status]);

    if(YDB_STORAGE_RUNNING == srt->status){
        ydb_storage_runtime_flush(srt);
        ydb_storage_mprtf_writer(c);
    }
    ydb_storage_sync_state_writer(c);
    return NULL;
}/*}}}*/

err_t ydb_storage_lifecycle_shutdown(
        ){
    err_t err = 0;

    return err;
}
