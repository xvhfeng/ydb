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

#include "ydb_storage_configurtion.h"
#include "ydb_storage_mountpoint.h"
#include "ydb_storage_sync.h"
#include "ydb_storage_runtime.h"

/*
 * note:please make sure the mountpoint of storages in the same syncgroup
 * are the same,must is the disksize ,freesize,and blocksize of disk.
 */
err_t ydb_storage_dsync_check(struct ydb_storage_configurtion *c,
        struct ydb_storage_runtime *srt){/*{{{*/
    err_t err = 0;
    ydb_storage_mprtf_reader(c);
    int i = 0;
    for( ; i< YDB_STORAGE_MOUNTPOINT_COUNT; i++){
        struct ydb_storage_mountpoint *mp = spx_list_get(c->mountpoints,i);
        if(NULL != mp && !SpxStringIsNullOrEmpty(mp->path) && mp->isusing){
            if(0 == mp->last_freesize){
                //the mp is adding just now,and for expand the storage size
                //it is not the new-adding for mp of badly
                mp->need_dsync = false;
            } else {
                if(mp->need_dsync &&
                        (mp->last_freesize + c->disksync_busysize >= mp->freesize)) {
                    mp->need_dsync = false;
                } else {
                    srt->dsync_mps[i] = 1;
                    srt->need_dsync = true;
                }
            }
        }
    }
    return err;
}/*}}}*/

err_t ydb_storage_dsync_begin(struct ydb_storage_configurtion *c,
        struct ydb_storage_runtime *srt){
    err_t err = 0;
    srt->status = YDB_STORAGE_DSYNCING;
    if(!srt->need_dsync){
        return 0;
    }

    return err;
}

spx_private err_t ydb_storage_dsync_query_sync_with_storage(
