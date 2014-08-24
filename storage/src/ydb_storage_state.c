/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_state.c
 *        Created:  2014/08/04 13时58分33秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */

#include <stdlib.h>
#include <stdio.h>

#include "include/spx_types.h"
#include "include/spx_alloc.h"
#include "include/spx_defs.h"
#include "include/spx_time.h"

#include "ydb_protocol.h"

#include "ydb_storage_state.h"


struct ydb_storage_state *g_ydb_storage_state = NULL;

struct ydb_storage_state *ydb_storage_state_init(SpxLogDelegate *log,err_t *err){
    struct ydb_storage_state *state = (struct ydb_storage_state *) \
                          spx_alloc_alone(sizeof(*state),err);
    if(NULL == state){
        SpxLog2(log,SpxLogError,*err,\
                "new ydb storage state is fail.");
    }
    state->ydb_storage_status = YDB_STORAGE_NORMAL;
    state->ydb_storage_first_start = spx_now();
    return state;
}
