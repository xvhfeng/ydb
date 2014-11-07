#ifndef _YDB_STORAGE_MOUNTPOINT_H_
#define _YDB_STORAGE_MOUNTPOINT_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <stdio.h>

#include "spx_types.h"
#include "ydb_storage_configurtion.h"


err_t ydb_storage_mountpoint_init(struct ydb_storage_configurtion *c);
err_t ydb_storage_mprtf_writer(struct ydb_storage_configurtion *c);
err_t ydb_storage_mprtf_reader(struct ydb_storage_configurtion *c);



#ifdef __cplusplus
}
#endif
#endif
