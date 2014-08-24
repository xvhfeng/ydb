/*
 * =====================================================================================
 *
 * this software or lib may be copied only under the terms of the gnu general
 * public license v3, which may be found in the source kit.
 *
 *       Filename:  ydb_storage_mainsocket.h
 *        Created:  2014/07/31 10时47分59秒
 *         Author:  Seapeak.Xu (seapeak.cnblog.com), xvhfeng@gmail.com
 *        Company:  Tencent Literature
 *         Remark:
 *
 * =====================================================================================
 */
#ifndef _YDB_STORAGE_MAINSOCKET_H_
#define _YDB_STORAGE_MAINSOCKET_H_
#ifdef __cplusplus
extern "C" {
#endif


#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#include "include/spx_types.h"

#include "ydb_storage_configurtion.h"

pthread_t ydb_storage_mainsocket_thread_new(SpxLogDelegate *log,struct ydb_storage_configurtion *c,err_t *err);

#ifdef __cplusplus
}
#endif
#endif
