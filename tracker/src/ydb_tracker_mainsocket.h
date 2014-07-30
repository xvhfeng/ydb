/*
 * =====================================================================================
 *
 *       Filename:  ydb_tracker_mainsocket.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/30 18时06分00秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#ifndef _YDB_TRACKER_MAINSOCKET_H_
#define _YDB_TRACKER_MAINSOCKET_H_
#ifdef __cplusplus
extern "C" {
#endif

#include <pthread.h>

#include "include/spx_types.h"

#include "ydb_tracker_configurtion.h"
pthread_t ydb_tracker_mainsocket_thread_new(SpxLogDelegate *log,struct ydb_tracker_configurtion *c,err_t *err);

#ifdef __cplusplus
}
#endif
#endif
