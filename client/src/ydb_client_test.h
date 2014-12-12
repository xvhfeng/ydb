#ifndef _YDB_CLIENT_TEST_H_
#define _YDB_CLIENT_TEST_H_
#ifdef __cplusplus
extern "C" {
#endif

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
#include "spx_defs.h"
#include "spx_time.h"

#include "ydb_client.h"

err_t ydb_client_upload_test(
        char *groupname,
        char *tracker,
        char *local_suffix,
        char *local_fname,
        char *write_fpath,
        u32_t timeout,
        bool_t isprintf
        );

err_t ydb_client_delete_test(
        char *groupname,
        char *tracker,
        char *local_suffix,
        char *local_fname,
        u32_t timeout,
        bool_t isprintf
        );

err_t ydb_client_modify_c2c_test(
        char *groupname,
        char *tracker,
        char *local_suffix,
        char *local_fname,
        char *write_fpath,
        char *modify_suffix,
        char *modify_local_fname,
        char *modify_write_fpath,
        u32_t timeout,
        bool_t isprintf
        );


#ifdef __cplusplus
}
#endif
#endif
