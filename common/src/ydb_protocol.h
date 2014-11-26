/*
 * =====================================================================================
 *
 *       Filename:  ydb_protocol.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  2014/06/24 10时06分28秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (),
 *   Organization:
 *
 * =====================================================================================
 */
#ifndef _YDB_PROTOCOL_H_
#define _YDB_PROTOCOL_H_
#ifdef __cplusplus
extern "C" {
#endif

#define YDB_VERSION 1

//protocol

//client to tracker
#define YDB_C2T_QUERY_UPLOAD_STORAGE 1
#define YDB_C2T_QUERY_SELECT_STORAGE 2
#define YDB_C2T_QUERY_DELETE_STORAGE 3
#define YDB_C2T_QUERY_MODIFY_STORAGE 4

    //client to storage
#define YDB_C2S_UPLOAD 129
#define YDB_C2S_FIND 130
#define YDB_C2S_MODIFY 131
#define YDB_C2S_DELETE 132
#define YDB_C2S_SYNC_DELETE 133
#define YDB_C2S_SYNC_FILE 134

//storage to tracker
#define YDB_S2T_REGEDIT 257
#define YDB_S2T_HEARTBEAT 258
#define YDB_S2T_SHUTDOWN 259
#define YDB_S2T_QUERY_SYNC_STORAGES 260
#define YDB_S2T_QUERY_BASE_STORAGE 261
#define YDB_S2T_QUERY_SYNC_BEGIN_TIMESPAN 262

//storage to storage
#define YDB_S2S_QUERY_STORAGE_STATUS 385
#define YDB_S2S_SYNC_LOGFILE 386
#define YDB_S2S_DSYNC 387
#define YDB_S2S_CSYNC_ADD 388
#define YDB_S2S_CSYNC_DELETE 389
#define YDB_S2S_CSYNC_MODIFY 340
#define YDB_S2S_SYNC_OVER 341
#define YDB_S2S_CSYNC_BEGIN 342
#define YDB_S2S_RESTORE_CSYNC_OVER 343
#define YDB_S2S_QUERY_CSYNC_BEGINPOINT 344

//storage status code
#define YDB_STORAGE_NORMAL 0
#define YDB_STORAGE_INITING 1
#define YDB_STORAGE_ACCEPTING 2
#define YDB_STORAGE_DSYNCING 3
#define YDB_STORAGE_DSYNCED 4
#define YDB_STORAGE_CSYNCING 5
#define YDB_STORAGE_RUNNING 6
#define YDB_STORAGE_CLOSING 7
#define YDB_STORAGE_CLOSED 8


#define YDB_STORAGE_LOG_UPLOAD 'A'
#define YDB_STORAGE_LOG_MODIFY 'M'
#define YDB_STORAGE_LOG_DELETE 'D'

//const use in bosh tracker and storage
#define YDB_GROUPNAME_LEN 7
#define YDB_MACHINEID_LEN 7
#define YDB_SYNCGROUP_LEN 7
#define YDB_FILENAME_SUFFIX_SIZE 7
#define YDB_HASHCODE_SIZE 64
//reserve zone and for funtrue
#define YDB_RESERVEZONE_SIZE (4 * sizeog(u64_t))

#define YDB_CHUNKFILE_MEMADATA_SIZE \
    sizeof(char) + sizeof(u32_t) + sizeof(u64_t) + sizeof(u64_t) \
    + sizeof(u32_t) + sizeof(u64_t) + sizeof(u64_t) \
     + YDB_FILENAME_SUFFIX_SIZE + YDB_HASHCODE_SIZE \
    + YDB_RESERVEZONE_SIZE

#define YDB_CHUNKFILE_OFFSET_ISDELETE 0
#define YDB_CHUNKFILE_SIZE_ISDELETE sizeof(char)

#define YDB_CHUNKFILE_OFFSET_OPVER \
    YDB_CHUNKFILE_OFFSET_ISDELETE + YDB_CHUNKFILE_SIZE_ISDELETE
#define YDB_CHUNKFILE_SIZE_OPVER sizeof(u32_t)

#define YDB_CHUNKFILE_OFFSET_VERSION \
    YDB_CHUNKFILE_OFFSET_OPVER + YDB_CHUNKFILE_SIZE_OPVER
#define YDB_CHUNKFILE_SIZE_VERSION sizeof(u32_t)

#define YDB_CHUNKFILE_OFFSET_CREATETIME \
    YDB_CHUNKFILE_OFFSET_VERSION + YDB_CHUNKFILE_SIZE_VERSION
#define YDB_CHUNKFILE_SIZE_CREATETIME sizeof(u64_t)

#define YDB_CHUNKFILE_OFFSET_LASTMODIFYTIME \
    YDB_CHUNKFILE_OFFSET_CREATETIME + YDB_CHUNKFILE_SIZE_CREATETIME
#define YDB_CHUNKFILE_SIZE_LASTMODIFYTIME sizeof(u64_t)

#define YDB_CHUNKFILE_OFFSET_TOTALLEN \
    YDB_CHUNKFILE_OFFSET_LASTMODIFYTIME + YDB_CHUNKFILE_SIZE_LASTMODIFYTIME
#define YDB_CHUNKFILE_SIZE_TOTALLEN sizeof(u64_t)

#define YDB_CHUNKFILE_OFFSET_REALSIZE \
    YDB_CHUNKFILE_OFFSET_TOTALLEN + YDB_CHUNKFILE_SIZE_TOTALLEN
#define YDB_CHUNKFILE_SIZE_REALSIZE sizeof(u64_t)

#define YDB_CHUNKFILE_OFFSET_SUFFIX \
    YDB_CHUNKFILE_OFFSET_REALSIZE + YDB_CHUNKFILE_SIZE_REALSIZE
#define YDB_CHUNKFILE_SIZE_SUFFIX YDB_FILENAME_SUFFIX_SIZE

#define YDB_CHUNKFILE_OFFSET_HASHCODE \
    YDB_CHUNKFILE_OFFSET_SUFFIX + YDB_CHUNKFILE_SIZE_SUFFIX
#define YDB_CHUNKFILE_SIZE_HASHCODE YDB_HASHCODE_SIZE

#define YDB_CUNKFILE_OFFSET_RESERVEZONE \
    YDB_CHUNKFILE_OFFSET_HASHCODE + YDB_CUNKFILE_SIZE_HASHCODE
#define YDB_CHUNKFILE_SIZE_RESERVEZONE YDB_RESERVEZONE_SIZE

#define YDB_CHUNKFILE_OFFSET_BODY YDB_CHUNKFILE_MEMADATA_SIZE

#define YDB_TRANSPORT_UPLOAD_OFFSET_HASSUFFIX 0
#define YDB_TRANSPORT_UPLOAD_SIZE_HASSUFFIX 1

#define YDB_TRANSPORT_UPLOAD_OFFSET_SUFFIX \
    YDB_TRANSPORT_UPLOAD_OFFSET_HASSUFFIX + YDB_TRANSPORT_UPLOAD_SIZE_HASSUFFIX
#define YDB_TRANSPORT_UPLOAD_SIZE_SUFFIX YDB_FILENAME_SUFFIX

#define YDB_TRANSPORT_UPLOAD_OFFSET_BODYBUFFER(has_suffix) \
    has_suffix \
    ? YDB_TRANSPORT_UPLOAD_OFFSET_SUFFIX + YDB_TRANSPORT_UPLOAD_SIZE_SUFFIX \
    YDB_TRANSPORT_UPLOAD_OFFSET_HASSUFFIX + YDB_TRANSPORT_UPLOAD_SIZE_HASSUFFIX



#ifdef __cplusplus
}
#endif
#endif
