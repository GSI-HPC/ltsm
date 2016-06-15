/* 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>. 
 */

/*
 * Copyright (c) 2016, Thomas Stibor <t.stibor@gsi.de>
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <libgen.h>
#include <dirent.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/param.h>
#include "tsmapi.h"
#include "log.h"
#include "qarray.h"

#define DIR_PERM (S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)

#define OBJ_TYPE(type)							\
	(DSM_OBJ_FILE == type ? "DSM_OBJ_FILE" :			\
	(DSM_OBJ_DIRECTORY == type ? "DSM_OBJ_DIRECTORY" :		\
	(DSM_OBJ_RESERVED1 == type ? "DSM_OBJ_RESERVED1" :		\
	(DSM_OBJ_RESERVED2 == type ? "DSM_OBJ_RESERVED2" :		\
	(DSM_OBJ_RESERVED3 == type ? "DSM_OBJ_RESERVED3" :		\
	(DSM_OBJ_WILDCARD == type ? "DSM_OBJ_WILDCARD" :		\
	(DSM_OBJ_ANY_TYPE == type ? "DSM_OBJ_ANY_TYPE" : "UNKNOWN")))))))

static dsUint32_t handle;
static char rcmsg[DSM_MAX_RC_MSG_LENGTH + 1] = {0};
static dsUint16_t  max_obj_per_txn;
static dsUint32_t  max_bytes_per_txn;

#define TSM_GET_MSG(rc)					\
do {							\
	memset(&rcmsg, 0, DSM_MAX_RC_MSG_LENGTH + 1);	\
	dsmRCMsg(handle, rc, rcmsg);			\
	rcmsg[strlen(rcmsg)-1] = '\0';			\
} while (0)

#define TSM_ERROR(rc, func)					\
do {								\
	TSM_GET_MSG(rc);					\
	CT_ERROR(0, "%s: handle: %d %s", func, handle, rcmsg);	\
} while (0)

#define TSM_TRACE(rc, func)					\
do {								\
	TSM_GET_MSG(rc);					\
	CT_TRACE("%s: handle: %d %s", func, handle, rcmsg);	\
} while (0)

off_t to_off_t(const dsStruct64_t size)
{
	off_t res;
	res = (off_t)size.hi << 32 | size.lo;

	return res;
}

dsStruct64_t to_dsStruct64_t(const off_t size)
{
	dsStruct64_t res;
	res.lo = (dsUint32_t)size;
	res.hi = (dsUint32_t)((off_t)size >> 32);
    
	return res;
}

static void date_to_str(char *str, const dsmDate *date)
{
	sprintf(str, "%i/%i/%i %i:%i:%i",
		date->year,
		(dsInt16_t)date->month,
		(dsInt16_t)date->day,
		(dsInt16_t)date->hour,
		(dsInt16_t)date->minute,
		(dsInt16_t)date->second);
}

static dsInt16_t mkdir_p(const char *path)
{
	int rc = 0;
	size_t i = 0;
	size_t len = strlen(path);

	if (path[i] == '/')
		i++;
    
	char sub_path[len + 1];
	bzero(sub_path, len + 1);

	while (i <= len) {
		while (i <= len && path[i++] != '/');
		memcpy(sub_path, &path[0], i++);

		mode_t process_mask = umask(0);
		rc = mkdir(sub_path, DIR_PERM);
		umask(process_mask);

		if (rc < 0 && errno != EEXIST)
			return rc;
	}
	/* If directory already exists return success. */
	rc = rc < 0 && errno == EEXIST ? 0 : rc;
    
	return rc;
}

static dsInt16_t tsm_open_fstream(const char *fs, const char *hl, const char *ll, FILE **fstream)
{
	dsInt16_t rc;
	char *filename;
	unsigned int str_length = strlen(fs) + strlen(hl) + strlen(ll) + 1;
	filename = malloc(sizeof(char) * str_length);
	if (!filename) {
		rc = errno;
		CT_ERROR(rc, "malloc");
		return rc;
	}

	snprintf(filename, str_length, "%s%s%s", fs, hl, ll);
	CT_INFO("fs/hl/ll filename: %s\n", filename);

	rc = mkdir_p(hl);
	if (rc) {
		CT_ERROR(rc, "mkdir_p: %s", hl);
		goto clean_up;
	}

	*fstream = fopen(filename, "w");
	if (!fstream) {
		rc = errno;
		CT_ERROR(rc, "fopen: %s", filename);
		goto clean_up;
	}
	rc = DSM_RC_SUCCESSFUL;
    
clean_up:
	if (filename)
		free(filename);

	return rc;
}

static dsInt16_t tsm_close_fstream(FILE **fstream)
{
	dsInt16_t rc;
    
	if (*fstream) {
		rc = fclose(*fstream);
		if (rc != 0) {
			rc = errno;
			CT_ERROR(rc, "fclose");
		} else {
			*fstream = NULL;
			rc = DSM_RC_SUCCESSFUL;
		}
	} else
		rc = DSM_RC_SUCCESSFUL;	/* FILE was already closed. */

	return rc;
}

static dsInt16_t retrieve_file_obj(qryRespArchiveData *query_data,
				   obj_info_t *obj_info)
{
	dsInt16_t rc;
	FILE *fstream = NULL;
	char *buf = NULL;
	DataBlk dataBlk;

	rc = tsm_open_fstream(query_data->objName.fs,
			      query_data->objName.hl,
			      query_data->objName.ll, &fstream);
	if (rc != DSM_RC_SUCCESSFUL)
		goto clean_up;

	buf = malloc(sizeof(char) * TSM_BUF_LENGTH);
	if (!buf) {
		rc = errno;
		CT_ERROR(rc, "malloc");
		goto clean_up;
	}

	dataBlk.stVersion = DataBlkVersion;
	dataBlk.bufferLen = TSM_BUF_LENGTH;
	dataBlk.numBytes  = 0;
	dataBlk.bufferPtr = buf; /* Now we have a valid buffer pointer. */
	memset(dataBlk.bufferPtr, 0, TSM_BUF_LENGTH);

	off_t obj_size = to_off_t(obj_info->size);
	off_t obj_size_write = 0;

	/* Request data with a single dsmGetObj call, otherwise data is larger and we need
	   additional dsmGetData calls. */
	rc = dsmGetObj(handle, &(query_data->objId), &dataBlk);
	TSM_TRACE(rc, "dsmGetObj");
	dsBool_t done = bFalse;
	while (!done) {

		/* Note: dataBlk.numBytes always returns TSM_BUF_LENGTH (65536). */
		if (!(rc == DSM_RC_MORE_DATA || rc == DSM_RC_FINISHED)) {
			TSM_ERROR(rc, "dsmGetObj or dsmGetData");
			done = bTrue;
		} else {
			obj_size_write = obj_size < TSM_BUF_LENGTH ? obj_size : TSM_BUF_LENGTH;
			fwrite(buf, 1, obj_size_write, fstream);

			if (obj_size < TSM_BUF_LENGTH)
				done = bTrue;
			else {
				obj_size -= TSM_BUF_LENGTH;
				dataBlk.numBytes = 0;
				rc = dsmGetData(handle, &dataBlk);
				TSM_TRACE(rc, "dsmGetData");
			}
		}
	} /* End while (!done) */

	rc = dsmEndGetObj(handle);
	TSM_TRACE(rc, "dsmEndGetObj");
	if (rc != DSM_RC_SUCCESSFUL)
		TSM_ERROR(rc, "dsmEndGetObj");

clean_up:
	if (buf)
		free(buf);
	if (fstream)
		tsm_close_fstream(&fstream);

	return rc;
}

void tsm_print_query_node(const qryRespArchiveData *qry_resp_arv_data,
			  const unsigned long n)
{
	char ins_str_date[128] = {0};
	char exp_str_date[128] = {0};

	date_to_str(ins_str_date, &(qry_resp_arv_data->insDate));
	date_to_str(exp_str_date, &(qry_resp_arv_data->expDate));

	obj_info_t obj_info;
	memcpy(&obj_info, (char *)qry_resp_arv_data->objInfo, qry_resp_arv_data->objInfolen);

	printf("object # %lu\n"
	       "fs: %s, hl: %s, ll: %s\n"
	       "object id (hi,lo)                          : (%u,%u)\n"
	       "object info length                         : %d\n"
	       "object info size (hi,lo)                   : (%u,%u)\n"
	       "object type                                : %s\n"
	       "object magic id                            : %d\n"
	       "archive description                        : %s\n"
	       "owner                                      : %s\n"
	       "insert date                                : %s\n"
	       "expiration date                            : %s\n"
	       "restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (%u,%u,%u,%u,%u)\n"
	       "estimated size (hi,lo)                     : (%u,%u)\n",
	       n,
	       qry_resp_arv_data->objName.fs,
	       qry_resp_arv_data->objName.hl,
	       qry_resp_arv_data->objName.ll,
	       qry_resp_arv_data->objId.hi,
	       qry_resp_arv_data->objId.lo,
	       qry_resp_arv_data->objInfolen,
	       obj_info.size.hi,
	       obj_info.size.lo,
	       OBJ_TYPE(qry_resp_arv_data->objName.objType),
	       obj_info.magic,
	       qry_resp_arv_data->descr,
	       qry_resp_arv_data->owner,
	       ins_str_date,
	       exp_str_date,
	       qry_resp_arv_data->restoreOrderExt.top,
	       qry_resp_arv_data->restoreOrderExt.hi_hi,
	       qry_resp_arv_data->restoreOrderExt.hi_lo,
	       qry_resp_arv_data->restoreOrderExt.lo_hi,
	       qry_resp_arv_data->restoreOrderExt.lo_lo,
	       qry_resp_arv_data->sizeEstimate.hi,
	       qry_resp_arv_data->sizeEstimate.lo);
}

dsInt16_t tsm_init(login_t *login)
{
	dsmInitExIn_t initIn;
	dsmInitExOut_t initOut;
	dsmApiVersionEx apiApplVer;
	dsmApiVersion dsmApiVer;
	dsInt16_t rc;
    
	memset(&initIn, 0, sizeof(dsmInitExIn_t));
	memset(&initOut, 0, sizeof(dsmInitExOut_t));
	memset(&apiApplVer, 0, sizeof(dsmApiVersionEx));
	memset(&dsmApiVer, 0, sizeof(dsmApiVersion));
    
	dsmApiVer.version   = DSM_API_VERSION;
	dsmApiVer.release   = DSM_API_RELEASE;
	dsmApiVer.level     = DSM_API_LEVEL  ;
	apiApplVer.version  = DSM_API_VERSION;
	apiApplVer.release  = DSM_API_RELEASE;
	apiApplVer.level    = DSM_API_LEVEL  ;
	apiApplVer.subLevel = DSM_API_SUBLEVEL;

	initIn.stVersion        = dsmInitExInVersion;
	initIn.apiVersionExP    = &apiApplVer;
	initIn.clientNodeNameP  = login->node;
	initIn.clientOwnerNameP = login->username;
	initIn.clientPasswordP  = login->password;
	initIn.applicationTypeP = login->platform;
	initIn.configfile       = NULL;
	initIn.options          = login->options;
	initIn.userNameP        = NULL; /* Administrative user. */
	initIn.userPasswordP    = NULL; /* Administrative password. */

	rc = dsmInitEx(&handle, &initIn, &initOut);
	TSM_TRACE(rc, "dsmInitEx");
	if (rc) {
		TSM_ERROR(rc, "dsmInitEx");
		return rc;
	}

	regFSData reg_fs_data;
	memset(&reg_fs_data, 0, sizeof(reg_fs_data));
	reg_fs_data.fsName = "/";
	reg_fs_data.fsType = "/";
	reg_fs_data.capacity.lo = 0;
	reg_fs_data.capacity.hi = 0;
	reg_fs_data.occupancy.lo = 0;
	reg_fs_data.occupancy.hi = 0;
	reg_fs_data.stVersion = regFSDataVersion;
	strcpy(reg_fs_data.fsAttr.unixFSAttr.fsInfo, "fsinfo");
	reg_fs_data.fsAttr.unixFSAttr.fsInfoLength = strlen("fsinfo");
    
	rc = dsmRegisterFS(handle, &reg_fs_data);
	TSM_TRACE(rc, "dsmRegisterFS");
	if (rc == DSM_RC_FS_ALREADY_REGED || rc == DSM_RC_OK)
		return DSM_RC_OK;
    
	TSM_ERROR(rc, "dsmRegisterFS");
	return rc;
}

void tsm_quit()
{
	dsmTerminate(handle);
}

dsInt16_t tsm_query_session_info()
{
	optStruct dsmOpt;
	dsInt16_t rc;
	rc = dsmQuerySessOptions(handle, &dsmOpt);
	TSM_TRACE(rc, "dsmQuerySessOptions");
	if (rc) {
		TSM_ERROR(rc, "dsmQuerySessOptions");
		return rc;
	}

	CT_INFO("DSMI_DIR \t: %s\n"
		 "DSMI_CONFIG \t: %s\n"
		 "serverName \t: %s\n"
		 "commMethod \t: %d\n"
		 "serverAddress \t: %s\n"
		 "nodeName \t: %s\n"
		 "compress \t: %d\n"
		 "compressalways \t: %d\n"
		 "passwordAccess \t: %d\n",
		 dsmOpt.dsmiDir, dsmOpt.dsmiConfig,
		 dsmOpt.serverName, dsmOpt.commMethod,
		 dsmOpt.serverAddress, dsmOpt.nodeName,
		 dsmOpt.compression, dsmOpt.compressalways,
		 dsmOpt.passwordAccess);
    
	ApiSessInfo dsmSessInfo;
	memset(&dsmSessInfo, 0, sizeof(ApiSessInfo));

	rc = dsmQuerySessInfo(handle, &dsmSessInfo);
	TSM_TRACE(rc, "dsmQuerySessInfo");
	if (rc) {
		TSM_ERROR(rc, "dsmQuerySessInfo");
		return rc;
	}

	max_obj_per_txn = dsmSessInfo.maxObjPerTxn;
	max_bytes_per_txn = dsmSessInfo.maxBytesPerTxn;

	CT_INFO("\n"
		 "Server's ver.rel.lev       : %d.%d.%d.%d\n"
		 "ArchiveRetentionProtection : %s\n",
		 dsmSessInfo.serverVer, dsmSessInfo.serverRel, 
		 dsmSessInfo.serverLev, dsmSessInfo.serverSubLev,
		 dsmSessInfo.archiveRetentionProtection ? "Yes" : "No");
	CT_INFO("\n"
		 "Max number of multiple objects per transaction: %d\n"
		 "Max number of Bytes per transaction: %d\n"
		 "dsmSessInfo.fsdelim: %c\ndsmSessInfo.hldelim: %c\n",
		 dsmSessInfo.maxObjPerTxn, 
		 dsmSessInfo.maxBytesPerTxn,
		 dsmSessInfo.fsdelim, dsmSessInfo.hldelim);

	dsmApiVersionEx qry_lib_ver;
	memset(&qry_lib_ver, 0, sizeof(qry_lib_ver));
	dsmQueryApiVersionEx(&qry_lib_ver);

	dsUint32_t app_ver = 0;
	dsUint32_t lib_ver = 0;
	app_ver = (DSM_API_VERSION * 10000) + (DSM_API_RELEASE * 1000) +
		(DSM_API_LEVEL * 100) + (DSM_API_SUBLEVEL);
	lib_ver = (qry_lib_ver.version * 10000) + (qry_lib_ver.release * 1000) +
		(qry_lib_ver.level * 100) + (qry_lib_ver.subLevel);

	if (lib_ver < app_ver) {
		rc = DSM_RC_UNSUCCESSFUL;
		TSM_ERROR(rc, "The TSM API library is lower than the application version\n"
			    "Install the current library version.");
	} 

	CT_TRACE("API Library Version = %d.%d.%d.%d\n",
		 qry_lib_ver.version,
		 qry_lib_ver.release,
		 qry_lib_ver.level,
		 qry_lib_ver.subLevel);

	return rc;
}

dsInt16_t tsm_query_hl_ll(const char *fs, const char *hl, const char *ll, const char *desc, dsBool_t display)
{
	qryArchiveData qry_ar_data;
	dsmObjName obj_name;
	dsInt16_t rc;

	strcpy(obj_name.fs, fs);
	strcpy(obj_name.hl, hl);
	strcpy(obj_name.ll, ll);
	obj_name.objType = DSM_OBJ_ANY_TYPE;

	/* Fill up query structure. */
	qry_ar_data.stVersion = qryArchiveDataVersion;
	qry_ar_data.insDateLowerBound.year = DATE_MINUS_INFINITE;
	qry_ar_data.insDateUpperBound.year = DATE_PLUS_INFINITE;
	qry_ar_data.expDateLowerBound.year = DATE_MINUS_INFINITE;
	qry_ar_data.expDateUpperBound.year = DATE_PLUS_INFINITE;
	qry_ar_data.descr = desc == NULL || strlen(desc) == 0 ? "*" : (char *)desc;
	qry_ar_data.owner = "";  /* Omit owner. */
	qry_ar_data.objName = &obj_name;

	CT_INFO("tsm_query_archive with settings\n"
		 "fs: %s\n"
		 "hl: %s\n"
		 "ll: %s\n"
		 "owner: %s\n"
		 "descr: %s\n",
		 qry_ar_data.objName->fs,
		 qry_ar_data.objName->hl,
		 qry_ar_data.objName->ll,
		 qry_ar_data.owner,
		 qry_ar_data.descr);

	rc = dsmBeginQuery(handle, qtArchive, &qry_ar_data);
	TSM_TRACE(rc, "dsmBeginQuery");
	if (rc) {
		TSM_ERROR(rc, "dsmBeginQuery");
		goto clean_up;
	}

	qryRespArchiveData qry_resp_ar_data;
	DataBlk data_blk;
	data_blk.stVersion = DataBlkVersion;
	data_blk.bufferLen = sizeof(qry_resp_ar_data);
	data_blk.bufferPtr = (char *)&qry_resp_ar_data;
	qry_resp_ar_data.stVersion = qryRespArchiveDataVersion;

	dsBool_t done = bFalse;
	unsigned long n = 0;
	while (!done) {
		rc = dsmGetNextQObj(handle, &data_blk);
		TSM_TRACE(rc, "dsmGetNextQObj");
	
		if (((rc == DSM_RC_OK) || (rc == DSM_RC_MORE_DATA) || (rc == DSM_RC_FINISHED))
		    && data_blk.numBytes) {

			qry_resp_ar_data.objInfo[qry_resp_ar_data.objInfolen] = '\0';
	    
			if (display)	/* If query is only for printing, we are not filling the query array. */
				tsm_print_query_node(&qry_resp_ar_data, ++n);
			else {
				rc = add_query(&qry_resp_ar_data);
				if (rc) {
					CT_ERROR(0, "add_query");
					goto clean_up;
				}
			}
		} else {
			done = bTrue;
			if (rc == DSM_RC_ABORT_NO_MATCH)
				printf("tsm query has no match\n");
			else if (rc != DSM_RC_FINISHED)
				TSM_ERROR(rc, "dsmGetNextQObj");
		}
	}
    
	rc = dsmEndQuery(handle);
	if (rc)  {
		TSM_ERROR(rc, "dsmEndQuery");
		goto clean_up;
	}
    
clean_up:
	return rc;
}

dsInt16_t extract_hl_ll(const char *filename, char *hl, char *ll)
{
	if (filename == NULL || hl == NULL || ll == NULL)
		return DSM_RC_UNSUCCESSFUL;

	size_t len = strlen(filename);
	size_t i = len;

	while (i > 0 && filename[i] != '/' && i--);

	if (i > DSM_MAX_HL_LENGTH || (len - i) > DSM_MAX_LL_LENGTH)
		return DSM_RC_UNSUCCESSFUL;
    
	memcpy(hl, &filename[0], i);
	memcpy(ll, &filename[i], len - i);
    
	/* If i == 0 and either '/aaaa' or 'aaaa' was given. */
	if (i == 0 && !strncmp(&filename[0], &ll[0], len))
		hl[0] = '/';

	return DSM_RC_SUCCESSFUL;
}

static dsInt16_t tsm_archive(const char *fs, const char *filename, const char *desc, const dsBool_t is_file)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_HL_LENGTH + 1] = {0};
	FILE *file = NULL;
	dsmObjName objName;
	mcBindKey mcBindKey;
	sndArchiveData archData;
	obj_info_t *obj_info = NULL;
	ObjAttr objAttrArea;
	DataBlk dataBlkArea;
	size_t total_bytes = 0;
	char buf[TSM_BUF_LENGTH] = {0};
	size_t rbytes;
	dsUint16_t reason;
	dsBool_t success = bFalse;
	struct stat st_buf;
	char *resolved_filename = NULL;

	dataBlkArea.bufferPtr = NULL;
	objAttrArea.objInfo = NULL;

	resolved_filename = realpath(filename, resolved_filename);
	if (resolved_filename == NULL) {
		rc = errno;
		CT_ERROR(rc, "realpath failed: %s\n", filename);
		return rc;
	}

	rc = lstat(resolved_filename, &st_buf);
	if (rc) {
		CT_ERROR(rc, "stat");
		return rc;
	}
	if (!(S_ISREG(st_buf.st_mode) || S_ISDIR(st_buf.st_mode))) {
		rc = EPERM;
		CT_ERROR(rc, "no regular file or directory: %s", resolved_filename);
		return rc;
	}

	rc = extract_hl_ll(resolved_filename, hl, ll);
	if (rc != DSM_RC_SUCCESSFUL)
		goto clean_up;

	file = fopen(resolved_filename, "r");
	if (file == NULL) {
		rc = errno;
		CT_ERROR(rc, "fopen");
		goto clean_up;
	}

	memset(objName.fs, 0, DSM_MAX_FSNAME_LENGTH + 1);
	memset(objName.hl, 0, DSM_MAX_HL_LENGTH + 1);
	memset(objName.ll, 0, DSM_MAX_LL_LENGTH + 1);
	strcpy(objName.fs, fs);
	strcpy(objName.hl, hl);
	strcpy(objName.ll, ll);
	objName.objType = is_file ? DSM_OBJ_FILE : DSM_OBJ_DIRECTORY;

	/* A single transaction is an atomic action. Data sent within the
	   boundaries of a transaction is either committed to the system at the end of the
	   transaction or rolled back if the transaction ends prematurely.
	   Note: Objects that do not contain any bit data ( sizeEstimate=0 ) are not checked for
	   copy destination consistency. The following error is thrown:
	   dsmSendData: handle: 1 ANS0344E (RC2107) Cannot Send data with a zero byte sizeEstimate. */
	rc = dsmBeginTxn(handle);
	TSM_TRACE(rc, "dsmBeginTxn");
	if (rc) {
		TSM_ERROR(rc, "dsmBeginTxn");
		goto clean_up;
	}

	mcBindKey.stVersion = mcBindKeyVersion;
	rc = dsmBindMC(handle, &objName, stArchive, &mcBindKey);
	TSM_TRACE(rc, "dsmBeginMC");
	if (rc) {
		TSM_ERROR(rc, "dsmBindMC");
		goto clean_up_transaction;
	}
    
	archData.stVersion = sndArchiveDataVersion;
	if (desc && strlen(desc) <= DSM_MAX_DESCR_LENGTH)
		archData.descr = (char *)desc;
	else
		archData.descr = "\0";

#if 0
	/* TODO: Writing on the true TSM tape gives result:
	   ANS0226E (RC2019) The object owner is invalid.
	   Thus ignore owner for now. Note the owner is set by the true TSM tape library to
	   owner : hsmtest. */
	char uid_str[9] = {0};
	sprintf(uid_str, "%d", st_buf.st_uid);
	strcpy(objAttrArea.owner, uid_str);
#else
	strcpy(objAttrArea.owner, "\0");
#endif

	dsStruct64_t hi_lo_size = to_dsStruct64_t(st_buf.st_size);

	/* Note: If file is empty (filesize 0), then following error is thrown:
	   dsmSendData: handle: 1 ANS0344E (RC2107) Cannot Send data with a zero byte sizeEstimate.
	   Setting sizeEstimate.lo = 1 when archiving an empty file seems to fix the problem.
	*/
	objAttrArea.sizeEstimate.hi = hi_lo_size.hi;
	objAttrArea.sizeEstimate.lo = (hi_lo_size.hi == 0 && hi_lo_size.lo == 0) ? 1 : hi_lo_size.lo;
	objAttrArea.objCompressed = bFalse; /* Note: Currently no compression is supported. */
    
	obj_info = malloc(sizeof(obj_info_t));
	if (!obj_info) {
		rc = errno;
		CT_ERROR(rc, "malloc");
		goto clean_up_transaction;
	}
	obj_info->objType = objName.objType;
	obj_info->size.hi = hi_lo_size.hi;
	obj_info->size.lo = hi_lo_size.lo;
	obj_info->magic = MAGIC_ID; /* Indicate our data is stored. */

	/* Note: We have to make sure that
	   sizeof(obj_info_t) <= DSM_MAX_OBJINFO_LENGTH. */
	objAttrArea.objInfo = (char *)malloc(DSM_MAX_OBJINFO_LENGTH) ; 
	objAttrArea.objInfoLength = sizeof(obj_info_t);
	memcpy(objAttrArea.objInfo, (char *)obj_info, objAttrArea.objInfoLength);

	objAttrArea.stVersion = ObjAttrVersion;
	objAttrArea.mcNameP = NULL;

	rc = dsmSendObj(handle, stArchive, &archData, &objName, &objAttrArea, NULL);
	TSM_TRACE(rc, "dsmSendObj");
	if (rc) {
		TSM_ERROR(rc, "dsmSendObj");
		goto clean_up_transaction;
	}
   
	dataBlkArea.bufferPtr = (char *)malloc(sizeof(buf));
	dataBlkArea.stVersion = DataBlkVersion;
	dataBlkArea.bufferLen = sizeof(buf);
    
	while (is_file && !feof(file)) {
		rbytes = fread(dataBlkArea.bufferPtr, 1, dataBlkArea.bufferLen, file);
		if (ferror(file)) {
			rc = EBADF;
			CT_ERROR(rc, "fread");
			goto clean_up_sendobj;
		}
		total_bytes += rbytes;

		rc = dsmSendData(handle, &dataBlkArea);
		TSM_TRACE(rc, "dsmSendData");
		if (rc) {
			TSM_ERROR(rc, "dsmSendData");
			goto clean_up_sendobj;
		}
	}
	success = bTrue;
    
clean_up_sendobj:
	rc = dsmEndSendObj(handle);
	TSM_TRACE(rc, "dsmEndSendObj");
	if (rc) {
		TSM_ERROR(rc, "dsmEndSendObj");
		success = bFalse;
		goto clean_up_transaction;
	}

clean_up_transaction:
	rc = dsmEndTxn(handle, DSM_VOTE_COMMIT, &reason);
	TSM_TRACE(rc, "dsmEndTxn");
	if (rc || reason) {
		TSM_ERROR(rc, "dsmEndTxn");
		TSM_ERROR(reason, "dsmEndTxn reason");
		success = bFalse;
	}

	if (success) {
		CT_TRACE("\n*** successfully archived file: %s of size: %lu bytes with settings ***\n"
			 "fs: %s\n"
			 "hl: %s\n"
			 "ll: %s\n"
			 "desc: %s\n",
			 resolved_filename, total_bytes, objName.fs, objName.hl, objName.ll, desc);
	}

clean_up:
	if (file)
		fclose(file);
	if (obj_info)
		free(obj_info);
	if (objAttrArea.objInfo)
		free(objAttrArea.objInfo);
	if (dataBlkArea.bufferPtr)
		free(dataBlkArea.bufferPtr);
	if (resolved_filename)
		free(resolved_filename);

	return rc;
}

dsInt16_t tsm_archive_file(const char *fs, const char *filename, const char *desc)
{
	CT_INFO("fs: %s, filename: %s, desc: %s", fs, filename, desc);
	return tsm_archive(fs, filename, desc, bTrue);
}

static dsInt16_t tsm_del_obj(const qryRespArchiveData *qry_resp_ar_data)
{
	dsmDelInfo del_info;
	dsInt16_t rc;
	dsUint16_t reason;
    
	rc = dsmBeginTxn(handle);
	TSM_TRACE(rc, "dsmBeginTxn");
	if (rc != DSM_RC_SUCCESSFUL) {
		TSM_ERROR(rc, "dsmBeginTxn");
		return rc;
	}

	del_info.archInfo.stVersion = delArchVersion;
	del_info.archInfo.objId = qry_resp_ar_data->objId;

	rc = dsmDeleteObj(handle, dtArchive, del_info);
	TSM_TRACE(rc, "dsmDeleteObj");
	if (rc != DSM_RC_SUCCESSFUL) {
		TSM_ERROR(rc, "dsmDeleteObj");
		dsmEndTxn(handle, DSM_VOTE_COMMIT, &reason);
		return rc;
	}

	rc = dsmEndTxn(handle, DSM_VOTE_COMMIT, &reason);
	TSM_TRACE(rc, "dsmEndTxn");
	if (rc != DSM_RC_SUCCESSFUL) {
		TSM_ERROR(rc, "dsmEndTxn");
		return rc;
	}

	return rc;
}

dsInt16_t tsm_delete_hl_ll(const char *fs, const char *hl, const char *ll)
{
	dsInt16_t rc;

	rc = init_qarray();
	if (rc != DSM_RC_SUCCESSFUL)
		return rc;
    
	rc = tsm_query_hl_ll(fs, hl, ll, NULL, bFalse);
	if (rc != DSM_RC_SUCCESSFUL)
		goto clean_up;

	qryRespArchiveData query_data;
    
	for (unsigned long n = 0; n < qarray_size(); n++) {
		rc = get_query(&query_data, n);
		CT_INFO("get_query: %lu, rc: %d\n", n, rc);
		if (rc != DSM_RC_SUCCESSFUL) {
			errno = ENODATA; /* No data available */
			CT_ERROR(errno, "get_query");
			goto clean_up;
		}
		rc = tsm_del_obj(&query_data);
		if (rc != DSM_RC_SUCCESSFUL) {
			CT_WARN("\ncannot delete obj fs: %s\n"
				"                  hl: %s\n"
				"                  ll: %s\n",
				query_data.objName.fs,
				query_data.objName.hl,
				query_data.objName.ll);
		} else {
			CT_TRACE("\ndeleted obj fs: %s\n"
				"            hl: %s\n"
				"            ll: %s\n",
				query_data.objName.fs,
				query_data.objName.hl,
				query_data.objName.ll);
		}
	}

clean_up:    
	destroy_qarray();
	return rc;
}

dsInt16_t tsm_delete_file(const char *fs, const char *filename)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};
    
	rc = extract_hl_ll(filename, hl, ll);
	if (rc != DSM_RC_SUCCESSFUL)
		return rc;

	rc = tsm_delete_hl_ll(fs, hl, ll);

	return rc;
}

dsInt16_t tsm_query_file(const char *fs, const char *filename, const char *desc, dsBool_t display)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};
    
	rc = extract_hl_ll(filename, hl, ll);
	if (rc != DSM_RC_SUCCESSFUL)
		return rc;

	rc = tsm_query_hl_ll(fs, hl, ll, desc, display);

	return rc;
}

dsInt16_t tsm_retrieve_file(const char *fs, const char *filename, const char *desc)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};
    
	rc = extract_hl_ll(filename, hl, ll);
	if (rc != DSM_RC_SUCCESSFUL)
		return rc;

	rc = tsm_retrieve_hl_ll(fs, hl, ll, desc);

	return rc;
}

dsInt16_t tsm_retrieve_hl_ll(const char *fs, const char *hl, const char *ll, const char *desc)
{
	dsInt16_t rc;
	dsmGetList get_list;    

	get_list.objId = NULL;
    
	rc = init_qarray();
	if (rc != DSM_RC_SUCCESSFUL)
		goto clean_up;
    
	rc = tsm_query_hl_ll(fs, hl, ll, desc, bFalse);
	if (rc != DSM_RC_SUCCESSFUL)
		goto clean_up;

	/* Sort query replies to ensure that the data are read from the server in the most efficient order.
	   At least this is written on page Chapter 3. API design recommendations and considerations 57. */
	sort_qarray();

	/* TODO: Implement later also partialObjData handling. See page 56.*/    
	get_list.stVersion = dsmGetListVersion; /* dsmGetListVersion: Not using Partial Obj data,
						   dsmGetListPORVersion: Using Partial Obj data. */

	unsigned long c_begin = 0;
	unsigned long c_end = MIN(qarray_size(), DSM_MAX_GET_OBJ) - 1;
	unsigned int c_total = ceil((double)qarray_size() / (double)DSM_MAX_GET_OBJ);
	unsigned int c_cur = 0;
	unsigned long num_objs;
	qryRespArchiveData query_data;
	unsigned long i;
    
	do {
		num_objs = c_end - c_begin + 1;

		i = 0;
		get_list.numObjId = num_objs;
		get_list.objId = malloc(get_list.numObjId * sizeof(ObjID));
		if (!get_list.objId) {
			rc = errno;
			CT_ERROR(rc, "malloc");
			goto clean_up;
		}
		for (unsigned long c_iter = c_begin; c_iter <= c_end; c_iter++) {

			rc = get_query(&query_data, c_iter);
			CT_INFO("get_query: %lu, rc: %d\n", c_iter, rc);
			if (rc != DSM_RC_SUCCESSFUL) {
				errno = ENODATA; /* No data available */
				CT_ERROR(errno, "get_query");
				goto clean_up;
			}
			get_list.objId[i++] = query_data.objId;
		}
	
		rc = dsmBeginGetData(handle, bTrue /* mountWait */, gtArchive, &get_list);
		TSM_TRACE(rc, "dsmBeginGetData");
		if (rc) {
			TSM_ERROR(rc, "dsmBeginGetData");
			goto clean_up;
		}

		obj_info_t obj_info;
		for (unsigned long c_iter = c_begin; c_iter <= c_end; c_iter++) {

			rc = get_query(&query_data, c_iter);
			CT_INFO("get_query: %lu, rc: %d\n", c_iter, rc);
			if (rc != DSM_RC_SUCCESSFUL) {
				errno = ENODATA; /* No data available */
				CT_ERROR(errno, "get_query");
				goto clean_up;
			}
			memcpy(&obj_info,
			       (char *)&(query_data.objInfo),
			       query_data.objInfolen);
	
			CT_TRACE("\n"
				 "retrieving obj  fs          : %s\n"
				 "                hl          : %s\n"
				 "                ll          : %s\n"
				 "objtype                     : %s\n"
				 "objinfo magic               : %d\n"
				 "objinfo size bytes (hi,lo)  : (%u,%u)\n"
				 "estimated size bytes (hi,lo): (%u,%u)\n",
				 query_data.objName.fs,
				 query_data.objName.hl,
				 query_data.objName.ll,
				 OBJ_TYPE(query_data.objName.objType),
				 obj_info.magic,
				 obj_info.size.hi,
				 obj_info.size.lo,
				 query_data.sizeEstimate.hi,
				 query_data.sizeEstimate.lo);

			if (obj_info.magic != MAGIC_ID) {
				CT_WARN("Skip object due magic mismatch with MAGIC_ID: %d\n", obj_info.magic);
				continue;	/* Ignore this object and try next one. */
			}

			switch (query_data.objName.objType) {
			case DSM_OBJ_FILE: {
				rc = retrieve_file_obj(&query_data, &obj_info);
				CT_INFO("retrieve_file_obj, rc: %d\n", rc);
				if (rc != DSM_RC_SUCCESSFUL) {
					CT_ERROR(0, "retrieve_file_obj");
				}
			} break;
			case DSM_OBJ_DIRECTORY: {
				const unsigned int len = strlen(query_data.objName.fs) +
					strlen(query_data.objName.hl) +
					strlen(query_data.objName.ll) + 1;
				char directory[len];
				bzero(directory, sizeof(char) * len);
				snprintf(directory, len, "%s%s%s",
					 query_data.objName.fs,
					 query_data.objName.hl,
					 query_data.objName.ll);
				rc = mkdir_p(directory);
				CT_INFO("mkdir_p(%s)\n", directory);
				if (rc) {
					CT_ERROR(rc, "mkdir_p");
				}
				break;
			}
			default: {
				CT_WARN("Skip object due to unkown type %s\n", OBJ_TYPE(query_data.objName.objType));
				continue;
			}
			}
		} /* End-for iterate objid's. */

		/* There are no return codes that are specific to this call. */
		rc = dsmEndGetData(handle);
		TSM_TRACE(rc, "dsmEndGetData");

		c_cur++;
		c_begin = c_end + 1;
		/* Process last chunk and size not a multiple of DSM_MAX_GET_OBJ.*/
		c_end = (c_cur == c_total - 1 && qarray_size() % DSM_MAX_GET_OBJ != 0) ?
			c_begin + (qarray_size() % DSM_MAX_GET_OBJ) - 1 :
			c_begin + DSM_MAX_GET_OBJ - 1; /* Process not last chunk. */
	} while (c_cur < c_total);

clean_up:
	if (get_list.objId)
		free(get_list.objId);

	destroy_qarray();

	return rc;
}

#if 0
static dsInt16_t dir_walk(const char *fs, const char *directory, const char *desc)
{
	FTS *fts;
	int fts_options = FTS_COMFOLLOW | FTS_LOGICAL | FTS_NOCHDIR;
	char *paths[] = { (char *)directory, NULL };
	FTSENT *ftsent;
	int rc;

	fts = fts_open(paths, fts_options, NULL);
	if (fts == NULL) {
		rc = errno;
		CT_ERROR(rc, "fts_open");
		return rc;
	}

	while (1) {
		ftsent = fts_read(fts);
		if (ftsent == NULL) {
			if (errno == 0)
				break;
			else {
				rc = errno;
				CT_ERROR(rc, "fts_read");
				goto cleanup;
			}
		}

		switch (ftsent->fts_info) {
		case FTS_D:
			rc = tsm_archive(fs, ftsent->fts_path, desc, bFalse);
			break;
		case FTS_F:
			rc = tsm_archive(fs, ftsent->fts_path, desc, bTrue);
			break;
		default:
			rc = DSM_RC_UNSUCCESSFUL;
			break;
		}

		if (rc)
			CT_ERROR(rc, "tsm_archive failed: %s", ftsent->fts_path);
	}

cleanup:
	rc = fts_close(fts);
	if (rc) {
		rc = errno;
		CT_ERROR(rc, "fts_close");
	}

	return rc;
}
#endif

static dsInt16_t dir_walk(const char *fs, const char *fpath, const char *desc)
{
	int rc;
        DIR *dir;
        struct dirent *entry = NULL;
        char path[PATH_MAX] = {0};
        int path_len;
	int old_errno;

        dir = opendir(fpath);
        if (!dir) {
		rc = errno;
		CT_ERROR(rc, "opendir: %s", fpath);
		return rc;
        }
        while (1) {
		old_errno = errno;
                entry = readdir(dir);
                if (!entry) {
			/* End of dir stream, NULL is returned and errno
			   is not changed. */
			if (errno == old_errno) {
				rc = DSM_RC_SUCCESSFUL;
				break;
			}
			else {
				rc = errno;
				CT_ERROR(rc, "readdir: %s", fpath);
				break;
			}
                }
                path_len = snprintf(path, PATH_MAX, "%s/%s", fpath, entry->d_name);
                if (path_len >= PATH_MAX) {
                        CT_ERROR(ENAMETOOLONG, "path too long, ignoring: %s/%s", fpath, entry->d_name);
			continue;
                }

		switch (entry->d_type) {
		case DT_REG: {
			rc = tsm_archive(fs, path, desc, bTrue);
			if (rc)
				CT_ERROR(0, "tsm_archive file failed: %s", path);
			break;
		}
		case DT_DIR: {
			if (strcmp(entry->d_name, ".") == 0 ||
			    strcmp(entry->d_name, "..") == 0)
				continue;
			rc = tsm_archive(fs, path, desc, bFalse);
			if (rc)
				CT_ERROR(0, "tsm_archive directory failed: %s", path);
			rc = dir_walk(fs, path, desc);
			break;
		}
		default: /* Flag error on fifos, block/character devices, links, etc. */
			rc = EPERM;
			CT_ERROR(rc, "no regular file or directory: %s", path);
			continue;
			break;
		}
        }
        closedir(dir);

        return rc;
}

dsInt16_t tsm_archive_dir(const char *fs, const char *fpath, const char *desc)
{
	dsInt16_t rc;
	char *fpath_r = strdup(fpath);

	if (strlen(fpath_r) > 1 && strncmp(&fpath_r[strlen(fpath_r)-1], "/", 1) == 0)
		fpath_r[strlen(fpath_r)-1] = '\0';

	CT_INFO("fs: %s, fpath: %s, desc: %s\n", fs, fpath_r, desc);
	rc = dir_walk(fs, fpath_r, desc);

	return rc;
}
