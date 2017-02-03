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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/param.h>
#include "tsmapi.h"
#include "log.h"
#include "qarray.h"

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
static dsUint16_t max_obj_per_txn;
static dsUint32_t max_bytes_per_txn;

static dsBool_t do_recursive;
static dsBool_t use_latest;

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

/**
 * @brief Set internal boolean variable recursive to flag recursive archiving.
 *
 * @param[in] recursive Boolean variable flags whether to archive recursively.
 */
void set_recursive(const dsBool_t recursive)
{
	do_recursive = recursive;
}

/**
 * @brief Set internal boolean variable use_latest to process the most recent
 *        objects only.
 *
 *        If objects are archived multiple times (e.g. with different content),
 *        then add in the query array only those having the most recent date.
 *        That is, only objects will be retrieved or deleted which have the most
 *        recent insertion date.
 *
 * @param[in] latest Boolean variable flags whether to retrieve or delete the
 *                   most recent files/directories.
 */
void select_latest(const dsBool_t latest)
{
	use_latest = latest;
}

/**
 * @brief Convert dsStruct64_t to off64_t type.
 *
 * Convert dsStruct64_t struct that consists of fields
 * dsUint32_t hi; (Most significant 32 bits) and
 * dsUint32_t lo; (Least significant 32 bits) to off64_t.
 *
 * @param[in] size Type used to represent file/object sizes.
 * @return off64_t
 */
off64_t to_off64_t(const dsStruct64_t size)
{
	return (off64_t)size.hi << 32 | size.lo;
}

/**
 * @brief Convert off64_t to to_dsStruct64_t type.
 *
 * Convert off64_t to dsStruct64_t struct which consists of fields
 * dsUint32_t hi; (Most significant 32 bits) and
 * dsUint32_t lo; (Least significant 32 bits).
 *
 * @param[in] size Type used to represent file/object sizes.
 * @return dsStruct64_t Struct consists of {dsUint32_t hi, dsUint32_t lo};
 */
dsStruct64_t to_dsStruct64_t(const off64_t size)
{
	dsStruct64_t res;
	res.lo = (dsUint32_t)size;
	res.hi = (dsUint32_t)((off64_t)size >> 32);

	return res;
}

/**
 * @brief Fallback for determining d_type with lstat().
 *
 * Some file systems such as xfs return DT_UNKNOWN in dirent when calling
 * opendir(). Fallback with lstat() and set fields in dirent according to stat
 * information. Note: The TSM API can handle only regular files and directories.
 * For supporting all d_types types, define HANDLE_ALL_D_TYPES.
 *
 * @param entry Structure representing the next directory entry in the directory
 *              stream.
 * @return DSM_RC_SUCCESSFUL on success otherwise DSM_RC_UNSUCCESSFUL.
 */
static dsInt16_t fallback_dt_unknown(struct dirent *entry, const char *fpath)
{
	dsInt16_t rc;
	struct stat st_buf;

	rc = lstat(fpath, &st_buf);
	if (rc) {
		CT_ERROR(errno, "lstat");
		rc = DSM_RC_UNSUCCESSFUL;
	} else {
		switch (st_buf.st_mode & S_IFMT) {
		case S_IFREG : entry->d_type = DT_REG ; break;
		case S_IFDIR : entry->d_type = DT_DIR ; break;
#ifdef HANDLE_ALL_D_TYPES
		case S_IFLNK : entry->d_type = DT_LNK ; break;
		case S_IFCHR : entry->d_type = DT_CHR ; break;
		case S_IFBLK : entry->d_type = DT_BLK ; break;
		case S_IFIFO : entry->d_type = DT_FIFO; break;
		case S_IFSOCK: entry->d_type = DT_SOCK; break;
#endif
		}
		rc = DSM_RC_SUCCESSFUL;
	}
	return rc;
}

static void date_to_str(char *str, const dsmDate *date)
{
	sprintf(str, "%i/%02i/%02i %02i:%02i:%02i",
		date->year,
		(dsInt16_t)date->month,
		(dsInt16_t)date->day,
		(dsInt16_t)date->hour,
		(dsInt16_t)date->minute,
		(dsInt16_t)date->second);
}

static dsInt16_t mkdir_p(const char *path, const mode_t st_mode)
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
		rc = mkdir(sub_path, st_mode);
		umask(process_mask);

		if (rc < 0 && errno != EEXIST)
			return rc;
	}
	/* If directory already exists return success. */
	rc = rc < 0 && errno == EEXIST ? 0 : rc;

	return rc;
}


/**
 * @brief Extract from qryRespArchiveData fields fs, hl, ll and construct fpath.
 *
 * @param[in]  query_data Query data response containing fs, hl and ll.
 * @param[out] fpath      File path contructed and set from fs, hl and ll.
 * @return DSM_RC_SUCCESSFUL on success otherwise DSM_RC_UNSUCCESSFUL.
 */
static dsInt16_t extract_fpath(const qryRespArchiveData *query_data, char *fpath)
{
	if (fpath == NULL) {
		CT_WARN("fpath: %p", fpath);
		return DSM_RC_UNSUCCESSFUL;
	}

	const size_t len = strlen(query_data->objName.fs) +
		strlen(query_data->objName.hl) +
		strlen(query_data->objName.ll) + 1;

	if (len > PATH_MAX) {
		CT_ERROR(ENAMETOOLONG, "len > PATH_MAX");
		return DSM_RC_UNSUCCESSFUL;
	}

	snprintf(fpath, len, "%s%s%s", query_data->objName.fs,
		 query_data->objName.hl, query_data->objName.ll);
	CT_INFO("fs/hl/ll fpath: %s\n", fpath);

	return DSM_RC_SUCCESSFUL;
}

/**
 * @brief Retrieve and write object data into file descriptor.
 *
 * Given response of query (qryRespArchiveData) and
 * the corresponding object information (obj_info_t), retrieve
 * data from TSM storage and write data into file descriptor.
 *
 * @param[in] query_data Description of query_data
 * @param[in] obj_info   Description of obj_info
 * @return DSM_RC_SUCCESSFUL on success otherwise DSM_RC_UNSUCCESSFUL.
 */
static dsInt16_t retrieve_obj(qryRespArchiveData *query_data,
			      const obj_info_t *obj_info, int fd)
{
	char *buf = NULL;
	DataBlk dataBlk;
	dsBool_t done = bFalse;
	off64_t obj_size = to_off64_t(obj_info->size);
	ssize_t obj_size_written = 0;
	dsInt16_t rc;
	dsInt16_t rc_minor = 0;
	dsBool_t is_local_fd = bFalse;

	if (fd < 0) {
		/* If a regular file was archived, e.g. /dir1/dir2/data.txt,
		   then on the TSM storage only the object
		   hl: /dir1/dir2, ll: /data.txt is stored. In contrast to
		   IBM's dsmc tool where also the directories /dir1 and
		   /dir1/dir2 are stored as objects. Our approach saves us
		   two objects, however we have no st_mode information of /dir1
		   and /dir1/dir2, therefore use the default directory permission:
		   S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH */
		rc = mkdir_p(query_data->objName.hl,
			     S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
		CT_INFO("mkdir_p(%s)", query_data->objName.hl);
		if (rc) {
			CT_ERROR(rc, "mkdir_p");
			return DSM_RC_UNSUCCESSFUL;
		}

		char fpath[PATH_MAX + 1] = {0};
		rc = extract_fpath(query_data, &fpath[0]);
		if (rc != DSM_RC_SUCCESSFUL)
			return  DSM_RC_UNSUCCESSFUL;

		fd = open(fpath, O_WRONLY | O_TRUNC | O_CREAT, obj_info->st_mode);
		if (fd < 0) {
			CT_ERROR(errno, "open '%s'", fpath);
			return DSM_RC_UNSUCCESSFUL;
		}
		is_local_fd = bTrue;
	}

	buf = malloc(sizeof(char) * TSM_BUF_LENGTH);
	if (!buf) {
		CT_ERROR(errno, "malloc");
		return DSM_RC_UNSUCCESSFUL;
	}

	dataBlk.stVersion = DataBlkVersion;
	dataBlk.bufferLen = TSM_BUF_LENGTH;
	dataBlk.numBytes  = 0;
	dataBlk.bufferPtr = buf;
	memset(dataBlk.bufferPtr, 0, TSM_BUF_LENGTH);

	/* Request data with a single dsmGetObj call, otherwise data is larger and we need
	   additional dsmGetData calls. */
	rc = dsmGetObj(handle, &(query_data->objId), &dataBlk);
	TSM_TRACE(rc, "dsmGetObj");

	while (!done) {
		/* Note: dataBlk.numBytes always returns TSM_BUF_LENGTH (65536). */
		if (!(rc == DSM_RC_MORE_DATA || rc == DSM_RC_FINISHED)) {
			TSM_ERROR(rc, "dsmGetObj or dsmGetData");
			rc_minor = rc;
			done = bTrue;
		} else {
			obj_size_written = write(fd, buf, obj_size < TSM_BUF_LENGTH ? obj_size : TSM_BUF_LENGTH);
			if (obj_size_written < 0) {
				CT_ERROR(errno, "write");
				rc_minor = DSM_RC_UNSUCCESSFUL;
				goto cleanup;
			}

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

cleanup:
	rc = dsmEndGetObj(handle);
	TSM_TRACE(rc, "dsmEndGetObj");
	if (rc != DSM_RC_SUCCESSFUL)
		TSM_ERROR(rc, "dsmEndGetObj");

	if (buf)
		free(buf);

	if (is_local_fd && !(fd < 0)) {
		rc = close(fd);
		if (rc < 0) {
			CT_ERROR(errno, "close failed: %d", rc, fd);
			rc = DSM_RC_UNSUCCESSFUL;
		}

	}

	return (rc_minor ? DSM_RC_UNSUCCESSFUL : rc);
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

	fprintf(stdout, "object # %lu\n"
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

	fflush(stdout);
}

dsInt16_t tsm_init(login_t *login)
{
	dsmInitExIn_t init_in;
	dsmInitExOut_t init_out;
	dsmApiVersionEx libapi_ver;
	dsmAppVersion appapi_ver;
	dsInt16_t rc;

	memset(&init_in, 0, sizeof(dsmInitExIn_t));
	memset(&init_out, 0, sizeof(dsmInitExOut_t));

	libapi_ver = get_libapi_ver();
	appapi_ver = get_appapi_ver();

	init_in.stVersion        = dsmInitExInVersion;
	init_in.apiVersionExP    = &libapi_ver;
	init_in.clientNodeNameP  = login->node;
	init_in.clientOwnerNameP = login->owner;
	init_in.clientPasswordP  = login->password;
	init_in.applicationTypeP = login->platform;
	init_in.configfile       = NULL;
	init_in.options          = login->options;
	init_in.userNameP        = NULL; /* Administrative user. */
	init_in.userPasswordP    = NULL; /* Administrative password. */
	init_in.appVersionP      = &appapi_ver;

	rc = dsmInitEx(&handle, &init_in, &init_out);
	TSM_TRACE(rc, "dsmInitEx");
	if (rc) {
		TSM_ERROR(rc, "dsmInitEx");
		return rc;
	}

	regFSData reg_fs_data;
	memset(&reg_fs_data, 0, sizeof(reg_fs_data));
	reg_fs_data.fsName = login->fsname;
	reg_fs_data.fsType = login->fstype;
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

/**
 * @brief Return application client API version.
 *
 * Returns API version number of the application client which is entered
 * in the compiled object code as a set of four constants defined in dsmapitd.h.
 *
 * @return dsmAppVersion
 */
dsmAppVersion get_appapi_ver()
{
	dsmAppVersion appapi_ver;
	memset(&appapi_ver, 0, sizeof(appapi_ver));
	appapi_ver.stVersion = appVersionVer;
	appapi_ver.applicationVersion = DSM_API_VERSION;
	appapi_ver.applicationRelease = DSM_API_RELEASE;
	appapi_ver.applicationLevel = DSM_API_LEVEL;
	appapi_ver.applicationSubLevel = DSM_API_SUBLEVEL;

	return appapi_ver;
}

/**
 * @brief Return version of the API library.
 *
 * Returns version of the API library that is installed and used.
 *
 * @return dsmApiVersionEx
 */
dsmApiVersionEx get_libapi_ver()
{
	dsmApiVersionEx libapi_ver;
	memset(&libapi_ver, 0, sizeof(libapi_ver));
	dsmQueryApiVersionEx(&libapi_ver);

	return libapi_ver;
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

	CT_INFO("\nDSMI_DIR \t: %s\n"
		 "DSMI_CONFIG \t: %s\n"
		 "serverName \t: %s\n"
		 "commMethod \t: %d\n"
		 "serverAddress \t: %s\n"
		 "nodeName \t: %s\n"
		 "compress \t: %d\n"
		 "compressalways \t: %d\n"
		 "passwordAccess \t: %d",
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

	dsmApiVersionEx libapi_ver_t = get_libapi_ver();
	dsmAppVersion appapi_ver_t = get_appapi_ver();
	dsUint32_t libapi_ver = (libapi_ver_t.version * 10000) +
		(libapi_ver_t.release * 1000) +
		(libapi_ver_t.level * 100) +
		libapi_ver_t.subLevel;
	dsUint32_t appapi_ver = (appapi_ver_t.applicationVersion * 10000) +
		(appapi_ver_t.applicationRelease * 1000) +
		(appapi_ver_t.applicationLevel * 100) +
		appapi_ver_t.applicationSubLevel;

	if (libapi_ver < appapi_ver) {
		rc = DSM_RC_UNSUCCESSFUL;
		TSM_ERROR(rc, "The TSM API library is lower than the application version\n"
			  "Install the current library version.");
	}

	CT_TRACE("IBM API library version = %d.%d.%d.%d\n",
		 libapi_ver_t.version,
		 libapi_ver_t.release,
		 libapi_ver_t.level,
		 libapi_ver_t.subLevel);

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

	CT_INFO("query structure\n"
		 "fs: %s\n"
		 "hl: %s\n"
		 "ll: %s\n"
		 "owner: %s\n"
		 "descr: %s",
		 qry_ar_data.objName->fs,
		 qry_ar_data.objName->hl,
		 qry_ar_data.objName->ll,
		 qry_ar_data.owner,
		 qry_ar_data.descr);

	rc = dsmBeginQuery(handle, qtArchive, &qry_ar_data);
	TSM_TRACE(rc, "dsmBeginQuery");
	if (rc) {
		TSM_ERROR(rc, "dsmBeginQuery");
		goto cleanup;
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
				rc = add_query(&qry_resp_ar_data, use_latest);
				if (rc) {
					CT_ERROR(0, "add_query");
					goto cleanup;
				}
			}
		} else if (rc == DSM_RC_UNKNOWN_FORMAT) {
			/* head over to next object if format error occurs when trying to access non api archived files */
                        CT_WARN("DSM_OBJECT not archived by API. Skipping Object!");
                } else {
			done = bTrue;
			if (rc == DSM_RC_ABORT_NO_MATCH)
				CT_MESSAGE("query has no match");
			else if (rc != DSM_RC_FINISHED)
				TSM_ERROR(rc, "dsmGetNextQObj");
		}
	}

	rc = dsmEndQuery(handle);
	if (rc)  {
		TSM_ERROR(rc, "dsmEndQuery");
		goto cleanup;
	}

cleanup:
	return rc;
}

static dsInt16_t extract_hl_ll(const char *fpath, char *hl, char *ll)
{
	size_t len, i;

	bzero(hl, DSM_MAX_HL_LENGTH + 1);
	bzero(ll, DSM_MAX_LL_LENGTH + 1);
	len = strlen(fpath);
	i = len;

	while (i > 0 && fpath[i] != '/' && i--);

	if (i > DSM_MAX_HL_LENGTH || (len - i) > DSM_MAX_LL_LENGTH)
	{
		CT_ERROR(EINVAL, "incorrect length");
		return DSM_RC_UNSUCCESSFUL;
	}

	memcpy(hl, &fpath[0], i);
	memcpy(ll, &fpath[i], len - i);

	/* If i == 0 and either '/aaaa' or 'aaaa' was given. */
	if (i == 0 && !strncmp(&fpath[0], &ll[0], len))
		hl[0] = '/';

	return DSM_RC_SUCCESSFUL;
}

static dsInt16_t obj_attr_prepare(ObjAttr *obj_attr,
				  const archive_info_t *archive_info)
{
	dsInt16_t rc;
	obj_attr->owner[0] = '\0';

	/* Note: If file is empty (filesize 0), then following error is thrown:
	   dsmSendData: handle: 1 ANS0344E (RC2107) Cannot Send data with a zero
	   byte sizeEstimate. Set thus size.lo to 1 byte when file is empty. */
	obj_attr->sizeEstimate.hi = archive_info->obj_info.size.hi;
	obj_attr->sizeEstimate.lo = (archive_info->obj_info.size.hi == 0 &&
				     archive_info->obj_info.size.lo == 0) ?
		1 : archive_info->obj_info.size.lo;

	obj_attr->stVersion = ObjAttrVersion;
	obj_attr->mcNameP = NULL;
	obj_attr->objCompressed = bFalse; /* Note: Currently no compression is supported. */
	obj_attr->objInfoLength = sizeof(obj_info_t);
	obj_attr->objInfo = (char *)malloc(obj_attr->objInfoLength);
	if (!obj_attr->objInfo) {
		rc = errno;
		CT_ERROR(rc, "malloc");
		return rc;
	}

	memcpy(obj_attr->objInfo, (char *)&(archive_info->obj_info), obj_attr->objInfoLength);

	return DSM_RC_SUCCESSFUL;
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

static dsInt16_t tsm_delete_hl_ll(const char *fs, const char *hl,
				  const char *ll)
{
	dsInt16_t rc;

	rc = init_qarray();
	if (rc != DSM_RC_SUCCESSFUL)
		return rc;

	rc = tsm_query_hl_ll(fs, hl, ll, NULL, bFalse);
	if (rc != DSM_RC_SUCCESSFUL)
		goto cleanup;

	qryRespArchiveData query_data;

	for (unsigned long n = 0; n < qarray_size(); n++) {
		rc = get_query(&query_data, n);
		CT_INFO("get_query: %lu, rc: %d", n, rc);
		if (rc != DSM_RC_SUCCESSFUL) {
			errno = ENODATA; /* No data available */
			CT_ERROR(errno, "get_query");
			goto cleanup;
		}
		rc = tsm_del_obj(&query_data);
		if (rc != DSM_RC_SUCCESSFUL) {
			CT_WARN("\ncannot delete obj %s\n"
				"\t\tfs: %s\n"
				"\t\thl: %s\n"
				"\t\tll: %s",
				OBJ_TYPE(query_data.objName.objType),
				query_data.objName.fs,
				query_data.objName.hl,
				query_data.objName.ll);
		} else {
			CT_INFO("\ndeleted obj fs: %s\n"
				"\t\tfs: %s\n"
				"\t\thl: %s\n"
				"\t\tll: %s",
				OBJ_TYPE(query_data.objName.objType),
				query_data.objName.fs,
				query_data.objName.hl,
				query_data.objName.ll);
		}
	}

cleanup:
	destroy_qarray();
	return rc;
}

dsInt16_t tsm_delete_fpath(const char *fs, const char *fpath)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	rc = extract_hl_ll(fpath, hl, ll);
	CT_INFO("extract_hl_ll:\n"
		"fpath: %s\n"
		"hl: %s\n"
		"ll: %s\n", fpath, hl, ll);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "extract_hl_ll");
		return rc;
	}
	rc = tsm_delete_hl_ll(fs, hl, ll);

	return rc;
}

dsInt16_t tsm_query_fpath(const char *fs, const char *fpath, const char *desc,
			  dsBool_t display)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	rc = extract_hl_ll(fpath, hl, ll);
	CT_INFO("extract_hl_ll:\n"
		"fpath: %s\n"
		"hl: %s\n"
		"ll: %s\n", fpath, hl, ll);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "extract_hl_ll");
		return rc;
	}
	rc = tsm_query_hl_ll(fs, hl, ll, desc, display);

	return rc;
}

static dsInt16_t tsm_retrieve_generic(const char *fs, const char *hl,
				      const char *ll, int fd,
				      const char *desc)
{
	dsInt16_t rc;
	dsInt16_t rc_minor = 0;
	dsmGetList get_list;

	get_list.objId = NULL;

	rc = init_qarray();
	if (rc != DSM_RC_SUCCESSFUL)
		goto cleanup;

	rc = tsm_query_hl_ll(fs, hl, ll, desc, bFalse);
	if (rc != DSM_RC_SUCCESSFUL)
		goto cleanup;

	/* Sort query replies to ensure that the data are read from the server in the most efficient order.
	   At least this is written on page Chapter 3. API design recommendations and considerations 57. */
	sort_qarray();

	/* TODO: Implement later also partialObjData handling. See page 56.*/
	get_list.stVersion = dsmGetListVersion; /* dsmGetListVersion: Not using Partial Obj data,
						   dsmGetListPORVersion: Using Partial Obj data. */

	/* Objects which are inserted in dsmGetList after querying more than
	   DSM_MAX_GET_OBJ (= 4080) items cannot be retrieved with a single
	   function call dsmBeginGetData. To overcome this limitation, partition
	   the query replies in chunks of maximum size DSM_MAX_GET_OBJ and call
	   dsmBeginGetData on each chunk. */
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
			goto cleanup;
		}
		for (unsigned long c_iter = c_begin; c_iter <= c_end; c_iter++) {

			rc = get_query(&query_data, c_iter);
			CT_INFO("get_query: %lu, rc: %d", c_iter, rc);
			if (rc != DSM_RC_SUCCESSFUL) {
				errno = ENODATA; /* No data available */
				CT_ERROR(errno, "get_query");
				goto cleanup;
			} else if (qarray_size() == 0) {
				CT_INFO("get_query has no match");
				goto cleanup;
			}
			get_list.objId[i++] = query_data.objId;
		}

		rc = dsmBeginGetData(handle, bTrue /* mountWait */, gtArchive, &get_list);
		TSM_TRACE(rc, "dsmBeginGetData");
		if (rc) {
			TSM_ERROR(rc, "dsmBeginGetData");
			goto cleanup;
		}

		obj_info_t obj_info;
		for (unsigned long c_iter = c_begin; c_iter <= c_end; c_iter++) {

			rc = get_query(&query_data, c_iter);
			CT_INFO("get_query: %lu, rc: %d", c_iter, rc);
			if (rc != DSM_RC_SUCCESSFUL) {
				rc_minor = ENODATA; /* No data available */
				CT_ERROR(rc_minor, "get_query");
				goto cleanup_getdata;
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

			if (obj_info.magic != MAGIC_ID_V1) {
				CT_WARN("Skip object due magic mismatch with MAGIC_ID: %d\n", obj_info.magic);
				continue;	/* Ignore this object and try next one. */
			}

			switch (query_data.objName.objType) {
			case DSM_OBJ_FILE: {
				rc_minor = retrieve_obj(&query_data, &obj_info, fd);
				CT_INFO("retrieve_obj, rc: %d\n", rc_minor);
				if (rc_minor != DSM_RC_SUCCESSFUL) {
					CT_ERROR(0, "retrieve_obj");
					goto cleanup_getdata;
				}
			} break;
			case DSM_OBJ_DIRECTORY: {
				const size_t len = strlen(query_data.objName.fs) +
					strlen(query_data.objName.hl) +
					strlen(query_data.objName.ll) + 1;
				char directory[len];
				bzero(directory, sizeof(char) * len);
				snprintf(directory, len, "%s%s%s",
					 query_data.objName.fs,
					 query_data.objName.hl,
					 query_data.objName.ll);
				rc_minor = mkdir_p(directory, obj_info.st_mode);
				CT_INFO("mkdir_p(%s)\n", directory);
				if (rc_minor) {
					CT_ERROR(rc_minor, "mkdir_p");
					goto cleanup_getdata;
				}
				break;
			}
			default: {
				CT_WARN("Skip object due to unkown type %s\n",
					OBJ_TYPE(query_data.objName.objType));
				continue;
			}
			} /* End-switch */
		} /* End-for iterate objid's. */
cleanup_getdata:
		/* There are no return codes that are specific to this call. */
		rc = dsmEndGetData(handle);
		TSM_TRACE(rc, "dsmEndGetData");
		if (rc_minor)
			break;

		c_cur++;
		c_begin = c_end + 1;
		/* Process last chunk and size not a multiple of DSM_MAX_GET_OBJ.*/
		c_end = (c_cur == c_total - 1 && qarray_size() % DSM_MAX_GET_OBJ != 0) ?
			c_begin + (qarray_size() % DSM_MAX_GET_OBJ) - 1 :
			c_begin + DSM_MAX_GET_OBJ - 1; /* Process not last chunk. */
	} while (c_cur < c_total);

cleanup:
	if (get_list.objId)
		free(get_list.objId);

	destroy_qarray();

	return (rc_minor == 0 ? rc : rc_minor);
}

dsInt16_t tsm_retrieve_fpath(const char *fs, const char *fpath,
			     const char *desc, int fd)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	rc = extract_hl_ll(fpath, hl, ll);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "extract_hl_ll");
		return rc;
	}
	rc = tsm_retrieve_generic(fs, hl, ll, fd, desc);

	return rc;
}

static dsInt16_t tsm_archive_generic(archive_info_t *archive_info, int fd)
{
	dsInt16_t rc;
	dsInt16_t rc_minor = 0;
	mcBindKey mc_bind_key;
	sndArchiveData arch_data;
	ObjAttr obj_attr;
	DataBlk data_blk;
	dsUint16_t err_reason;
	dsBool_t success = bFalse;
	ssize_t total_read = 0;
	ssize_t obj_size_read = 0;
	dsBool_t done = bFalse;
	dsUint8_t vote_txn;
	dsBool_t is_local_fd = bFalse;

	data_blk.bufferPtr = NULL;
	obj_attr.objInfo = NULL;

	if (fd < 0) {
		fd = open(archive_info->fpath, O_RDONLY,
			  archive_info->obj_info.st_mode);
		if (fd < 0) {
			CT_ERROR(errno, "open '%s'", archive_info->fpath);
			return DSM_RC_UNSUCCESSFUL;
		}
		is_local_fd = bTrue;
	}

	/* Start transaction. */
	rc = dsmBeginTxn(handle);
	TSM_TRACE(rc, "dsmBeginTxn");
	if (rc) {
		TSM_ERROR(rc, "dsmBeginTxn");
		goto cleanup;
	}

	mc_bind_key.stVersion = mcBindKeyVersion;
	rc = dsmBindMC(handle, &(archive_info->obj_name), stArchive, &mc_bind_key);
	TSM_TRACE(rc, "dsmBindMC");
	if (rc) {
		TSM_ERROR(rc, "dsmBindMC");
		goto cleanup_transaction;
	}

	arch_data.stVersion = sndArchiveDataVersion;
	if (strlen(archive_info->desc) <= DSM_MAX_DESCR_LENGTH)
		arch_data.descr = (char *)archive_info->desc;
	else
		arch_data.descr[0] = '\0';

	rc = obj_attr_prepare(&obj_attr, archive_info);
	if (rc)
		goto cleanup_transaction;

	/* Start sending object. */
	rc = dsmSendObj(handle, stArchive, &arch_data,
			&(archive_info->obj_name), &obj_attr, NULL);
	TSM_TRACE(rc, "dsmSendObj");
	if (rc) {
		TSM_ERROR(rc, "dsmSendObj");
		goto cleanup_transaction;
	}

	if (archive_info->obj_name.objType == DSM_OBJ_FILE) {
		data_blk.bufferPtr = (char *)malloc(sizeof(char) * TSM_BUF_LENGTH);
		if (!data_blk.bufferPtr) {
			rc = errno;
			CT_ERROR(rc, "malloc");
			goto cleanup_transaction;
		}
		data_blk.stVersion = DataBlkVersion;
		data_blk.numBytes = 0;

		while (!done) {
			obj_size_read = read(fd, data_blk.bufferPtr, TSM_BUF_LENGTH);
			if (obj_size_read < 0) {
				CT_ERROR(errno, "read");
				rc_minor = DSM_RC_UNSUCCESSFUL;
				goto cleanup_transaction;
			} else if (obj_size_read == 0) /* Zero indicates end of file. */
				done = bTrue;
			else {
				total_read += obj_size_read;
				data_blk.bufferLen = obj_size_read;

				rc = dsmSendData(handle, &data_blk);
				TSM_TRACE(rc, "dsmSendData");
				if (rc) {
					TSM_ERROR(rc, "dsmSendData");
					goto cleanup_transaction;
				}
				CT_TRACE("data_blk.numBytes: %zu, current_read: %zu, total_read: %zu, obj_size: %zu",
					 data_blk.numBytes, obj_size_read, total_read, to_off64_t(archive_info->obj_info.size));
				data_blk.numBytes = 0;
			}
		}
		/* File obj. was archived, verify that the number of bytes read
		   from file descriptor matches the number of bytes we
		   transfered with dsmSendData. */
		success = total_read == to_off64_t(archive_info->obj_info.size) ?
			bTrue : bFalse;
	} else /* dsmSendObj was successful and we archived a directory obj. */
		success = bTrue;

	rc = dsmEndSendObj(handle);
	TSM_TRACE(rc, "dsmEndSendObj");
	if (rc) {
		TSM_ERROR(rc, "dsmEndSendObj");
		success = bFalse;
	}

cleanup_transaction:
	/* Commit transaction (DSM_VOTE_COMMIT) on success, otherwise
	   roll back current transaction (DSM_VOTE_ABORT). */
	vote_txn = success == bTrue ? DSM_VOTE_COMMIT : DSM_VOTE_ABORT;
	rc = dsmEndTxn(handle, vote_txn, &err_reason);
	TSM_TRACE(rc, "dsmEndTxn");
	if (rc || err_reason) {
		TSM_ERROR(rc, "dsmEndTxn");
		TSM_ERROR(err_reason, "dsmEndTxn reason");
		success = bFalse;
	}

	if (success) {
		CT_INFO("\n*** successfully archived: %s %s of size: %lu bytes "
			"with settings ***\n"
			"fs: %s\n"
			"hl: %s\n"
			"ll: %s\n"
			"desc: %s\n",
			OBJ_TYPE(archive_info->obj_name.objType),
			archive_info->fpath, total_read,
			archive_info->obj_name.fs, archive_info->obj_name.hl,
			archive_info->obj_name.ll, archive_info->desc);
	}

cleanup:
	if (obj_attr.objInfo)
		free(obj_attr.objInfo);
	if (data_blk.bufferPtr)
		free(data_blk.bufferPtr);

	if (is_local_fd && !(fd < 0)) {
		rc = close(fd);
		if (rc < 0) {
			CT_ERROR(errno, "close failed: %d", fd);
			rc = DSM_RC_UNSUCCESSFUL;
		}

	}

	return (rc_minor ? DSM_RC_UNSUCCESSFUL : rc);
}

/**
 * @brief Initialize and setup archive_info_t struct fields.
 *
 *  Processes input file or directory name, file space name and description.
 *  Extract from fpath the high-level name hl, the low-level name ll and fill
 *  struct fields in archive_info_t with fs, desc, hl and ll.
 *
 *  @param[in] fs    File space name set in archive_info->dsmObjectName.fs.
 *  @param[in] fpath Path to file or directory, converted to hl, ll and set
 *                   archive_info->dsmObjectName.hl and
 *                   archive_info->dsmObjectName.ll.
 *  @param[in] desc  Description of fpath and to be set in
 *                   archive_info->dsmObjectName.desc.
 *  @return DSM_RC_SUCCESSFUL on success otherwise DSM_RC_UNSUCCESSFUL.
 */
static dsInt16_t tsm_archive_prepare(const char *fs, const char *fpath,
				     const char *desc,
				     archive_info_t *archive_info)
{
	dsInt16_t rc;
	char *resolved_fpath = NULL;
	struct stat st_buf;

	if (fs == NULL || fpath == NULL) {
		CT_ERROR(EFAULT, "fs || fpath null argument");
		return DSM_RC_UNSUCCESSFUL;
	}

	resolved_fpath = realpath(fpath, resolved_fpath);
	if (resolved_fpath == NULL) {
		CT_ERROR(errno, "realpath failed: %s", fpath);
		return DSM_RC_UNSUCCESSFUL;
	}
	strncpy(archive_info->fpath, resolved_fpath, PATH_MAX);

	rc = lstat(resolved_fpath, &st_buf);
	if (rc) {
		CT_ERROR(errno, "lstat");
		rc = DSM_RC_UNSUCCESSFUL;
		goto cleanup;
	}
	archive_info->obj_info.size = to_dsStruct64_t(st_buf.st_size);
	archive_info->obj_info.magic = MAGIC_ID_V1;
	archive_info->obj_info.st_mode = st_buf.st_mode;

	if (S_ISREG(st_buf.st_mode))
		archive_info->obj_name.objType = DSM_OBJ_FILE;
	else if (S_ISDIR(st_buf.st_mode))
		archive_info->obj_name.objType = DSM_OBJ_DIRECTORY;
	else {
		CT_ERROR(EINVAL, "no regular file or directory: %s", resolved_fpath);
		rc = DSM_RC_UNSUCCESSFUL;
		goto cleanup;
	}

	rc = extract_hl_ll(resolved_fpath, archive_info->obj_name.hl,
			   archive_info->obj_name.ll);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "extract_hl_ll failed, resolved_path: %s, "
			 "hl: %s, ll: %s", resolved_fpath,
			 archive_info->obj_name.hl,
			 archive_info->obj_name.ll);
		rc = DSM_RC_UNSUCCESSFUL;
		goto cleanup;
	}
	strncpy(archive_info->obj_name.fs, fs, DSM_MAX_FSNAME_LENGTH);

	if (desc == NULL)
		archive_info->desc[0] = '\0';
	else
		strncpy(archive_info->desc, desc, DSM_MAX_DESCR_LENGTH);

cleanup:
	if (resolved_fpath)
		free(resolved_fpath);

	return rc;
}

static dsInt16_t tsm_archive_recursive(archive_info_t *archive_info)
{
	int rc;
        DIR *dir;
        struct dirent *entry = NULL;
        char path[PATH_MAX] = {0};
	char dpath[PATH_MAX] = {0};
        int path_len;
	int old_errno;

	/* TODO: Still not happy with this implementation.
	   There must be a smarter and more elegant approach. */
	strncpy(dpath, archive_info->fpath, PATH_MAX);

        dir = opendir(dpath);
        if (!dir) {
		rc = errno;
		CT_ERROR(rc, "opendir: %s", dpath);
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
				CT_ERROR(rc, "readdir: %s", dpath);
				break;
			}
                }
                path_len = snprintf(path, PATH_MAX, "%s/%s", dpath,
				    entry->d_name);
                if (path_len >= PATH_MAX) {
                        CT_ERROR(ENAMETOOLONG, "path too long, ignoring: %s/%s",
				 dpath, entry->d_name);
			continue;
                }

		/* Currently, only some filesystems (among them: Btrfs, ext2,
		   ext3, and ext4) have full support for returning the file type
		   in d_type. Other file systems such as xfs return DT_UNKNOWN.
		   When d_type is DT_UNKNOWN determine d_type with lstat()
		   inside fallback_dt_unknown(). */
		if (entry->d_type == DT_UNKNOWN) {
			rc = fallback_dt_unknown(entry, path);
			/* If fallback fails we skip this entry, flag an error
			   and try to archive the next entry. Note: For being
			   more conservative we can also break() here. */
			if (rc != DSM_RC_SUCCESSFUL) {
				CT_ERROR(rc, "fallback_dt_unkown failed: '%s'", path);
				continue;
			}
		}

		switch (entry->d_type) {
		case DT_REG: {
			rc = tsm_archive_prepare(archive_info->obj_name.fs,
						 path,
						 archive_info->desc,
						 archive_info);
			if (rc) {
				CT_WARN("tsm_archive_prepare failed: \n"
					"fs: %s, fpath: %s, hl: %s, ll: %s\n",
					archive_info->obj_name.fs,
					archive_info->fpath,
					archive_info->obj_name.hl,
					archive_info->obj_name.ll);
				break;
			}
			rc = tsm_archive_generic(archive_info, -1);
			if (rc)
				CT_WARN("tsm_archive_generic failed: %s", archive_info->fpath);
			break;
		}
		case DT_DIR: {
			if (strcmp(entry->d_name, ".") == 0 ||
			    strcmp(entry->d_name, "..") == 0)
				continue;
			rc = tsm_archive_prepare(archive_info->obj_name.fs,
						 path,
						 archive_info->desc,
						 archive_info);
			if (rc) {
				CT_WARN("tsm_archive_prepare failed: \n"
					"fs: %s, fpath: %s, hl: %s, ll: %s\n",
					archive_info->obj_name.fs,
					archive_info->fpath,
					archive_info->obj_name.hl,
					archive_info->obj_name.ll);
				break;
			}
			rc = tsm_archive_generic(archive_info, -1);
			if (rc) {
				CT_WARN("tsm_archive_generic failed: %s", archive_info->fpath);
				break;
			}
			if (do_recursive) {
				snprintf(archive_info->fpath, PATH_MAX, "%s/%s", dpath, entry->d_name);
				rc = tsm_archive_recursive(archive_info);
			}
			break;
		}
		default: /* Flag error on fifos, block/character devices, links, etc. */
			rc = EINVAL;
			CT_ERROR(rc, "no regular file or directory: %s", path);
			break;
		}
        }
        closedir(dir);

        return rc;
}

/**
 * @brief Archive file or directory.
 *
 * Archive file or directory, specified by fs, fpath and additional description
 * information. Note: If fpath is a directory and do_recursive is true, then
 * recursively all files and subdirectories inside fpath are also archived.
 *
 * @param[in] fs    File space name set in archive_info->dsmObjectName.fs.
 * @param[in] fpath Path to file or directory, converted to hl, ll and set
 *                  archive_info->dsmObjectName.hl and
 *                  archive_info->dsmObjectName.ll.
 * @param[in] desc  Description of fpath and set in
 *                  archive_info->dsmObjectName.desc.
 * @param[in] fd    File descriptor.
 * @param[in] lu_fid Lustre FID information is set in
 *                   archive_info->obj_info.lu_fid.
 * @return DSM_RC_SUCCESSFUL on success otherwise DSM_RC_UNSUCCESSFUL.
 */
dsInt16_t tsm_archive_fpath(const char *fs, const char *fpath,
			    const char *desc, int fd, const lu_fid_t *lu_fid)
{
	int rc;
	archive_info_t archive_info;

	CT_INFO("tsm_archive_fpath:\n"
		"fs: %s, fpath: %s, desc: %s, fd: %d, *lu_fid: %p",
		fs, fpath, desc, fd, lu_fid);

	memset(&archive_info, 0, sizeof(archive_info_t));
	if (lu_fid)
		memcpy(&(archive_info.obj_info.lu_fid), lu_fid, sizeof(lu_fid_t));
	rc = tsm_archive_prepare(fs, fpath, desc, &archive_info);
	if (rc) {
		CT_WARN("tsm_archive_prepare failed: \n"
			"fs: %s, fpath: %s, desc: %s\n",
			fs, fpath, desc);
		return rc;
	}
	/* If fpath is a directory D, then archive D and all regular files
	   inside D. If do_recursive is bTrue, archive recursively
	   regular files and directories inside D. */
	if (archive_info.obj_name.objType == DSM_OBJ_DIRECTORY)
		return tsm_archive_recursive(&archive_info); /* Archive (recursively) inside D. */
	else
		return tsm_archive_generic(&archive_info, fd); /* Archive regular file. */

	return rc;
}
