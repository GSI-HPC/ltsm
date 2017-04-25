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
 * Copyright (c) 2016, 2017, Thomas Stibor <t.stibor@gsi.de>
 * 			     Jörg Behrendt <j.behrendt@gsi.de>
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
#include <stdint.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/param.h>
#include <zlib.h>
#include "tsmapi.h"
#include "qtable.h"

#define OBJ_TYPE(type)							\
	(DSM_OBJ_FILE == type ? "DSM_OBJ_FILE" :			\
	(DSM_OBJ_DIRECTORY == type ? "DSM_OBJ_DIRECTORY" :		\
	(DSM_OBJ_RESERVED1 == type ? "DSM_OBJ_RESERVED1" :		\
	(DSM_OBJ_RESERVED2 == type ? "DSM_OBJ_RESERVED2" :		\
	(DSM_OBJ_RESERVED3 == type ? "DSM_OBJ_RESERVED3" :		\
	(DSM_OBJ_WILDCARD == type ? "DSM_OBJ_WILDCARD" :		\
	(DSM_OBJ_ANY_TYPE == type ? "DSM_OBJ_ANY_TYPE" : "UNKNOWN")))))))

static char rcmsg[DSM_MAX_RC_MSG_LENGTH + 1] = {0};

static dsBool_t do_recursive;

#define TSM_GET_MSG(session, rc)			\
do {							\
	memset(&rcmsg, 0, DSM_MAX_RC_MSG_LENGTH + 1);	\
	dsmRCMsg(session->handle, rc, rcmsg);		\
	rcmsg[strlen(rcmsg)-1] = '\0';			\
} while (0)

#define TSM_ERROR(session, rc, func)					\
do {									\
	TSM_GET_MSG(session, rc);					\
	CT_ERROR(0, "%s: handle: %d %s", func, session->handle, rcmsg);	\
} while (0)

#define TSM_DEBUG(session, rc, func)					\
do {									\
	TSM_GET_MSG(session, rc);					\
	CT_DEBUG("%s: handle: %d %s", func, session->handle, rcmsg);	\
} while (0)

void login_fill(struct login_t *login, const char *servername,
		const char *node, const char *password,
		const char *owner, const char *platform,
		const char *fsname, const char *fstype)
{
	memset(login, 0, sizeof(*login));
	strcpy(login->node, node);
	strcpy(login->password, password);
	strcpy(login->owner, owner);
	strcpy(login->platform, platform);
	strcpy(login->fsname, fsname);
	strcpy(login->fstype, fstype);

	const unsigned short s_arg_len = 1 + strlen(servername) +
		strlen("-se=");
	if (s_arg_len < MAX_OPTIONS_LENGTH)
		snprintf(login->options, s_arg_len, "-se=%s", servername);
	else
		CT_WARN("Option parameter \'-se=%s\' is larger than "
			"MAX_OPTIONS_LENGTH: %d and is ignored\n",
			servername, MAX_OPTIONS_LENGTH);
}

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
	return (uint64_t)size.hi << 32 | (uint64_t)size.lo;
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
	res.hi = (dsUint32_t)((uint64_t)size >> 32);

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
			      const struct obj_info_t *obj_info, int fd,
			      struct session_t* session)
{
	char	  *buf		= NULL;
	dsInt16_t rc;
	dsInt16_t rc_minor	= 0;
	dsBool_t  is_local_fd	= bFalse;

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

		fd = open(fpath, O_WRONLY | O_TRUNC | O_CREAT,
			  obj_info->st_mode);
		if (fd < 0) {
			CT_ERROR(errno, "open '%s'", fpath);
			return DSM_RC_UNSUCCESSFUL;
		}
		is_local_fd = bTrue;
	}

	buf = malloc(sizeof(char) * TSM_BUF_LENGTH);
	if (!buf) {
		CT_ERROR(errno, "malloc");
		rc_minor = DSM_RC_UNSUCCESSFUL;
		goto cleanup_fd;
	}

	DataBlk dataBlk;
	dataBlk.stVersion = DataBlkVersion;
	dataBlk.bufferLen = TSM_BUF_LENGTH;
	dataBlk.numBytes  = 0;
	dataBlk.bufferPtr = buf;
	memset(dataBlk.bufferPtr, 0, TSM_BUF_LENGTH);

	off64_t obj_size = to_off64_t(obj_info->size);
	dsBool_t done = bFalse;
	ssize_t total_written = 0;
	ssize_t cur_written = 0;

	/* Request data with a single dsmGetObj call, otherwise data
	   is larger and we need additional dsmGetData calls. */
	rc = dsmGetObj(session->handle, &(query_data->objId), &dataBlk);
	TSM_DEBUG(session, rc,  "dsmGetObj");
	uint32_t crc32sum = 0;

	while (!done) {
		if (!(rc == DSM_RC_MORE_DATA || rc == DSM_RC_FINISHED)) {
			TSM_ERROR(session, rc, "dsmGetObj or dsmGetData");
			rc_minor = rc;
			goto cleanup;
		}
		cur_written = write(fd, buf, dataBlk.numBytes);
		if (cur_written < 0) {
			CT_ERROR(errno, "write");
			rc_minor = DSM_RC_UNSUCCESSFUL;
			goto cleanup;
		}
		crc32sum = crc32(crc32sum, (const unsigned char *)buf,
				 cur_written);
		total_written += cur_written;
		CT_INFO("datablk_numbytes: %zu, cur_written: %zu,"
			" total_written: %zu, obj_size: %zu",
			dataBlk.numBytes, cur_written, total_written, obj_size);

		/* Function callback on updating progress */
		if (session->progress != NULL) {
			struct progress_size_t progress_size = {
				.cur = cur_written,
				.cur_total = total_written,
				.total = obj_size
			};
			rc_minor = session->progress(&progress_size,
						     session);
			if (rc_minor) {
				CT_ERROR(rc_minor, "progress function"
					 " callback failed");
				goto cleanup;
			}
		}
		if (rc == DSM_RC_MORE_DATA) {
			dataBlk.numBytes = 0;
			rc = dsmGetData(session->handle, &dataBlk);
			TSM_DEBUG(session, rc,  "dsmGetData");
		} else	/* DSM_RC_FINISHED */
			done = bTrue;
	} /* End while (!done) */

	/* Do a sanity check whether size of object (hi, lo) matches
	   the total_written bytes. */
	if (obj_size != total_written)
		CT_WARN("object size: %zu and written data size: %zu differs",
			obj_size, total_written);

	/* Do a sanity check whether CRC32 sum of object matches
	   the CRC32 sum of fd written data. */
	if (obj_info->crc32 != crc32sum)
		CT_WARN("object crc32: 0x%08x and written fd crc32: "
			"0x%08x differs", obj_info->crc32, crc32sum);

cleanup:
	rc = dsmEndGetObj(session->handle);
	TSM_DEBUG(session, rc,  "dsmEndGetObj");
	if (rc != DSM_RC_SUCCESSFUL)
		TSM_ERROR(session, rc, "dsmEndGetObj");

	if (buf)
		free(buf);

cleanup_fd:
	if (is_local_fd && !(fd < 0)) {
		rc = close(fd);
		if (rc < 0) {
			CT_ERROR(errno, "close failed: %d", rc, fd);
			rc = DSM_RC_UNSUCCESSFUL;
		}

	}

	return (rc_minor ? DSM_RC_UNSUCCESSFUL : rc);
}

static void display_qra(const qryRespArchiveData *qra_data, const uint32_t n,
	const char *msg)
{
	char ins_str_date[128] = {0};
	char exp_str_date[128] = {0};
	struct obj_info_t obj_info;
	memcpy(&obj_info, (char *)qra_data->objInfo, qra_data->objInfolen);
	date_to_str(ins_str_date, &(qra_data->insDate));
	date_to_str(exp_str_date, &(qra_data->expDate));

	if (api_msg_get_level() == API_MSG_NORMAL) {
		fprintf(stdout, "%s %20s %14zu, fs:%s hl:%s ll:%s\n",
			msg,
			OBJ_TYPE(qra_data->objName.objType),
			to_off64_t(obj_info.size),
			qra_data->objName.fs,
			qra_data->objName.hl,
			qra_data->objName.ll);
		fflush(stdout);
	} else if (api_msg_get_level() > API_MSG_NORMAL) {
		CT_INFO("%s object # %lu\n"
			"fs: %s, hl: %s, ll: %s\n"
			"object id (hi,lo)                          : (%u,%u)\n"
			"object info length                         : %d\n"
			"object info size (hi,lo)                   : (%u,%u)\n"
			"object type                                : %s\n"
			"object magic id                            : %d\n"
			"crc32                                      : 0x%08x (%010u)\n"
			"archive description                        : %s\n"
			"owner                                      : %s\n"
			"insert date                                : %s\n"
			"expiration date                            : %s\n"
			"restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (%u,%u,%u,%u,%u)\n"
			"estimated size (hi,lo)                     : (%u,%u)\n",
			msg,
			n,
			qra_data->objName.fs,
			qra_data->objName.hl,
			qra_data->objName.ll,
			qra_data->objId.hi,
			qra_data->objId.lo,
			qra_data->objInfolen,
			obj_info.size.hi,
			obj_info.size.lo,
			OBJ_TYPE(qra_data->objName.objType),
			obj_info.magic,
			obj_info.crc32, obj_info.crc32,
			qra_data->descr,
			qra_data->owner,
			ins_str_date,
			exp_str_date,
			qra_data->restoreOrderExt.top,
			qra_data->restoreOrderExt.hi_hi,
			qra_data->restoreOrderExt.hi_lo,
			qra_data->restoreOrderExt.lo_hi,
			qra_data->restoreOrderExt.lo_lo,
			qra_data->sizeEstimate.hi,
			qra_data->sizeEstimate.lo);
	}
}

dsInt16_t tsm_print_query(struct session_t *session)
{
	dsInt16_t rc;

	qryRespArchiveData qra_data;
	for (uint32_t n = 0; n < session->qtable.qarray.size; n++) {
		rc = get_qra(&session->qtable, &qra_data, n);
		if (rc)
			return DSM_RC_UNSUCCESSFUL;
		display_qra(&qra_data, n, "[query]");
	}
	return DSM_RC_SUCCESSFUL;
}

dsInt16_t tsm_init(const dsBool_t mt_flag)
{
	dsInt16_t rc;
	struct session_t *empty_session = NULL;
	empty_session = calloc(1, sizeof(struct session_t));
	if (empty_session == NULL)
		return DSM_RC_UNSUCCESSFUL;

	/* The dsmSetUp call must be the first call
	   after the dsmQueryApiVersionEx call. This
	   call must return before any thread calls
	   the dsmInitEx call. When all threads complete processing,
	   enter a call to dsmCleanUp.
	 */
	get_libapi_ver();	/* Calls dsmQueryApiVersionEx.*/
	rc = dsmSetUp(mt_flag, NULL);
	TSM_DEBUG(empty_session, rc, "dsmSetUp");
	if (rc) {
		TSM_ERROR(empty_session, rc, "dsmSetUp");
		dsmCleanUp(mt_flag);
		free(empty_session);
		return DSM_RC_UNSUCCESSFUL;
	}
	free(empty_session);
	return DSM_RC_SUCCESSFUL;
}

void tsm_cleanup(const dsBool_t mt_flag)
{
	/* The dsmCleanUp function call should be called after dsmTerminate.
	   You cannot make any other calls after you call dsmCleanUp. */
	dsmCleanUp(mt_flag);
}

dsInt16_t tsm_connect(struct login_t *login, struct session_t *session)
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

	rc = dsmInitEx(&(session->handle), &init_in, &init_out);
	TSM_DEBUG(session, rc,  "dsmInitEx");
	if (rc) {
		TSM_ERROR(session, rc, "dsmInitEx");
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

	rc = dsmRegisterFS(session->handle, &reg_fs_data);
	TSM_DEBUG(session, rc,  "dsmRegisterFS");
	if (rc == DSM_RC_FS_ALREADY_REGED || rc == DSM_RC_OK)
		return DSM_RC_OK;

	TSM_ERROR(session, rc, "dsmRegisterFS");
	return rc;
}

void tsm_disconnect(struct session_t *session)
{
	dsmTerminate(session->handle);
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

dsInt16_t tsm_query_session(struct session_t *session)
{
	optStruct dsmOpt;
	dsInt16_t rc;
	rc = dsmQuerySessOptions(session->handle, &dsmOpt);
	TSM_DEBUG(session, rc,  "dsmQuerySessOptions");
	if (rc) {
		TSM_ERROR(session, rc, "dsmQuerySessOptions");
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

	rc = dsmQuerySessInfo(session->handle, &dsmSessInfo);
	TSM_DEBUG(session, rc,  "dsmQuerySessInfo");
	if (rc) {
		TSM_ERROR(session, rc, "dsmQuerySessInfo");
		return rc;
	}

	CT_INFO("\n"
		 "Server's ver.rel.lev       : %d.%d.%d.%d\n"
		 "ArchiveRetentionProtection : %s\n",
		 dsmSessInfo.serverVer, dsmSessInfo.serverRel,
		 dsmSessInfo.serverLev, dsmSessInfo.serverSubLev,
		 dsmSessInfo.archiveRetentionProtection ? "Yes" : "No");
	CT_INFO("\n"
		 "Max number of multiple objects per transaction: %d\n"
		 "Max number of Bytes per transaction: %u\n"
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
		TSM_ERROR(session, rc, "TSM API library is lower than the"
			  " application version, \n"
			  "install the current library version.");
		return rc;
	}

	CT_INFO("IBM API library version = %d.%d.%d.%d\n",
		libapi_ver_t.version,
		libapi_ver_t.release,
		libapi_ver_t.level,
		libapi_ver_t.subLevel);

	return rc;
}

static dsInt16_t tsm_query_hl_ll(const char *fs, const char *hl, const char *ll,
				 const char *desc, struct session_t *session)
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

	rc = dsmBeginQuery(session->handle, qtArchive, &qry_ar_data);
	TSM_DEBUG(session, rc,  "dsmBeginQuery");
	if (rc) {
		TSM_ERROR(session, rc, "dsmBeginQuery");
		goto cleanup;
	}

	qryRespArchiveData qry_resp_ar_data;
	DataBlk data_blk;
	data_blk.stVersion = DataBlkVersion;
	data_blk.bufferLen = sizeof(qry_resp_ar_data);
	data_blk.bufferPtr = (char *)&qry_resp_ar_data;
	qry_resp_ar_data.stVersion = qryRespArchiveDataVersion;

	dsBool_t done = bFalse;
	while (!done) {
		rc = dsmGetNextQObj(session->handle, &data_blk);
		TSM_DEBUG(session, rc, "dsmGetNextQObj");

		if (((rc == DSM_RC_OK) || (rc == DSM_RC_MORE_DATA) ||
		     (rc == DSM_RC_FINISHED))
		    && data_blk.numBytes) {

			qry_resp_ar_data.objInfo[qry_resp_ar_data.objInfolen] = '\0';
			rc = insert_qtable(&session->qtable, &qry_resp_ar_data);
			if (rc) {
				CT_ERROR(EFAILED, "insert_qtable failed");
				goto cleanup;
			}
		} else if (rc == DSM_RC_UNKNOWN_FORMAT)
                        CT_WARN("DSM_OBJECT not archived by API, skipping object");
		else {
			done = bTrue;
			if (rc == DSM_RC_ABORT_NO_MATCH)
				CT_MESSAGE("query has no match");
			else if (rc != DSM_RC_FINISHED)
				TSM_ERROR(session, rc, "dsmGetNextQObj");
		}
	}

	rc = dsmEndQuery(session->handle);
	if (rc)  {
		TSM_ERROR(session, rc, "dsmEndQuery");
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
				  const struct archive_info_t *archive_info)
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
	obj_attr->objInfoLength = sizeof(struct obj_info_t);
	obj_attr->objInfo = (char *)malloc(obj_attr->objInfoLength);
	if (!obj_attr->objInfo) {
		rc = errno;
		CT_ERROR(rc, "malloc");
		return rc;
	}

	memcpy(obj_attr->objInfo, (char *)&(archive_info->obj_info),
	       obj_attr->objInfoLength);

	return DSM_RC_SUCCESSFUL;
}

static dsInt16_t tsm_obj_update_crc32(ObjAttr *obj_attr,
				      struct archive_info_t *archive_info,
				      const uint32_t crc32,
				      struct session_t *session)
{
	dsInt16_t rc;
	struct obj_info_t obj_info;

	memcpy(&obj_info, (char *)obj_attr->objInfo, obj_attr->objInfoLength);
	obj_info.crc32 = crc32;
	memcpy(obj_attr->objInfo, (char *)&(obj_info), obj_attr->objInfoLength);

	rc = dsmUpdateObj(session->handle, stArchive, NULL,
			  &(archive_info->obj_name), obj_attr,
			  DSM_ARCHUPD_OBJINFO);
	if (rc) {
		TSM_ERROR(session, rc, "dsmUpdateObj");
		return DSM_RC_UNSUCCESSFUL;
	}
	return DSM_RC_SUCCESSFUL;
}

static dsInt16_t tsm_del_obj(const qryRespArchiveData *qry_resp_ar_data,
			     struct session_t *session)
{
	dsmDelInfo del_info;
	dsInt16_t rc;
	dsUint16_t err_reason;
	dsUint8_t vote_txn = DSM_VOTE_COMMIT;

	rc = dsmBeginTxn(session->handle);
	TSM_DEBUG(session, rc,  "dsmBeginTxn");
	if (rc) {
		TSM_ERROR(session, rc, "dsmBeginTxn");
		return rc;
	}

	del_info.archInfo.stVersion = delArchVersion;
	del_info.archInfo.objId = qry_resp_ar_data->objId;

	rc = dsmDeleteObj(session->handle, dtArchive, del_info);
	TSM_DEBUG(session, rc, "dsmDeleteObj");
	if (rc) {
		TSM_ERROR(session, rc, "dsmDeleteObj");
		vote_txn = DSM_VOTE_ABORT;
		goto cleanup_transaction;
	}

cleanup_transaction:
	rc = dsmEndTxn(session->handle, vote_txn, &err_reason);
	TSM_DEBUG(session, rc,  "dsmEndTxn");
	if (rc || err_reason) {
		TSM_ERROR(session, rc, "dsmEndTxn");
		TSM_ERROR(session, err_reason, "dsmEndTxn reason");
	}

	return rc;
}

static dsInt16_t tsm_delete_hl_ll(struct session_t *session)
{
	dsInt16_t rc = DSM_RC_SUCCESSFUL;
	qryRespArchiveData qra_data;

	for (uint32_t n = 0; n < session->qtable.qarray.size; n++) {
		rc = get_qra(&session->qtable, &qra_data, n);
		CT_INFO("get_query: %lu, rc: %d", n, rc);
		if (rc) {
			errno = ENODATA; /* No data available */
			CT_ERROR(errno, "get_query");
			return rc;
		}
		rc = tsm_del_obj(&qra_data, session);
		CT_DEBUG("tsm_del_obj: %lu, rc: %d", n, rc);
		if (rc) {
			CT_WARN("tsm_del_obj failed, object not deleted\n");
			display_qra(&qra_data, n, "[delete failed]");
		} else
			display_qra(&qra_data, n, "[delete]");
	}
	return rc;
}

dsInt16_t tsm_delete_fpath(const char *fs, const char *fpath,
			   struct session_t *session)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	rc = extract_hl_ll(fpath, hl, ll);
	CT_INFO("extract_hl_ll:\n"
		"fpath: %s\n"
		"hl: %s\n"
		"ll: %s\n", fpath, hl, ll);
	if (rc) {
		CT_ERROR(EFAILED, "extract_hl_ll failed");
		return rc;
	}
	rc = init_qtable(&session->qtable);
	if (rc) {
		CT_ERROR(EFAILED, "init_qtable failed");
		return rc;
	}
	rc = tsm_query_hl_ll(fs, hl, ll, NULL, session);
	if (rc) {
		CT_ERROR(EFAILED, "tsm_query_hl_ll failed");
		goto cleanup;
	}
	rc = create_array(&session->qtable, bFalse);
	if (rc) {
		CT_ERROR(EFAILED, "create_array failed");
		goto cleanup;
	}
	rc = tsm_delete_hl_ll(session);
	if (rc)
		CT_ERROR(EFAILED, "tsm_print_query failed");

cleanup:
	destroy_qtable(&session->qtable);
	return rc;
}

dsInt16_t tsm_query_fpath(const char *fs, const char *fpath, const char *desc,
			  struct session_t *session)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	rc = extract_hl_ll(fpath, hl, ll);
	CT_INFO("extract_hl_ll:\n"
		"fpath: %s\n"
		"hl: %s\n"
		"ll: %s\n", fpath, hl, ll);
	if (rc) {
		CT_ERROR(EFAILED, "extract_hl_ll");
		return rc;
	}
	rc = init_qtable(&session->qtable);
	if (rc) {
		CT_ERROR(EFAILED, "init_qtable failed");
		return rc;
	}
	rc = tsm_query_hl_ll(fs, hl, ll, desc, session);
	if (rc) {
		CT_ERROR(EFAILED, "tsm_query_hl_ll failed");
		goto cleanup;
	}
	rc = create_array(&session->qtable, bFalse);
	if (rc) {
		CT_ERROR(EFAILED, "create_array failed");
		goto cleanup;
	}
	rc = tsm_print_query(session);
	if (rc)
		CT_ERROR(EFAILED, "tsm_print_query failed");

cleanup:
	destroy_qtable(&session->qtable);
	return rc;
}

static dsInt16_t tsm_retrieve_generic(int fd, struct session_t *session)
{
	dsInt16_t rc;
	dsInt16_t rc_minor = 0;
	dsmGetList get_list;
	get_list.objId = NULL;

	/* TODO: Implement later also partialObjData handling. See page 56.*/
	get_list.stVersion = dsmGetListVersion; /* dsmGetListVersion: Not using Partial Obj data,
						   dsmGetListPORVersion: Using Partial Obj data. */

	/* Objects which are inserted in dsmGetList after querying more than
	   DSM_MAX_GET_OBJ (= 4080) items cannot be retrieved with a single
	   function call dsmBeginGetData. To overcome this limitation, partition
	   the query replies in chunks of maximum size DSM_MAX_GET_OBJ and call
	   dsmBeginGetData on each chunk. */
	uint32_t c_begin = 0;
	uint32_t c_end = MIN(session->qtable.qarray.size, DSM_MAX_GET_OBJ) - 1;
	uint32_t c_total = ceil((double)session->qtable.qarray.size /
				(double)DSM_MAX_GET_OBJ);
	uint32_t c_cur = 0;
	uint32_t num_objs;
	qryRespArchiveData query_data;
	uint32_t i;

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
		for (uint32_t c_iter = c_begin; c_iter <= c_end; c_iter++) {

			rc = get_qra(&session->qtable, &query_data, c_iter);
			CT_INFO("get_query: %lu, rc: %d", c_iter, rc);
			if (rc != DSM_RC_SUCCESSFUL) {
				errno = ENODATA; /* No data available */
				CT_ERROR(errno, "get_query");
				goto cleanup;
			} else if (session->qtable.qarray.size == 0) {
				CT_INFO("get_query has no match");
				goto cleanup;
			}
			get_list.objId[i++] = query_data.objId;
		}

		rc = dsmBeginGetData(session->handle, bTrue /* mountWait */, gtArchive, &get_list);
		TSM_DEBUG(session, rc,  "dsmBeginGetData");
		if (rc) {
			TSM_ERROR(session, rc, "dsmBeginGetData");
			goto cleanup;
		}

		struct obj_info_t obj_info;
		for (uint32_t c_iter = c_begin; c_iter <= c_end; c_iter++) {

			rc = get_qra(&session->qtable, &query_data, c_iter);
			CT_INFO("get_query: %lu, rc: %d", c_iter, rc);
			if (rc != DSM_RC_SUCCESSFUL) {
				rc_minor = ENODATA; /* No data available */
				CT_ERROR(rc_minor, "get_query");
				goto cleanup_getdata;
			}
			memcpy(&obj_info,
			       (char *)&(query_data.objInfo),
			       query_data.objInfolen);

			if (obj_info.magic != MAGIC_ID_V1) {
				CT_WARN("skip object due magic mismatch with MAGIC_ID: %d\n", obj_info.magic);
				continue;	/* Ignore this object and try next one. */
			}

			display_qra(&query_data, c_iter, "[retrieve]");
			switch (query_data.objName.objType) {
			case DSM_OBJ_FILE: {
				rc_minor = retrieve_obj(&query_data, &obj_info, fd, session);
				CT_INFO("retrieve_obj, rc: %d\n", rc_minor);
				if (rc_minor != DSM_RC_SUCCESSFUL) {
					CT_ERROR(EFAILED, "retrieve_obj failed");
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
		rc = dsmEndGetData(session->handle);
		TSM_DEBUG(session, rc,  "dsmEndGetData");
		if (rc_minor)
			break;

		c_cur++;
		c_begin = c_end + 1;
		/* Process last chunk and size not a multiple of DSM_MAX_GET_OBJ.*/
		c_end = (c_cur == c_total - 1 && session->qtable.qarray.size % DSM_MAX_GET_OBJ != 0) ?
			c_begin + (session->qtable.qarray.size % DSM_MAX_GET_OBJ) - 1 :
			c_begin + DSM_MAX_GET_OBJ - 1; /* Process not last chunk. */
	} while (c_cur < c_total);

cleanup:
	if (get_list.objId)
		free(get_list.objId);

	return (rc_minor == 0 ? rc : rc_minor);
}

dsInt16_t tsm_retrieve_fpath(const char *fs, const char *fpath,
			     const char *desc, int fd,
			     struct session_t *session)
{
	dsInt16_t rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	rc = extract_hl_ll(fpath, hl, ll);
	if (rc) {
		CT_ERROR(EFAILED, "extract_hl_ll");
		return rc;
	}

	rc = init_qtable(&session->qtable);
	if (rc) {
		CT_ERROR(EFAILED, "init_qtable failed");
		return rc;
	}

	rc = tsm_query_hl_ll(fs, hl, ll, desc, session);
	if (rc) {
		CT_ERROR(EFAILED, "tsm_query_hl_ll failed");
		goto cleanup;
	}

	rc = create_array(&session->qtable, bFalse);
	if (rc) {
		CT_ERROR(EFAILED, "create_array failed");
		goto cleanup;
	}
	rc = tsm_retrieve_generic(fd, session);
	if (rc)
		CT_ERROR(EFAILED, "tsm_retrieve_generic failed");

cleanup:
	destroy_qtable(&session->qtable);
	return rc;
}

static dsInt16_t tsm_archive_generic(struct archive_info_t *archive_info,
				     int fd, struct session_t *session)
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
	ssize_t cur_read = 0;
	dsBool_t done = bFalse;
	dsUint8_t vote_txn;
	dsBool_t is_local_fd = bFalse;
	uint32_t crc32sum = 0;

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
	rc = dsmBeginTxn(session->handle);
	TSM_DEBUG(session, rc,  "dsmBeginTxn");
	if (rc) {
		TSM_ERROR(session, rc, "dsmBeginTxn");
		goto cleanup;
	}

	mc_bind_key.stVersion = mcBindKeyVersion;
	rc = dsmBindMC(session->handle, &(archive_info->obj_name), stArchive, &mc_bind_key);
	TSM_DEBUG(session, rc,  "dsmBindMC");
	if (rc) {
		TSM_ERROR(session, rc, "dsmBindMC");
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
	rc = dsmSendObj(session->handle, stArchive, &arch_data,
			&(archive_info->obj_name), &obj_attr, NULL);
	TSM_DEBUG(session, rc,  "dsmSendObj");
	if (rc) {
		TSM_ERROR(session, rc, "dsmSendObj");
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
		ssize_t total_size = to_off64_t(archive_info->obj_info.size);

		while (!done) {
			cur_read = read(fd, data_blk.bufferPtr, TSM_BUF_LENGTH);
			if (cur_read < 0) {
				CT_ERROR(errno, "read");
				rc_minor = DSM_RC_UNSUCCESSFUL;
				goto cleanup_transaction;
			} else if (cur_read == 0)
				/* Zero indicates end of file. */
				done = bTrue;
			else {
				total_read += cur_read;
				data_blk.bufferLen = cur_read;

				data_blk.numBytes = 0;
				rc = dsmSendData(session->handle, &data_blk);
				TSM_DEBUG(session, rc,  "dsmSendData");
				if (rc) {
					TSM_ERROR(session, rc, "dsmSendData");
					goto cleanup_transaction;
				}
				CT_INFO("cur_read: %zu, total_read: %zu,"
					" total_size: %zu", cur_read,
					total_read, total_size);

				crc32sum = crc32(crc32sum,
						 (const unsigned char *)
						 data_blk.bufferPtr,
						 data_blk.numBytes);

				if (data_blk.numBytes != data_blk.bufferLen)
					CT_WARN("dsmSendData transmitted %u"
						" out of %d",
						data_blk.numBytes,
						data_blk.bufferLen);

				/* Function callback on progress */
				if (session->progress != NULL) {
					struct progress_size_t progress_size = {
						.cur = cur_read,
						.cur_total = total_read,
						.total = total_size
					};
					rc_minor = session->progress(
						&progress_size, session);
					if (rc_minor) {
						CT_ERROR(rc_minor,
							 "progress function"
							 " callback failed");
	 					goto cleanup_transaction;
	 				}
				}
			}
		}
		/* File obj. was archived, verify that the number of bytes read
		   from file descriptor matches the number of bytes we
		   transfered with dsmSendData. */
		success = total_read == to_off64_t(archive_info->obj_info.size) ?
			bTrue : bFalse;
	} else /* dsmSendObj was successful and we archived a directory obj. */
		success = bTrue;

	rc = dsmEndSendObj(session->handle);
	TSM_DEBUG(session, rc,  "dsmEndSendObj");
	if (rc) {
		TSM_ERROR(session, rc, "dsmEndSendObj");
		success = bFalse;
	}

cleanup_transaction:
	/* Commit transaction (DSM_VOTE_COMMIT) on success, otherwise
	   roll back current transaction (DSM_VOTE_ABORT). */
	vote_txn = success == bTrue ? DSM_VOTE_COMMIT : DSM_VOTE_ABORT;
	rc = dsmEndTxn(session->handle, vote_txn, &err_reason);
	TSM_DEBUG(session, rc,  "dsmEndTxn");
	if (rc || err_reason) {
		TSM_ERROR(session, rc, "dsmEndTxn");
		TSM_ERROR(session, err_reason, "dsmEndTxn reason");
		success = bFalse;
	}
	if (success) {
		if (api_msg_get_level() == API_MSG_NORMAL) {
			fprintf(stdout, "%s %20s %14zu, fs:%s hl:%s "
				"ll:%s\n",
				"[archive] ",
				OBJ_TYPE(archive_info->obj_name.objType),
				total_read,
				archive_info->obj_name.fs,
				archive_info->obj_name.hl,
				archive_info->obj_name.ll);
		} else if (api_msg_get_level() > API_MSG_NORMAL) {
			CT_INFO("\n*** successfully archived: %s %s of "
				"size: %lu bytes "
				"with settings ***\nfs: %s\nhl: "
				"%s\nll: %s\ndesc: %s\n",
				OBJ_TYPE(archive_info->obj_name.objType),
				archive_info->fpath, total_read,
				archive_info->obj_name.fs,
				archive_info->obj_name.hl,
				archive_info->obj_name.ll,
				archive_info->desc);
		}
	}

	rc = tsm_obj_update_crc32(&obj_attr, archive_info, crc32sum, session);
	CT_INFO("tsm_obj_update_crc32, rc: %d, crc32: 0x%08x (%010u)", rc,
		crc32sum, crc32sum);
	if (rc)
		CT_ERROR(EFAILED, "tsm_obj_update_crc32");

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
				     struct archive_info_t *archive_info)
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

static dsInt16_t tsm_archive_recursive(struct archive_info_t *archive_info,
				       struct session_t *session)
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
			rc = tsm_archive_generic(archive_info, -1, session);
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
			rc = tsm_archive_generic(archive_info, -1, session);
			if (rc) {
				CT_WARN("tsm_archive_generic failed: %s", archive_info->fpath);
				break;
			}
			if (do_recursive) {
				snprintf(archive_info->fpath, PATH_MAX, "%s/%s", dpath, entry->d_name);
				rc = tsm_archive_recursive(archive_info, session);
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
 * @param[in] session
 *
 * @return DSM_RC_SUCCESSFUL on success otherwise DSM_RC_UNSUCCESSFUL.
 */
dsInt16_t tsm_archive_fpath(const char *fs, const char *fpath, const char *desc,
                            int fd, const struct lu_fid_t *lu_fid,
			    struct session_t *session)
{
	int rc;
	struct archive_info_t archive_info;

	CT_INFO("tsm_archive_fpath:\n"
		"fs: %s, fpath: %s, desc: %s, fd: %d, *lu_fid: %p",
		fs, fpath, desc, fd, lu_fid);

	memset(&archive_info, 0, sizeof(struct archive_info_t));
	if (lu_fid)
		memcpy(&(archive_info.obj_info.lu_fid), lu_fid, sizeof(struct lu_fid_t));
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
		/* Archive (recursively) inside D. */
		return tsm_archive_recursive(&archive_info, session);
	else
		/* Archive regular file. */
		return tsm_archive_generic(&archive_info, fd, session);

	return rc;
}


/**
 * @brief Send dummy object to find the maximum number of mountpoints.
 *
 * The TSM API allows to create multiple sessions, thus
 * enabling multithreaded operations. This however requires, that the TSM server
 * has enabled multiple parallel mount points. There exists no low level TSM API
 * function to query this value. This function should be called by threaded
 * applications (e.g. lhsmtool_tsm) having multiple sessions opened to find
 * maximum number of allowed mountpoints (alias the number of maxium threads)
 * by sending DSM_OBJ_DIRECTORY and verifying whether transaction was
 * successful.
 *
 * @param[in] session Active session.
 * @param[in] fsname File space name
 * @return    DSM_RC_SUCCESSFUL on succes, otherwise ECONNABORTED, or
 *            ECONNREFUSED if maximum number of mountpoints is exceeded.
 */
dsInt16_t tsm_check_free_mountp(struct session_t *session, const char *fsname)
{
	dsInt16_t rc;

	/* Create dummy DSM_OBJ_DIRECTORY. */
	struct archive_info_t archive_info = {.fpath =
					      "/.mount/.test-maxnummp",
					      .desc = "node mountpoint check",
					      .obj_info = {.magic =
							   MAGIC_ID_V1,
							   .size.hi = 0,
							   .size.lo = 1},
					      .obj_name = {.hl = "/.mount",
							   .ll =
							   "/.test-maxnummp",
							   .objType =
							   DSM_OBJ_DIRECTORY}};
	strcpy(archive_info.obj_name.fs, fsname);

	/* Check if we have free mountpoints left for the node. */
	dsUint8_t vote_txn = DSM_VOTE_COMMIT;
	dsUint16_t err_reason = 0;
	mcBindKey mc_bind_key = {.stVersion =  mcBindKeyVersion};
	ObjAttr obj_attr;
	rc = obj_attr_prepare(&obj_attr, &archive_info);
	if (rc) {
		TSM_ERROR(session, rc, "obj_attr_prepare");
		return ECONNABORTED;
	}

	rc = dsmBeginTxn(session->handle);
	if (rc) {
		TSM_ERROR(session, rc, "dsmBeginTxn");
		return ECONNABORTED;
	}

	rc = dsmBindMC(session->handle, &(archive_info.obj_name), stArchive,
		       &mc_bind_key);
	if (rc) {
		TSM_ERROR(session, rc, "dsmBindMC");
		return ECONNABORTED;
	}

	sndArchiveData arch_data = {
		.stVersion = sndArchiveDataVersion,
		.descr = archive_info.desc};

	rc = dsmSendObj(session->handle, stArchive, &arch_data,
			&(archive_info.obj_name), &obj_attr, NULL);
	if (rc) {
		TSM_ERROR(session, rc, "dsmSendObj");
		return ECONNABORTED;
	}

	rc = dsmEndSendObj(session->handle);
	if (rc) {
		TSM_ERROR(session, rc, "dsmEndSendObj");
		return ECONNABORTED;
	}

	rc = dsmEndTxn(session->handle, vote_txn, &err_reason);
	if (rc) {
		TSM_DEBUG(session, err_reason, "dsmEndTxn reason");
		if (err_reason == DSM_RS_ABORT_EXCEED_MAX_MP)
			return ECONNREFUSED;
		else
			return ECONNABORTED;
	}

	/* Delete dummy DSM_OBJ_DIRECTORY. */
	rc = tsm_delete_fpath(fsname, archive_info.fpath, session);
	if (rc) {
		CT_ERROR(rc, "tsm_delete_fpath failed on '%s'",
			 archive_info.fpath);
		rc = ECONNABORTED;
	}
	else
		CT_INFO("passed mount point check");

	return rc;
}
