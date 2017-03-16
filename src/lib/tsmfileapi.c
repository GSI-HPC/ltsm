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
#if HAVE_CONFIG_H
 #include <config.h>
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


#include "qtable.h"
#include "tsmapi.h"
#include "tsmfileapi.h"
#include "log.h"



static int tsm_file_open_read(struct tsm_filehandle_t *fh)
{
	int rc;

	rc = init_qtable(&fh->session.qtable);
	fh->session.qtable.multiple = 0;
	if (rc) {
		CT_ERROR(rc, "init qtable failed");
		return rc;
	}
	rc = tsm_query_hl_ll(fh->login->fsname, fh->archive_info.obj_name.hl,
		             fh->archive_info.obj_name.ll, fh->o_desc,
		             &fh->session);
	if (rc) {
		CT_ERROR(rc, "tsm_query_hl_ll failed");
		return rc;
	}
	qryRespArchiveData query_data;
	rc = create_array(&fh->session.qtable, 0);
	if (rc) {
		CT_ERROR(rc, "create_array failed");
		return rc;
	}
	if (fh->session.qtable.qarray.size != 1) {
		CT_ERROR(rc, "qtable does not have exactly one object");
		return ENOENT;
	}

	rc = get_qra(&fh->session.qtable, &query_data, 0);
	if (rc) {
		CT_ERROR(rc, "get_qra failed");
		return rc;
	}

	dsmGetList get_list;
	get_list.numObjId = 1;
	get_list.stVersion = dsmGetListVersion;
	get_list.objId = malloc(sizeof(ObjID));
	if (get_list.objId == NULL) {
		CT_ERROR(rc, "get_list.objId space reservation failed");
		return rc;
	}
	get_list.objId[0] = query_data.objId;

	rc = dsmBeginGetData(fh->session.handle, bTrue, gtArchive, &get_list);
	if (rc) {
		CT_ERROR(rc, "dsmBeginGetData failed");
		goto cleanup;
	}
	rc = dsmGetObj(fh->session.handle, &(query_data.objId), NULL);
	if (rc == DSM_RC_MORE_DATA) rc = DSM_RC_SUCCESSFUL;
	if (rc) {
		CT_ERROR(rc, "dsmGetObj failed");
	}

	CT_DEBUG("TSM Object to read from has fs:'%s' hl:'%s' ll:'%s'",
		query_data.objName.fs,
		query_data.objName.hl,
		query_data.objName.ll);

cleanup:
	free(get_list.objId);
	return rc;
}

static int tsm_file_open_write(struct tsm_filehandle_t *fh)
{
	int rc;
	rc = dsmBeginTxn(fh->session.handle);
	if (rc) {
		CT_ERROR(rc, "dsmBeginTxn failed");
		return rc;
	}
	fh->archive_info.obj_name.objType = DSM_OBJ_FILE;
	strncpy(fh->archive_info.obj_name.fs, fh->login->fsname,
		sizeof(fh->login->fsname));

	fh->mc_bind_key.stVersion = mcBindKeyVersion;
	rc = dsmBindMC(fh->session.handle, &(fh->archive_info.obj_name),
		       stArchive, &fh->mc_bind_key);
	if (rc) {
		CT_ERROR(rc, "dsmBindMC failed");
		return rc;
	}
	struct obj_info_t ob;
	ob.size.hi = ~((dsUint32_t)0);
	ob.size.lo = ~((dsUint32_t)0);
	ob.magic = MAGIC_ID_V1;

	sndArchiveData arch_data;
	arch_data.descr = fh->o_desc;
	arch_data.stVersion = sndArchiveDataVersion;

	fh->obj_attr.owner[0] = '\0';
	fh->obj_attr.objInfoLength = sizeof(struct obj_info_t);
	fh->obj_attr.objInfo = (char *)malloc(fh->obj_attr.objInfoLength);
	if (fh->obj_attr.objInfo == NULL) {
		CT_ERROR(rc, "fh->obj_attr.objInfo space reservation failed");
		return rc;
	}
	memcpy(fh->obj_attr.objInfo, (char *)&(ob), fh->obj_attr.objInfoLength);
	fh->obj_attr.sizeEstimate.hi = ob.size.hi;
	fh->obj_attr.sizeEstimate.lo = ob.size.lo;
	fh->obj_attr.stVersion = ObjAttrVersion;

	rc = dsmSendObj(fh->session.handle, stArchive, &arch_data,
			 &(fh->archive_info.obj_name), &fh->obj_attr, NULL);
	if (rc) {
		CT_ERROR(rc, "dsmSendObj failed");
	}
	return rc;
}

/**
 * @brief opens a filestream to an tsm-object
 *
 * Takes the login credentials to connect to a tsm server and request a
 * tsm-object specified by path and description.
 * If opened in TSM_FILE_MODE_READ everything is set up to read from the latest
 * matched tsm object via tsm_file_read.
 * If opened in TSM_FILE_MODE_WRITE everything is set up to write data into the
 * tsm object via tsm_file_write.
 * Assure dsmSetup(...) is called before this function especialy if using
 * multiple threads.
 *
 * @param[in] fh filehandle which will be used for further actions
 * @param[in] login the login information to connect to the tsm server
 * @param[in] path the path (hl and ll) to the tsm object
 * @param[in] desc description of tsm object
 * @param[in] mode TSM_FILE_MODE_READ or TSM_FILE_MODE_WRITE
 * @return DSM_RC_SUCCESSFUL on success. error code otherwise
 */
int tsm_file_open(struct tsm_filehandle_t *fh, struct login_t* login,
	          char* path, char* desc, int mode)
{
	int rc;
	strcpy(fh->o_desc, desc);
	fh->login = login;
	fh->mode = mode;

	rc = tsm_connect(fh->login, &fh->session);
	if (rc) {
		CT_ERROR(rc, "tsm_connect failed for %s", path);
		return rc;
	}

	rc = tsm_query_session(&fh->session, login->fsname);
	if (rc) {
		CT_ERROR(rc, "tsm_query_session failed for %s", path);
		return rc;
	}

	rc = extract_hl_ll(path, fh->archive_info.obj_name.hl,
			   fh->archive_info.obj_name.ll);
	if (rc) {
		CT_ERROR(rc, "extract_hl_ll failed for %s", path);
		return rc;
	}

	switch (mode) {
		case TSM_FILE_MODE_READ : {
			return  tsm_file_open_read(fh);
		}
		case TSM_FILE_MODE_WRITE : {
			return tsm_file_open_write(fh);
		}
		default: {
			rc = DSM_RC_UNSUCCESSFUL;
			CT_ERROR(rc, "unknown file open mode %i for %s",
				 mode ,path);
			return rc;
		}
	}
}

/**
 * @brief writes data from buffer into a tsm file
 *
 * Writes data into a tsm-object which was previously setup with tsm_file_open
 * in write mode.
 * Only when the file gets closed the tsm transaction will be commited.
 *
 * @param[in] fh filehandle for tsm-file connection
 * @param[in] data_ptr buffer with data to write
 * @param[in] datasize size of one buffer element
 * @param[in] elements number of elements from buffer to write
 * @return DSM_RC_SUCCESSFUL on success. error code otherwise
 */
int tsm_file_write(struct tsm_filehandle_t *fh, void* data_ptr,
		   size_t datasize, size_t elements)
{
	if (fh->mode != TSM_FILE_MODE_WRITE)
		return DSM_RC_UNSUCCESSFUL;
	int rc;
	fh->data_blk.bufferLen = datasize*elements;
	fh->data_blk.bufferPtr = data_ptr;
	fh->data_blk.stVersion = DataBlkVersion;
	rc = dsmSendData(fh->session.handle, &fh->data_blk);
	if (rc) {
		CT_ERROR(rc, "dsmSendData failed");
		return rc;
	}
	return rc;
}

/**
 * @brief reads data from tsm-file object into buffer
 *
 *
 * @param[in] fh filehandle for tsm-file connection
 * @param[in] data_ptr buffer where data should be stored
 * @param[in] datasize size of one buffer element
 * @param[in] elements number of elements to read and store to buffer
 * @param[in] read pointer to int where actual read byte count will be stored
 * @return int DSM_RC_MORE_DATA if more data in tsm file or DSM_RC_FINISHED
 * if EOF. Some error code otherwise
 */
int tsm_file_read(struct tsm_filehandle_t *fh, void* data_ptr,
		  size_t datasize, size_t elements, size_t *read)
{
	if (fh->mode != TSM_FILE_MODE_READ)
		return DSM_RC_UNSUCCESSFUL;
	int rc;
	fh->data_blk.bufferLen = datasize*elements;
	fh->data_blk.bufferPtr = data_ptr;
	fh->data_blk.stVersion = DataBlkVersion;
	fh->data_blk.numBytes = 0;
	rc = dsmGetData(fh->session.handle, &fh->data_blk);
	*read = fh->data_blk.numBytes;
	if (!(rc == DSM_RC_MORE_DATA || rc == DSM_RC_FINISHED)) {
		CT_ERROR(rc, "dsmGetData failed");
		return rc;
	}
	return rc;
}


static  int tsm_file_close_write(struct tsm_filehandle_t *fh)
{
	int rc;
	rc = dsmEndSendObj(fh->session.handle);
	if (rc) {
		CT_ERROR(rc, "dsmEndSendObj failed");
	}
	dsUint16_t reason;
	rc = dsmEndTxn(fh->session.handle, DSM_VOTE_COMMIT, &reason);
	if (rc) {
		CT_ERROR(rc, "dsmEndTxn failed with reason %i", reason);
	}
	free(fh->obj_attr.objInfo);
	return rc;
}

static  int tsm_file_close_read(struct tsm_filehandle_t *fh)
{
	int rc;
	rc = dsmEndGetData(fh->session.handle);
	if (rc) {
		CT_ERROR(rc, "dsmEndGetData failed");
	}
	destroy_qtable(&fh->session.qtable);
	return rc;
}

/**
 * @brief closes a filestream to an
  tsm-object
 *
 * Shutdown the connection to the tsm-server.
 * If file was opened in write mode the transaction will be commited.
 * Cleanup with dsmcleanup(...) after all files closed with this function.
 *
 * @param[in] fh filehandle for tsm-file connection
 * @return DSM_RC_SUCCESSFUL on success. error code otherwise
 */
int tsm_file_close(struct tsm_filehandle_t *fh)
{
	int rc;
	switch (fh->mode) {
		case TSM_FILE_MODE_READ : {
			rc = tsm_file_close_read(fh);
			break;
		}
		case TSM_FILE_MODE_WRITE : {
			rc = tsm_file_close_write(fh);
			break;
		}
		default: {
			rc = DSM_RC_UNSUCCESSFUL;;
			break;
		}
	}
	tsm_disconnect(&fh->session);
	return rc;
}