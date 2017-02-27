/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Copyright (c) 2016, 2017 Thomas Stibor <t.stibor@gsi.de>
 */

/* Important note: The API can only retrieve objects that were
   archived using TSM API calls, that is, data archived with dsmc cannot
   be retrieved with these TSM API calls or console client ltsmc.
   Moreover the API doesn't support subdir opperations, that is, hl/ll
   queries must be constructed (in a clever way) with wildcard (*) and
   question mark (?) to match sub directories and files. For more detail c.f.
   PDF Dokument: Using the Application Programming Interface
   (http://www.ibm.com/support/knowledgecenter/SSGSG7_7.1.3/api/b_api_using.pdf)
*/

#ifndef TSMAPI_H
#define TSMAPI_H

#if HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef HAVE_LUSTRE
#include <lustre/lustreapi.h>
#endif

#include <sys/types.h>
#include "dsmapitd.h"
#include "dsmapifp.h"
#include "dsmapips.h"
#include "dsmrc.h"
#include "dapint64.h"
#include "log.h"

#define FSNAME "/"
#define FSTYPE "ltsm"
#define LOGIN_PLATFORM "GNU/Linux"

#define TSM_BUF_LENGTH 65536
#define MAX_OPTIONS_LENGTH 256

#define MAGIC_ID_V1 71147

typedef struct {
	char node[DSM_MAX_NODE_LENGTH + 1];
	char password[DSM_MAX_VERIFIER_LENGTH + 1];
	char owner[DSM_MAX_OWNER_LENGTH + 1];
	char platform[DSM_MAX_PLATFORM_LENGTH + 1];
	char options[MAX_OPTIONS_LENGTH + 1];
	char fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char fstype[DSM_MAX_FSTYPE_LENGTH + 1];
} login_t;

typedef struct {
	dsUint64_t f_seq;
	dsUint32_t f_oid;
	dsUint32_t f_ver;
} lu_fid_t;

/* ltsm object description. */
typedef struct {
	unsigned int magic;
	dsStruct64_t size;
	mode_t st_mode;
	lu_fid_t lu_fid;
} obj_info_t;

typedef struct {
	char fpath[PATH_MAX + 1];
	char desc[DSM_MAX_DESCR_LENGTH + 1];
	obj_info_t obj_info;
	dsmObjName obj_name;
} archive_info_t;

typedef struct {
	unsigned long capacity;
	unsigned long N;
	qryRespArchiveData *data;
	struct hsearch_data *htab;
} qarray_t;

typedef struct {
	dsUint16_t id;
	dsUint32_t handle;
	qarray_t *qarray;
	dsmBool_t overwrite_older;
	struct hsm_action_item *hai;
	struct hsm_copyaction_private *hcp;
	long hal_flags;
	void (*progress)(void *data, void *session);
} session_t;

typedef struct {
	ssize_t cur;
	ssize_t cur_total;
	ssize_t total;
} progress_size_t;

off64_t to_off64_t(const dsStruct64_t size);
dsStruct64_t to_dsStruct64_t(const off_t size);
void set_recursive(const dsBool_t recursive);
void select_latest(const dsBool_t latest);

void login_fill(login_t *login, const char *servername,
		const char *node, const char *password,
		const char *owner, const char *platform,
		const char *fsname, const char *fstype);

dsInt16_t tsm_init(const dsBool_t mt_flag);
void tsm_cleanup(const dsBool_t mt_flag);

dsInt16_t tsm_connect(login_t *login, session_t *session);
void tsm_disconnect(session_t *session);

dsmAppVersion get_appapi_ver();
dsmApiVersionEx get_libapi_ver();

dsInt16_t tsm_query_session(session_t *session);
dsInt16_t tsm_archive_fpath(const char *fs, const char *fpath, const char *desc,
			    int fd, const lu_fid_t *lu_fid, session_t *session);
dsInt16_t tsm_query_fpath(const char *fs, const char *fpath,
			  const char *desc, dsBool_t display,
			  session_t *session);
dsInt16_t tsm_delete_fpath(const char *fs, const char *fpath,
			   session_t *session);
dsInt16_t tsm_retrieve_fpath(const char *fs, const char *fpath,
			     const char *desc, int fd, session_t *session);

#endif /* TSMAPI_H */
