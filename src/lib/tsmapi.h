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
   Moreover the API doesn't support subdir operations, that is, hl/ll
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

#include <stdint.h>
#include <sys/types.h>
#include "dsmapitd.h"
#include "dsmapifp.h"
#include "dsmapips.h"
#include "dsmrc.h"
#include "dapint64.h"
#include "log.h"
#include "chashtable.h"

#ifndef PACKAGE_VERSION
#define PACKAGE_VERSION "NA"
#endif

#define DEFAULT_FSNAME "/"
#define DEFAULT_FSTYPE "ltsm"
#define LINUX_PLATFORM "GNU/Linux"

#define TSM_BUF_LENGTH 65536
#define MAX_OPTIONS_LENGTH 256
#define MAGIC_ID_V1 71147
#define DEFAULT_NUM_BUCKETS 64

#define OPTNCMP(str1, str2)			\
	((strlen(str1) == strlen(str2)) &&	\
	 (strncmp(str1, str2, strlen(str1)) == 0))

struct login_t{
	char node[DSM_MAX_NODE_LENGTH + 1];
	char password[DSM_MAX_VERIFIER_LENGTH + 1];
	char owner[DSM_MAX_OWNER_LENGTH + 1];
	char platform[DSM_MAX_PLATFORM_LENGTH + 1];
	char options[MAX_OPTIONS_LENGTH + 1];
	char fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char fstype[DSM_MAX_FSTYPE_LENGTH + 1];
};

struct lu_fid_t{
	dsUint64_t f_seq;
	dsUint32_t f_oid;
	dsUint32_t f_ver;
};

struct obj_info_t{
	unsigned int magic;
	dsStruct64_t size;
	mode_t st_mode;
	struct lu_fid_t lu_fid;
};

struct archive_info_t{
	char fpath[PATH_MAX + 1];
	char desc[DSM_MAX_DESCR_LENGTH + 1];
	struct obj_info_t obj_info;
	dsmObjName obj_name;
};

struct qarray_t {
	qryRespArchiveData *data;
	uint32_t size;
};

struct qtable_t {
	chashtable_t *chashtable;
	uint32_t nbuckets;
	dsmBool_t multiple;
	struct qarray_t qarray;
};

struct progress_size_t {
	ssize_t cur;
	ssize_t cur_total;
	ssize_t total;
};

struct session_t {
	dsUint32_t handle;
	struct qtable_t qtable;
	dsmBool_t overwrite_older;
	struct hsm_action_item *hai;
	struct hsm_copyaction_private *hcp;
	long hal_flags;
	int (*progress)(struct progress_size_t *data,
			struct session_t *session);
};

off64_t to_off64_t(const dsStruct64_t size);
dsStruct64_t to_dsStruct64_t(const off_t size);
void set_recursive(const dsBool_t recursive);
void select_latest(const dsBool_t latest);

void login_fill(struct login_t *login, const char *servername,
		const char *node, const char *password,
		const char *owner, const char *platform,
		const char *fsname, const char *fstype);

dsInt16_t tsm_init(const dsBool_t mt_flag);
void tsm_cleanup(const dsBool_t mt_flag);

dsInt16_t tsm_connect(struct login_t *login, struct session_t *session);
void tsm_disconnect(struct session_t *session);

dsmAppVersion get_appapi_ver();
dsmApiVersionEx get_libapi_ver();

dsInt16_t tsm_query_session(struct session_t *session);
dsInt16_t tsm_archive_fpath(const char *fs, const char *fpath,
			    const char *desc, int fd,
			    const struct lu_fid_t *lu_fid,
			    struct session_t *session);
dsInt16_t tsm_query_fpath(const char *fs, const char *fpath,
			  const char *desc, dsBool_t display,
			  struct  session_t *session);
dsInt16_t tsm_delete_fpath(const char *fs, const char *fpath,
			   struct  session_t *session);
dsInt16_t tsm_retrieve_fpath(const char *fs, const char *fpath,
			     const char *desc, int fd,
			     struct session_t *session);

#endif /* TSMAPI_H */
