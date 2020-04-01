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
 * Copyright (c) 2016-2019, GSI Helmholtz Centre for Heavy Ion Research
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
#include <stdbool.h>
#include <limits.h>
#include <sys/types.h>
#include "dsmapitd.h"
#include "dsmapifp.h"
#include "dsmapips.h"
#include "dsmrc.h"
#include "dapint64.h"
#include "common.h"
#include "log.h"
#include "chashtable.h"

#ifndef PACKAGE_VERSION
#define PACKAGE_VERSION "NA"
#endif

#define MAGIC_ID_V1 71147
#define DEFAULT_NUM_BUCKETS 64

enum sort_by_t {
	SORT_NONE	     = 0,
	SORT_DATE_ASCENDING  = 1,
	SORT_DATE_DESCENDING = 2,
	SORT_RESTORE_ORDER   = 3
};

struct fid_t {
	uint64_t seq;
	uint32_t oid;
	uint32_t ver;
};

struct lov_t {
	uint32_t stripe_size;
	uint16_t stripe_count;
#ifdef LOV_MAGIC_V3
	char pool_name[LOV_MAXPOOLNAME + 1];
#endif
};

struct lustre_info_t {
	struct fid_t fid;
	struct lov_t lov;
};

struct obj_info_t {
	uint32_t magic;
	dsStruct64_t size;
	mode_t st_mode;
	uint32_t crc32;
	struct lustre_info_t lustre_info;
};

struct archive_info_t {
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
	enum sort_by_t sort_by;
	struct qarray_t qarray;
};

struct progress_size_t {
	ssize_t cur;
	ssize_t cur_total;
	ssize_t total;
};

struct tsm_file_t {
	ObjAttr obj_attr;
	struct archive_info_t archive_info;
	off64_t bytes_processed;
	int err;
};

struct session_t {
	dsUint32_t handle;
	char owner[DSM_MAX_OWNER_LENGTH + 1];
	struct qtable_t qtable;

	struct hsm_action_item *hai;
	struct hsm_copyaction_private *hcp;
	long hal_flags;
	int (*progress)(struct progress_size_t *data,
			struct session_t *session);

	struct tsm_file_t *tsm_file;
};

void set_recursive(const dsBool_t recursive);
void select_latest(const dsBool_t latest);
void set_prefix(const char *_prefix);
void set_restore_stripe(const dsBool_t _restore_stripe);
int parse_verbose(const char *val, int *opt_verbose);
int mkdir_p(const char *path, const mode_t st_mode);
dsInt16_t extract_hl_ll(const char *fpath, const char *fs,
			char *hl, char *ll);

void login_fill(struct login_t *login, const char *servername,
		const char *node, const char *password,
		const char *owner, const char *platform,
		const char *fsname, const char *fstype);

dsInt16_t tsm_init(const dsBool_t mt_flag);
void tsm_cleanup(const dsBool_t mt_flag);

dsInt16_t tsm_connect(struct login_t *login, struct session_t *session);
void tsm_disconnect(struct session_t *session);

dsmAppVersion get_appapi_ver(void);
dsmApiVersionEx get_libapi_ver(void);

dsInt16_t tsm_check_free_mountp(const char *fs, struct session_t *session);
dsInt16_t tsm_query_session(struct session_t *session);
dsInt16_t tsm_archive_fpath(const char *fs, const char *fpath,
			    const char *desc, int fd,
			    const struct lustre_info_t *lustre_info,
			    struct session_t *session);
dsInt16_t tsm_query_fpath(const char *fs, const char *fpath,
			  const char *desc, const dsmDate *date_lower_bound,
			  const dsmDate *date_upper_bound,
			  struct  session_t *session);
dsInt16_t tsm_delete_fpath(const char *fs, const char *fpath,
			   struct  session_t *session);
dsInt16_t tsm_retrieve_fpath(const char *fs, const char *fpath,
			     const char *desc, int fd,
			     struct session_t *session);

#ifdef HAVE_LUSTRE
int xattr_get_lov(const int fd, struct lustre_info_t *lustre_info,
		  const char *fpath);
int xattr_set_lov(int fd, const struct lustre_info_t *lustre_info,
		  const char *fpath);
#endif

int tsm_fconnect(struct login_t *login, struct session_t *session);
void tsm_fdisconnect(struct session_t *session);
int tsm_fopen(const char *fs, const char *fpath, const char *desc,
	      struct session_t *session);
ssize_t tsm_fwrite(const void *ptr, size_t size, size_t nmemb,
		   struct session_t *session);
int tsm_fclose(struct session_t *session);

#endif /* TSMAPI_H */
