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
 * Copyright (c) 2016, 2017, GSI Helmholtz Centre for Heavy Ion Research
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
#include "log.h"
#include "chashtable.h"

#ifndef PACKAGE_VERSION
#define PACKAGE_VERSION "NA"
#endif

#define DEFAULT_FSNAME "/"
#define DEFAULT_FSTYPE "ltsm"
#define LINUX_PLATFORM "GNU/Linux"

#define TSM_BUF_LENGTH 32764 /* (32768 - 4) gives best transfer performance. */
#define MAX_OPTIONS_LENGTH 64
#define MAGIC_ID_V1 71147
#define DEFAULT_NUM_BUCKETS 64

#ifndef DSM_MAX_FSNAME_LENGTH
#define DSM_MAX_FSNAME_LENGTH 1024
#endif
#ifndef DSM_MAX_DESCR_LENGTH
#define DSM_MAX_DESCR_LENGTH 255
#endif

#define OPTNCMP(str1, str2)			\
	((strlen(str1) == strlen(str2)) &&	\
	 (strncmp(str1, str2, strlen(str1)) == 0))

#define FSD_PROTOCOL_STR(s)								 \
	s == FSD_CONNECT    ? "FSD_CONNECT"    :					 \
	s == FSD_OPEN       ? "FSD_OPEN"       :					 \
	s == FSD_DATA       ? "FSD_DATA"       :					 \
	s == FSD_CLOSE      ? "FSD_CLOSE"      :					 \
	s == FSD_DISCONNECT ? "FSD_DISCONNECT" :					 \
	s == (FSD_DISCONNECT | FSD_OPEN) ? "FSD_DISCONNECT | FSD_OPEN" : "UNKNOWN"       \

enum sort_by_t {
	SORT_NONE	     = 0,
	SORT_DATE_ASCENDING  = 1,
	SORT_DATE_DESCENDING = 2,
	SORT_RESTORE_ORDER   = 3
};

enum fsd_protocol_state_t {
	FSD_CONNECT    = 0x1,
	FSD_OPEN       = 0x2,
	FSD_DATA       = 0x4,
	FSD_CLOSE      = 0x8,
	FSD_DISCONNECT = 0x10,
};

struct login_t {
	/* TSM */
	char node[DSM_MAX_NODE_LENGTH + 1];
	char password[DSM_MAX_VERIFIER_LENGTH + 1];
	char owner[DSM_MAX_OWNER_LENGTH + 1];
	char platform[DSM_MAX_PLATFORM_LENGTH + 1];
	char options[MAX_OPTIONS_LENGTH + 1];
	char fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char fstype[DSM_MAX_FSTYPE_LENGTH + 1];
	/* FSD */
	char hostname[HOST_NAME_MAX + 1];
	int port;
};

struct fsd_info_t {
	char fs[DSM_MAX_FSNAME_LENGTH + 1];
	char fpath[PATH_MAX + 1];
	char desc[DSM_MAX_DESCR_LENGTH + 1];
};

struct fsd_protocol_t {
	enum fsd_protocol_state_t state;
	struct login_t login;
	struct fsd_info_t fsd_info;
	int sock_fd;
	size_t size;
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

	struct fsd_protocol_t fsd_protocol;
};

struct kv {
	char key[MAX_OPTIONS_LENGTH + 1];
	char val[MAX_OPTIONS_LENGTH + 1];
};

struct kv_opt {
	uint8_t N;
	struct kv *kv;
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

dsmAppVersion get_appapi_ver();
dsmApiVersionEx get_libapi_ver();

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

int crc32file(const char *filename, uint32_t *crc32result);
int parse_conf(const char *filename, struct kv_opt *kv_opt);
ssize_t read_size(int fd, void *ptr, size_t n);
ssize_t write_size(int fd, const void *ptr, size_t n);

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

void fsd_login_fill(struct login_t *login, const char *servername,
		    const char *node, const char *password,
		    const char *owner, const char *platform,
		    const char *fsname, const char *fstype,
		    const char *hostname, const int port);
int fsd_tsm_fconnect(struct login_t *login, struct session_t *session);
void fsd_tsm_fdisconnect(struct session_t *session);
int fsd_tsm_fopen(const char *fs, const char *fpath, const char *desc,
                  struct session_t *session);
ssize_t fsd_tsm_fwrite(const void *ptr, size_t size, size_t nmemb,
                       struct session_t *session);
int fsd_tsm_fclose(struct session_t *session);

#endif /* TSMAPI_H */
