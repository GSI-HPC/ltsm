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
 * Copyright (c) 2016, Thomas Stibor <t.stibor@gsi.de>
 */

/* Important note: The API can only restore or retrieve objects that were
   archived using TSM API calls, that is, data archived with dsmc cannot
   be retrieved with these TSM API calls and console client ltsmc.
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

#include <sys/types.h>
#include "dsmapitd.h"
#include "dsmapifp.h"
#include "dsmapips.h"
#include "dsmrc.h"
#include "dapint64.h"
#include "log.h"

#define TSM_BUF_LENGTH 65536
#define MAX_PASSWORD_LENGTH 32
#define MAX_USERNAME_LENGTH 32
#define MAX_OPTIONS_LENGTH 256
#define MAX_OFF_T_DS64 20

#define MAGIC_ID_V1 71147

typedef struct {
	char node[DSM_MAX_ID_LENGTH + 1];
	char password[MAX_PASSWORD_LENGTH + 1];
	char username[MAX_USERNAME_LENGTH + 1];
	char platform[DSM_MAX_PLATFORM_LENGTH + 1];
	char options[MAX_OPTIONS_LENGTH + 1];
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
} query_arr_t;

dsBool_t do_recursive;

off_t to_off_t(const dsStruct64_t size);
dsStruct64_t to_dsStruct64_t(const off_t size);
void set_recursive(dsBool_t recursive);

dsInt16_t tsm_init(login_t *login);
void tsm_quit();

dsInt16_t tsm_query_session_info();
dsInt16_t tsm_archive_fpath(const char *fs, const char *fpath, const char *desc);
dsInt16_t tsm_archive_fid(const char *fs, const char *fpath, const char *desc,
			  const lu_fid_t *lu_fid);
dsInt16_t tsm_query_fpath(const char *fs, const char *fpath,
			  const char *desc, dsBool_t display);
dsInt16_t tsm_delete_fpath(const char *fs, const char *fpath);
dsInt16_t tsm_retrieve_fpath(const char *fs, const char *fpath,
			     const char *desc);
dsInt16_t tsm_retrieve_fpath_fd(const char *fs, const char *fpath,
				const char *desc, int l_fd);

#endif /* TSMAPI_H */
