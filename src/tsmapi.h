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

/* Important note: The API can only restore or retrieve objects that were
   backed up or archived using API calls
   (see Chapter 3. API design recommendations and considerations page 55.) */

#ifndef TSMAPI_H
#define TSMAPI_H

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

#define MAGIC_ID 71147

typedef struct {
    char node[DSM_MAX_ID_LENGTH + 1];
    char password[MAX_PASSWORD_LENGTH + 1];
    char username[MAX_USERNAME_LENGTH + 1];
    char platform[DSM_MAX_PLATFORM_LENGTH + 1];
    char options[MAX_OPTIONS_LENGTH + 1];
} login_t;

typedef struct {
    dsUint64_t f_seq;	/* __u64 */
    dsUint32_t f_oid;	/* __u32 */
    dsUint32_t f_ver;	/* __u32 */
} lu_fid_t;

/* Own object description exchanged with TSM. */
typedef struct {
    unsigned int magic;
    dsUint8_t objType;
    dsStruct64_t size;
    lu_fid_t lu_fid;
 } obj_info_t;

typedef struct {
    unsigned long capacity;	/* Number of maximum elements. If max capacity is reached,
				   then capacity is doubled  by means realloc. */
    unsigned long N;		/* Number of actual elements. */
    qryRespArchiveData *data;	/* Array of query replies from TSM. */
} query_arr_t;


off_t to_off_t(const dsStruct64_t size); /* Convert dsStruct64_t to off_t required for filesize conversation. */
dsStruct64_t to_dsStruct64_t(const off_t size); /* Convert off_t to dsStruct64. */

dsInt16_t tsm_init(login_t *login);
void tsm_quit();

dsInt16_t tsm_query_session_info();
dsInt16_t tsm_archive_file(const char *fs, const char *filename, const char *desc);
dsInt16_t tsm_query_hl_ll(const char *fs, const char *hl, const char *ll, const char *desc, dsBool_t display);
dsInt16_t tsm_query_file(const char *fs, const char *filename, const char *desc, dsBool_t display);
dsInt16_t tsm_delete_file(const char *fs, const char *filename);
dsInt16_t tsm_delete_hl_ll(const char *fs, const char *hl, const char *ll);
dsInt16_t tsm_retrieve_file(const char *fs, const char *filename, const char *desc);
dsInt16_t tsm_retrieve_hl_ll(const char *fs, const char *hl, const char *ll, const char *desc);

#endif /* TSMAPI_H */
