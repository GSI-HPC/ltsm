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
 * Copyright (c) 2019, GSI Helmholtz Centre for Heavy Ion Research
 */

#ifndef COMMON_H
#define COMMON_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <zlib.h>
#include "log.h"

#ifndef DEFAULT_FSNAME
#define DEFAULT_FSNAME	"/"
#endif

#ifndef DEFAULT_FSTYPE
#define DEFAULT_FSTYPE	"ltsm"
#endif

#ifndef DEFAULT_OWNER
#define DEFAULT_OWNER	""
#endif

#ifndef LINUX_PLATFORM
#define LINUX_PLATFORM	"GNU/Linux"
#endif

#ifndef TSM_BUF_LENGTH
#define TSM_BUF_LENGTH	262144	/* 256 KiB. */
#endif

#ifndef MAX_OPTIONS_LENGTH
#define MAX_OPTIONS_LENGTH	64
#endif

/* Archive description. */
#ifndef DSM_MAX_DESCR_LENGTH
#define DSM_MAX_DESCR_LENGTH	255
#endif

/* Filespace name. */
#ifndef DSM_MAX_FSNAME_LENGTH
#define DSM_MAX_FSNAME_LENGTH	1024
#endif

/* Filespace type. */
#ifndef DSM_MAX_FSTYPE_LENGTH
#define DSM_MAX_FSTYPE_LENGTH	32
#endif

/* Object owner name. */
#ifndef DSM_MAX_OWNER_LENGTH
#define DSM_MAX_OWNER_LENGTH	64
#endif

/* Password length. */
#ifndef DSM_MAX_VERIFIER_LENGTH
#define DSM_MAX_VERIFIER_LENGTH 64
#endif

/* Node/machine name. */
#ifndef DSM_MAX_NODE_LENGTH
#define DSM_MAX_NODE_LENGTH	64
#endif

/* Application type. */
#ifndef DSM_MAX_PLATFORM_LENGTH
#define DSM_MAX_PLATFORM_LENGTH 16
#endif

/* Length of fsq error message. */
#ifndef FSQ_MAX_ERRMSG_LENGTH
#define FSQ_MAX_ERRMSG_LENGTH 1024
#endif

#define OPTNCMP(str1, str2)				\
	((strlen(str1) == strlen(str2)) &&		\
	 (strncmp(str1, str2, strlen(str1)) == 0))

struct login_t {
        char node[DSM_MAX_NODE_LENGTH + 1];
        char password[DSM_MAX_VERIFIER_LENGTH + 1];
        char owner[DSM_MAX_OWNER_LENGTH + 1];
        char platform[DSM_MAX_PLATFORM_LENGTH + 1];
        char options[MAX_OPTIONS_LENGTH + 1];
        char fsname[DSM_MAX_FSNAME_LENGTH + 1];
        char fstype[DSM_MAX_FSTYPE_LENGTH + 1];
};

struct kv {
	char key[MAX_OPTIONS_LENGTH + 1];
	char val[MAX_OPTIONS_LENGTH + 1];
};

struct kv_opt {
	uint8_t N;
	struct kv *kv;
};

ssize_t read_size(int fd, void *ptr, size_t n);
ssize_t write_size(int fd, const void *ptr, size_t n);
int parse_conf(const char *filename, struct kv_opt *kv_opt);
int crc32file(const char *filename, uint32_t *crc32result);
void login_init(struct login_t *login, const char *servername,
                const char *node, const char *password,
                const char *owner, const char *platform,
                const char *fsname, const char *fstype);

#endif	/* COMMON_H */
