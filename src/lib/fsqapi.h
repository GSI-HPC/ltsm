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
 * Copyright (c) 2019-2022, GSI Helmholtz Centre for Heavy Ion Research
 */

#ifndef FSQAPI_H
#define FSQAPI_H

#if HAVE_CONFIG_H
#include <config.h>
#else
#error "Missing autoconf generated config.h file"
#endif

#include <limits.h>
#include "common.h"

#define FSQ_PORT_DEFAULT        7625

#define XATTR_FSQ_PREFIX	"user.fsq"
#define XATTR_FSQ_STATE		XATTR_FSQ_PREFIX".state"
#define XATTR_FSQ_ARCHIVE_ID	XATTR_FSQ_PREFIX".arvid"
#define XATTR_FSQ_FS		XATTR_FSQ_PREFIX".fs"
#define XATTR_FSQ_FPATH		XATTR_FSQ_PREFIX".fpath"
#define XATTR_FSQ_DESC		XATTR_FSQ_PREFIX".desc"
#define XATTR_FSQ_STOR_DEST	XATTR_FSQ_PREFIX".stordest"

#define FSQ_PROTOCOL_VER_STR(s) \
	s == 1 ? "1" : "NA"	\

#define FSQ_PROTOCOL_STR(s)						             \
	s == FSQ_CONNECT                  ? "FSQ_CONNECT"                :           \
	s == (FSQ_CONNECT | FSQ_REPLY)    ? "FSQ_CONNECT | FSQ_REPLY"    :           \
	s == FSQ_OPEN                     ? "FSQ_OPEN"                   :           \
	s == (FSQ_OPEN | FSQ_REPLY)       ? "FSQ_OPEN | FSQ_REPLY"       :           \
        s == FSQ_DATA                     ? "FSQ_DATA"                   :           \
	s == (FSQ_DATA | FSQ_REPLY)       ? "FSQ_DATA | FSQ_REPLY"       :           \
	s == FSQ_CLOSE                    ? "FSQ_CLOSE"                  :           \
	s == (FSQ_CLOSE | FSQ_REPLY)      ? "FSQ_CLOSE | FSQ_REPLY"      :           \
	s == FSQ_DISCONNECT               ? "FSQ_DISCONNECT"             :           \
	s == (FSQ_DISCONNECT | FSQ_REPLY) ? "FSQ_DISCONNECT | FSQ_REPLY" :           \
	s == (FSQ_OPEN | FSQ_DISCONNECT)  ? "FSQ_OPEN | FSQ_DISCONNECT"  :           \
	s == (FSQ_DATA | FSQ_CLOSE)       ? "FSQ_DATA | FSQ_CLOSE"       :           \
	s == (FSQ_ERROR | FSQ_REPLY)      ? "FSQ_ERROR | FSQ_REPLY"      : "UNKNOWN" \

#define FSQ_ACTION_STR(s)						     \
	s == STATE_LOCAL_COPY_DONE   ? "STATE_LOCAL_COPY_DONE"   :	     \
	s == STATE_LUSTRE_COPY_RUN   ? "STATE_LUSTRE_COPY_RUN"   :	     \
	s == STATE_LUSTRE_COPY_ERROR ? "STATE_LUSTRE_COPY_ERROR" :	     \
	s == STATE_LUSTRE_COPY_DONE  ? "STATE_LUSTRE_COPY_DONE"  :	     \
	s == STATE_TSM_ARCHIVE_RUN   ? "STATE_TSM_ARCHIVE_RUN"   :	     \
	s == STATE_TSM_ARCHIVE_ERROR ? "STATE_TSM_ARCHIVE_ERROR" :           \
	s == STATE_TSM_ARCHIVE_DONE  ? "STATE_TSM_ARCHIVE_DONE"  :           \
	s == STATE_FILE_OMITTED      ? "STATE_FILE_OMITTED"      :           \
	s == STATE_FILE_KEEP         ? "STATE_FILE_KEEP"         : "UNKNOWN" \

#define FSQ_STORAGE_DEST_STR(s)						   \
	s == FSQ_STORAGE_LOCAL      ? "FSQ_STORAGE_LOCAL"      :	   \
	s == FSQ_STORAGE_LUSTRE     ? "FSQ_STORAGE_LUSTRE"     :	   \
	s == FSQ_STORAGE_LUSTRE_TSM ? "FSQ_STORAGE_LUSTRE_TSM" : 	   \
        s == FSQ_STORAGE_TSM        ? "FSQ_STORAGE_TSM"        :           \
        s == FSQ_STORAGE_NULL       ? "FSQ_STORAGE_NULL"       : "UNKNOWN"

#define FSQ_ERROR(fsq_session, rc, ...)				\
do {								\
	fsq_session.fsq_packet.fsq_error.rc = rc;		\
	snprintf(fsq_session.fsq_packet.fsq_error.strerror,	\
		 FSQ_MAX_ERRMSG_LENGTH, __VA_ARGS__);		\
	CT_ERROR(rc, __VA_ARGS__);				\
} while (0);

enum fsq_action_state_t {
	STATE_LOCAL_COPY_DONE	= 0x1,
	STATE_LUSTRE_COPY_RUN	= 0x2,
	STATE_LUSTRE_COPY_ERROR = 0x4,
	STATE_LUSTRE_COPY_DONE  = 0x8,
	STATE_TSM_ARCHIVE_RUN	= 0x10,
	STATE_TSM_ARCHIVE_ERROR	= 0x20,
	STATE_TSM_ARCHIVE_DONE	= 0x40,
	STATE_FILE_OMITTED      = 0x80,
	STATE_FILE_KEEP         = 0x100
};

enum fsq_protocol_state_t {
	FSQ_CONNECT    = 0x1,
	FSQ_OPEN       = 0x2,
	FSQ_DATA       = 0x4,
	FSQ_CLOSE      = 0x8,
	FSQ_DISCONNECT = 0x10,
	FSQ_REPLY      = 0x20,
	FSQ_ERROR      = 0x40
};

enum fsq_storage_dest_t {
        FSQ_STORAGE_NULL       = 0,
	FSQ_STORAGE_LOCAL      = 1,
	FSQ_STORAGE_LUSTRE     = 2,
	FSQ_STORAGE_TSM	       = 3,
	FSQ_STORAGE_LUSTRE_TSM = 4
};

struct fsq_login_t {
	char node[DSM_MAX_NODE_LENGTH + 1];
	char password[DSM_MAX_VERIFIER_LENGTH + 1];
	char hostname[HOST_NAME_MAX + 1];
	int port;
};

struct fsq_info_t {
	char fs[DSM_MAX_FSNAME_LENGTH + 1];
	char fpath[PATH_MAX + 1];
	char desc[DSM_MAX_DESCR_LENGTH + 1];
	enum fsq_storage_dest_t fsq_storage_dest;
};

struct fsq_data_t {
	size_t size;
};

struct fsq_error_t {
	int rc;
	char strerror[FSQ_MAX_ERRMSG_LENGTH + 1];
};

struct fsq_packet_t {
	uint8_t ver;
	struct fsq_error_t fsq_error;
	enum fsq_protocol_state_t state;
	union {
		struct fsq_login_t fsq_login;
		struct fsq_info_t fsq_info;
		struct fsq_data_t fsq_data;
	};
};

struct fsq_session_t {
	struct fsq_packet_t fsq_packet;
	int fd;
};

struct fsq_action_item_t {
	uint32_t fsq_action_state;
	struct fsq_info_t fsq_info;
	char fpath_local[PATH_MAX + 1];
	size_t size;
	size_t progress_size;
	double ts[4];
	size_t action_error_cnt;
	int archive_id;
	uid_t uid;
	gid_t gid;
} __attribute__ ((packed));

int fsq_send(struct fsq_session_t *fsq_session,
	     const enum fsq_protocol_state_t fsq_protocol_state);
int fsq_recv(struct fsq_session_t *fsq_session,
	     enum fsq_protocol_state_t fsq_protocol_state);

int fsq_init(struct fsq_login_t *fsq_login,
	     const char *node, const char *password, const char *hostname);
int fsq_fconnect(struct fsq_login_t *fsq_login,
		 struct fsq_session_t *fsq_session);
void fsq_fdisconnect(struct fsq_session_t *fsq_session);
int fsq_fopen(const char *fs, const char *fpath, const char *desc,
	      struct fsq_session_t *fsq_session);
int fsq_fdopen(const char *fs, const char *fpath, const char *desc,
	       enum fsq_storage_dest_t fsq_storage_dest,
	       struct fsq_session_t *fsq_session);
ssize_t fsq_fwrite(const void *ptr, size_t size, size_t nmemb,
		   struct fsq_session_t *fsq_session);
int fsq_fclose(struct fsq_session_t *fsq_session);

#endif /* FSQAPI_H */
