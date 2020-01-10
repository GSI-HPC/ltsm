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
 * Copyright (c) 2019, GSI Helmholtz Centre for Heavy Ion Research
 */

#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "fsdapi.h"
#include "log.h"
#include "common.h"

int fsd_send(struct fsd_session_t *fsd_session,
	     const enum fsd_protocol_state_t protocol_state)
{
	int rc = 0;
	ssize_t bytes_send;

	fsd_session->state = protocol_state;
	bytes_send = write_size(fsd_session->sock_fd, fsd_session,
				sizeof(struct fsd_session_t));
	CT_DEBUG("[fd=%d] fsd_send %zd, expected size: %zd, state: '%s'",
		 fsd_session->sock_fd,
		 bytes_send, sizeof(struct fsd_session_t),
		 FSD_PROTOCOL_STR(fsd_session->state));
	if (bytes_send < 0) {
		rc = -errno;
		CT_ERROR(rc, "write_size");
		goto out;
	}
	if ((size_t)bytes_send != sizeof(struct fsd_session_t)) {
		rc = -ENOMSG;
		CT_ERROR(rc, "write_size");
	}

out:
	return rc;
}


int fsd_recv(int fd, struct fsd_session_t *fsd_session,
	     enum fsd_protocol_state_t fsd_protocol_state)
{
	int rc = 0;
	ssize_t bytes_recv;

	if (fd < 0 || !fsd_session)
		return -EINVAL;

	bytes_recv = read_size(fd, fsd_session, sizeof(struct fsd_session_t));
	CT_DEBUG("[fd=%d] fsd_recv %zd, expected size: %zd, "
		 "state: '%s', expected: '%s'",
		 fd, bytes_recv, sizeof(struct fsd_session_t),
		 FSD_PROTOCOL_STR(fsd_session->state),
		 FSD_PROTOCOL_STR(fsd_protocol_state));
	if (bytes_recv < 0) {
		rc = -errno;
		CT_ERROR(rc, "read_size");
		goto out;
	}
	if (bytes_recv != sizeof(struct fsd_session_t)) {
		rc = -ENOMSG;
		CT_ERROR(rc, "read_size");
		goto out;
	}
	if (!(fsd_session->state & fsd_protocol_state))
		rc = -EPROTO;

out:
	return rc;
}

int fsd_fconnect(struct fsd_login_t *fsd_login, struct fsd_session_t *fsd_session)
{
	int rc;
        struct sockaddr_in sockaddr_cli;
	struct hostent *hostent;

	hostent = gethostbyname(fsd_login->hostname);
	if (!hostent) {
		rc = -h_errno;
		CT_ERROR(rc, "%s", hstrerror(h_errno));
		goto out;
	}

        /* Connect to file system daemon (fsd). */
        fsd_session->sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fsd_session->sock_fd < 0) {
                rc = -errno;
                CT_ERROR(rc, "socket");
                goto out;
        }

        memset(&sockaddr_cli, 0, sizeof(sockaddr_cli));
        sockaddr_cli.sin_family = AF_INET;
        sockaddr_cli.sin_addr = *((struct in_addr *)hostent->h_addr);
        sockaddr_cli.sin_port = htons(fsd_login->port);

	CT_INFO("connecting to '%s:%d'", fsd_login->hostname, fsd_login->port);
        rc = connect(fsd_session->sock_fd,
		     (struct sockaddr *)&sockaddr_cli,
                     sizeof(sockaddr_cli));
        if (rc < 0) {
                rc = errno;
                CT_ERROR(rc, "connect");
                goto out;
        }

	memcpy(&(fsd_session->fsd_login), fsd_login, sizeof(struct fsd_login_t));
	rc = fsd_send(fsd_session, FSD_CONNECT);

out:
        if (rc)
                close(fsd_session->sock_fd);

        return rc;
}

void fsd_fdisconnect(struct fsd_session_t *fsd_session)
{
	fsd_send(fsd_session, FSD_DISCONNECT);
	close(fsd_session->sock_fd);
}

int fsd_fopen(const char *fs, const char *fpath, const char *desc,
	      struct fsd_session_t *fsd_session)
{
	int rc = 0;
	struct fsd_info_t fsd_info = {
		.fs		   = {0},
		.fpath		   = {0},
		.desc		   = {0}
	};

	if (!fsd_session)
		return -EFAULT;

	if (!(fs && fpath)) {
		close(fsd_session->sock_fd);

		return -EFAULT;
	}

	/* Fillup struct fsd_info_t with fs, fpath, desc. */
	strncpy(fsd_info.fs, fs, DSM_MAX_FSNAME_LENGTH);
	strncpy(fsd_info.fpath, fpath, PATH_MAX);
	if (desc)
		strncpy(fsd_info.desc, desc, DSM_MAX_DESCR_LENGTH);
	memcpy(&(fsd_session->fsd_info), &fsd_info, sizeof(fsd_info));

	rc = fsd_send(fsd_session, FSD_OPEN);
	if (rc)
		close(fsd_session->sock_fd);

	return rc;
}

ssize_t fsd_fwrite(const void *ptr, size_t size, size_t nmemb,
		   struct fsd_session_t *fsd_session)
{
	int rc;
	ssize_t bytes_written;

	fsd_session->size = size * nmemb;

	rc = fsd_send(fsd_session, FSD_DATA);
	if (rc) {
		close(fsd_session->sock_fd);
		return rc;
	}

	bytes_written = write_size(fsd_session->sock_fd, ptr,
				   fsd_session->size);
	CT_DEBUG("[fd=%d] write size %zd, expected size %zd",
		 fsd_session->sock_fd, bytes_written, fsd_session->size);

	return bytes_written;
}

int fsd_fclose(struct fsd_session_t *fsd_session)
{
	int rc = 0;

	rc = fsd_send(fsd_session, FSD_CLOSE);
	if (rc)
		close(fsd_session->sock_fd);

	memset(&fsd_session->fsd_info, 0, sizeof(struct fsd_info_t));

	return rc;
}
