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
 * Copyright (c) 2019 GSI Helmholtz Centre for Heavy Ion Research
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <lustre/lustreapi.h>
#include "tsmapi.h"

#define PORT_DEFAULT_FSD	7625
#define N_THREADS_DEFAULT	4
#define N_THREADS_MAX		1024
#define BACKLOG			32

struct ident_map_t {
	char node[DSM_MAX_NODE_LENGTH + 1];
	uint16_t archive_id;
	uid_t uid;
	gid_t gid;
};

struct options {
	char *o_mnt_lustre;
	char o_local_mount[PATH_MAX + 1];
	char o_file_ident[PATH_MAX + 1];
	int o_port;
	int o_nthreads;
	int o_daemonize;
	int o_verbose;
};

static struct options opt = {
	.o_local_mount = {0},
	.o_file_ident  = {0},
	.o_port	       = PORT_DEFAULT_FSD,
	.o_nthreads    = N_THREADS_DEFAULT,
	.o_daemonize   = 0,
	.o_verbose     = API_MSG_NORMAL
};

static list_t ident_list;
static uint16_t thread_cnt = 0;
static pthread_mutex_t cnt_mutex = PTHREAD_MUTEX_INITIALIZER;

static void usage(const char *cmd_name, const int rc)
{
	fprintf(stdout, "usage: %s [options] <lustre_mount_point>\n"
		"\t-l, --localfs <string>\n"
		"\t\t""mount point of local file system\n"
		"\t-i, --identmap <file>\n"
		"\t\t""filename of identifier mapping\n"
		"\t-p, --port <int>\n"
		"\t\t""port accepting connections [default: %d]\n"
		"\t-t, --threads <int>\n"
		"\t\t""number of processing threads [default: %d]\n"
		"\t--daemon\n"
		"\t\t""daemon mode run in background\n"
		"\t-v, --verbose {error, warn, message, info, debug}"
		" [default: message]\n"
		"\t\t""produce more verbose output\n"
		"\t-h, --help\n"
		"\t\t""show this help\n"
		"version: %s © 2019 by GSI Helmholtz Centre for Heavy Ion Research\n",
		cmd_name,
		PORT_DEFAULT_FSD,
		N_THREADS_DEFAULT,
		PACKAGE_VERSION);
	exit(rc);
}

static void print_ident(void *data)
{
	struct ident_map_t *ident_map =
		(struct ident_map_t *)data;
	CT_INFO("node: '%s', archive_id: %d, uid: %lu, gid: %lu",
		ident_map->node, ident_map->archive_id,
		ident_map->uid, ident_map->gid);
}

static int parse_valid_num(const char *str, long int *val)
{
	char *end = NULL;

	*val = strtol(str, &end, 10);
	if (*end != '\0' || *val < 0)
		return -ERANGE;

	return 0;
}

static int parse_line_ident(char *line, struct ident_map_t *ident_map)
{
	const char *delim = " \t\r\n";
	char *token;
	uint16_t cnt_token = 0;
	int rc;
	long int val;

	if (!ident_map || !line)
		return -EINVAL;

	/* Parse node name. */
	token = strtok(line, delim);
	strncpy(ident_map->node, token, 16);
	cnt_token++;
	token = strtok(NULL, delim);
	/* Parse archive ID. */
	if (token && cnt_token++) {
		rc = parse_valid_num(token, &val);
		if (rc || val > UINT16_MAX)
			return -EINVAL;
		ident_map->archive_id = (uint16_t)val;
	}
	/* Parse uid. */
	token = strtok(NULL, delim);
	if (token && cnt_token++) {
		rc = parse_valid_num(token, &val);
		if (rc || val > UINT32_MAX)
			return -EINVAL;
		ident_map->uid = (uid_t)val;
	}
	/* Parse gid. */
	token = strtok(NULL, delim);
	if (token && cnt_token++) {
		rc = parse_valid_num(token, &val);
		if (rc || val > UINT32_MAX)
			return -EINVAL;
		ident_map->gid = (gid_t)val;
	}
	/* Final verification. */
	token = strtok(NULL, delim);
	if (token || cnt_token != 4)
		return -EINVAL;

	return 0;
}

static int parse_file_ident(const char *filename)
{
	FILE *file = NULL;
	char *line = NULL;
	size_t len = 0;
	ssize_t nread;
	struct ident_map_t ident_map;
	int rc = 0;
	uint16_t n = 0;

	/* Syntax: node archive_id uid gid */
	file = fopen(filename, "r");
	if (!file) {
		rc = -errno;
		CT_ERROR(rc, "fopen");
		return rc;
	}

	memset(&ident_map, 0, sizeof(struct ident_map_t));
	errno = 0;
	while ((nread = getline(&line, &len, file)) != -1) {

		if (line && line[0] == '#')
			continue;

		if (parse_line_ident(line, &ident_map))
			/* Skip malformed lines and output warning. */
			CT_WARN("ignoring settings in line %d file '%s'",
				++n, filename);
		else {
			struct ident_map_t *_ident_map;
			_ident_map = calloc(1, sizeof(struct ident_map_t));
			if (!_ident_map) {
				rc = -errno;
				CT_ERROR(rc, "calloc");
				goto out;
			}
			memcpy(_ident_map, &ident_map, sizeof(struct ident_map_t));
			rc = list_ins_next(&ident_list, NULL, _ident_map);
			if (rc) {
				rc = -EPERM;
				CT_ERROR(rc, "list_ins_next");
				goto out;
			}
		}
	};
	if (errno) {
		rc = -errno;
		CT_ERROR(rc, "getline");
	}

out:
	if (line)
		free(line);

	if (file)
		fclose(file);

	return rc;
}

static void sanity_arg_check(const struct options *opts, const char *argv)
{
	if (!opt.o_local_mount[0]) {
		fprintf(stdout, "missing argument -l, --localfs <string>\n\n");
		usage(argv, 1);
	}
	if (!opt.o_file_ident[0]) {
		fprintf(stdout, "missing argument -i, --identmap <file>\n\n");
		usage(argv, 1);
	}
	if (opt.o_nthreads > N_THREADS_MAX) {
		fprintf(stdout, "maximum number of threads %d exceeded\n\n",
			N_THREADS_MAX);
		usage(argv, 1);
	}
}

static int parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{"localfs",	required_argument, 0,		     'l'},
		{"identmap",	required_argument, 0,		     'i'},
		{"port",	required_argument, 0,		     'p'},
		{"threads",	required_argument, 0,		     't'},
		{"daemon",	no_argument,	   &opt.o_daemonize,   1},
		{"verbose",	required_argument, 0,		     'v'},
		{"help",	no_argument,	   0,		     'h'},
		{0, 0, 0, 0}
	};

	int c, rc;
	optind = 0;

	while ((c = getopt_long(argc, argv, "l:i:p:t:v:h",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'l': {
			strncpy(opt.o_local_mount, optarg, PATH_MAX);
			break;
		}
		case 'i': {
			strncpy(opt.o_file_ident, optarg, PATH_MAX);
			break;
		}
		case 'p': {
			long int p;
			rc = parse_valid_num(optarg, &p);
			if (rc)
				return rc;
			opt.o_port = (int)p;
			break;
		}
		case 't': {
			long int t;
			rc = parse_valid_num(optarg, &t);
			if (rc)
				return rc;
			opt.o_nthreads = (int)t;
			break;
		}
		case 'v': {
			if (OPTNCMP("error", optarg))
				opt.o_verbose = API_MSG_ERROR;
			else if (OPTNCMP("warn", optarg))
				opt.o_verbose = API_MSG_WARN;
			else if (OPTNCMP("message", optarg))
				opt.o_verbose = API_MSG_NORMAL;
			else if (OPTNCMP("info", optarg))
				opt.o_verbose = API_MSG_INFO;
			else if (OPTNCMP("debug", optarg))
				opt.o_verbose = API_MSG_DEBUG;
			else {
				fprintf(stdout, "wrong argument for -v, "
					"--verbose='%s'\n", optarg);
				usage(argv[0], 1);
			}
			api_msg_set_level(opt.o_verbose);
			break;
		}
		case 'h': {
			usage(argv[0], 0);
			break;
		}
		case 0: {
			break;
		}
		default:
			return -EINVAL;
		}
	}

	sanity_arg_check(&opt, argv[0]);

	if (argc != optind + 1) {
		rc = -EINVAL;
		CT_ERROR(rc, "no Lustre mount point specified");
		return rc;
	}
	opt.o_mnt_lustre = argv[optind];

	list_init(&ident_list, free);
	rc = parse_file_ident(opt.o_file_ident);
	if (!rc && opt.o_verbose >= API_MSG_INFO)
		list_for_each(&ident_list, print_ident);

	return rc;
}

static int verify_node(const struct login_t *login)
{
	int rc = 0;

	/* Check whether received node name is in listed identifier map. */
	list_node_t *node = list_head(&ident_list);
	struct ident_map_t *ident_map;
	bool found = false;
	while (node) {
		ident_map = (struct ident_map_t *)list_data(node);
		if (!strncmp(login->node, ident_map->node, DSM_MAX_NODE_LENGTH)) {
			found = true;
			break;
		}
		node = list_next(node);
	}
	if (!found) {
		CT_ERROR(0, "identifier mapping for node '%s' not found",
			 login->node);
		rc = -EPERM;
		goto out;
	}
	CT_INFO("found node '%s' in identmap, using archive_id %d, "
		"uid: %d, guid: %d", node->data,
		ident_map->archive_id, ident_map->uid, ident_map->gid);

out:
	return rc;
}

static int recv_fsd_protocol(int fd, struct fsd_protocol_t *fsd_protocol,
			     enum fsd_protocol_state_t fsd_protocol_state)
{
	int rc = 0;
	ssize_t bytes_recv;

	if (fd < 0 || !fsd_protocol)
		return -EINVAL;

	bytes_recv = read_size(fd, fsd_protocol, sizeof(struct fsd_protocol_t));
	CT_DEBUG("[fd=%d] protocol size: %zd, expected size: %zd, state: '%s', expected: '%s'",
		 fd, bytes_recv, sizeof(struct fsd_protocol_t),
		 FSD_PROTOCOL_STR(fsd_protocol->state),
		 FSD_PROTOCOL_STR(fsd_protocol_state));
	if (bytes_recv < 0) {
		rc = -errno;
		CT_ERROR(rc, "read_size");
		goto out;
	}
	if (bytes_recv != sizeof(struct fsd_protocol_t)) {
		rc = -ENOMSG;
		CT_ERROR(rc, "read_size");
		goto out;
	}
	CT_INFO("[fd=%d] recv_fsd_protocol state: '%s', expected: '%s'",
		fd,
		FSD_PROTOCOL_STR(fsd_protocol->state),
		FSD_PROTOCOL_STR(fsd_protocol_state));

	if (!(fsd_protocol->state & fsd_protocol_state))
		rc = -EPROTO;

out:
	return rc;
}

static void *thread_handle_client(void *arg)
{
	int			rc, *fd;
	struct fsd_protocol_t	fsd_protocol;
	char fpath_local[PATH_MAX]	 = {0};
	int			fd_local = -1;

	fd = (int *)arg;
	memset(&fsd_protocol, 0, sizeof(struct fsd_protocol_t));

	/* State 1: Client calls fsd_tsm_fconnect(...). */
	rc = recv_fsd_protocol(*fd, &fsd_protocol, FSD_CONNECT);
	CT_DEBUG("[rc=%d,fd=%d] recv_fsd_protocol", rc, *fd);
	if (rc) {
		CT_ERROR(rc, "recv_fsd_protocol failed");
		goto out;
	}

	rc = verify_node(&fsd_protocol.login);
	CT_DEBUG("[rc=%d] verify_node", rc);
	if (rc) {
		CT_ERROR(rc, "verify_node");
		goto out;
	}

	do {
		/* State 2: Client calls fsd_tsm_fopen(...) or fsd_tsm_disconnect(...). */
		rc = recv_fsd_protocol(*fd, &fsd_protocol, (FSD_OPEN | FSD_DISCONNECT));
		CT_DEBUG("[rc=%d,fd=%d] recv_fsd_protocol", rc, *fd);
		if (rc) {
			CT_ERROR(rc, "recv_fsd_protocol failed");
			goto out;
		}

		if (fsd_protocol.state & FSD_DISCONNECT)
			goto out;

		CT_INFO("[fd=%d] receiving buffer from node: '%s' with fpath: '%s'",
			*fd, fsd_protocol.login.node, fsd_protocol.fsd_info.fpath);

		char hl[DSM_MAX_HL_LENGTH + 1] = {0};
		char ll[DSM_MAX_LL_LENGTH + 1] = {0};
		rc = extract_hl_ll(fsd_protocol.fsd_info.fpath, fsd_protocol.fsd_info.fs,
				   hl, ll);
		if (rc) {
			rc = -EFAILED;
			CT_ERROR(rc, "extract_hl_ll");
			goto out;
		}

		snprintf(fpath_local, PATH_MAX - strlen(opt.o_local_mount),
			 "%s/%s", opt.o_local_mount, hl);
		/* Make sure the directory exists where to store the file. */
		rc = mkdir_p(fpath_local, S_IRWXU | S_IRGRP | S_IXGRP
			     | S_IROTH | S_IXOTH);
		CT_DEBUG("[rc=%d] mkdir_p '%s'", rc, fpath_local);
		if (rc) {
			CT_ERROR(rc, "mkdir_p '%s'", fpath_local);
			goto out;
		}

		snprintf(fpath_local + strlen(fpath_local), PATH_MAX, "/%s", ll);

		fd_local = open(fpath_local, O_WRONLY | O_TRUNC | O_CREAT,
				S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
		CT_DEBUG("[fd=%d] open '%s'", fd_local, fpath_local);
		if (fd_local < 0) {
			rc = -errno;
			CT_ERROR(rc, "open '%s'", fpath_local);
			goto out;
		}

		/* State 3: Client calls (multiple times) fsd_tsm_write(...).
		   Receive data buffer. */
		rc = recv_fsd_protocol(*fd, &fsd_protocol, FSD_DATA);
		CT_DEBUG("[rc=%d,fd=%d] recv_fsd_protocol, size: %zu", rc, *fd,
			 fsd_protocol.size);
		if (rc) {
			CT_ERROR(rc, "recv_fsd_protocol failed");
			goto out;
		}

		/* TODO: Make here some sanity on fsd_protocol.size. */

		uint8_t buf[0xfffff] = {0}; /* 1MiB. */
		ssize_t bytes_recv, bytes_to_recv, bytes_recv_total = 0;
		ssize_t bytes_send, bytes_send_total = 0;

		do {
			bytes_to_recv	      = fsd_protocol.size < sizeof(buf) ?
				fsd_protocol.size : sizeof(buf);
			if (fsd_protocol.size - bytes_recv_total < bytes_to_recv)
				bytes_to_recv = fsd_protocol.size - bytes_recv_total;

			bytes_recv = read_size(*fd, buf, bytes_to_recv);
			bytes_recv_total += bytes_recv;
			CT_DEBUG("[fd=%d] read_size %zd, max expected %zd, total recv size: %zd",
				 *fd, bytes_recv, sizeof(buf), bytes_recv_total);
			if (bytes_recv < 0) {
				CT_ERROR(errno, "recv");
				goto out;
			}
			if (bytes_recv >= 0)
				CT_INFO("bytes_recv: %zu, bytes_recv_total: %zu",
					bytes_recv, bytes_recv_total);
			bytes_send = write_size(fd_local, buf, bytes_recv);
			bytes_send_total += bytes_send;
			CT_DEBUG("[fd=%d] write_size %zd, max expected %zd, total recv size: %zd",
				 fd_local, bytes_send, sizeof(buf), bytes_send_total);
			if (bytes_send < 0) {
				CT_ERROR(errno, "send");
				goto out;
			}
		} while (bytes_recv_total != fsd_protocol.size);

		if (bytes_recv_total != bytes_send_total)
			CT_WARN("total number of bytes recv and send differs");
		else
			CT_DEBUG("[fd=%d,fd_local=%d] data buffer size %zd successfully "
				 "received and sent", *fd, fd_local, bytes_recv_total);

		/* State 4: Client calls fsd_tsm_fclose(...). */
		rc = recv_fsd_protocol(*fd, &fsd_protocol, FSD_CLOSE);
		CT_DEBUG("[rc=%d,fd=%d] recv_fsd_protocol", rc, *fd);
		if (rc) {
			CT_ERROR(rc, "recv_fsd_protocol failed");
			goto out;
		}

		rc = close(fd_local);
		if (rc < 0) {
			rc = -errno;
			CT_ERROR(rc, "close");
			goto out;
		}
		fd_local = -1;
	} while (fsd_protocol.state != FSD_DISCONNECT);

out:
	if (!(fd_local < 0))
		close(fd_local);

	pthread_mutex_lock(&cnt_mutex);
	if (thread_cnt > 0)
		thread_cnt--;
	pthread_mutex_unlock(&cnt_mutex);

	return NULL;
}

static int fsd_setup(void)
{
	int rc;
	struct stat st_buf;

	/* Verify we have a valid local mount point. */
	memset(&st_buf, 0, sizeof(st_buf));
	rc = stat(opt.o_local_mount, &st_buf);
	if (rc < 0) {
		CT_ERROR(errno, "stat '%s'", opt.o_local_mount);
		return rc;
	}
	if (!S_ISDIR(st_buf.st_mode)) {
		rc = -ENOTDIR;
		CT_ERROR(rc, "'%s'", opt.o_local_mount);
		return rc;
	}
#if 0   /* TODO: Deactivated for testing. */
	/* Verify we have a valid Lustre mount point. */
	rc = llapi_search_fsname(opt.o_mnt_lustre, opt.o_mnt_lustre);
	if (rc < 0) {
		CT_ERROR(rc, "cannot find a Lustre filesystem mounted at '%s'",
			 opt.o_mnt_lustre);
	}
#endif

	return rc;
}

int main(int argc, char *argv[])
{
	int rc;
	int sock_fd = -1;
	struct sockaddr_in sockaddr_srv;
	pthread_t *threads = NULL;
	pthread_attr_t attr;
	char thread_name[16] = {0};
	int reuse = 1;

	rc = parseopts(argc, argv);
	if (rc < 0) {
		CT_WARN("try '%s --help' for more information", argv[0]);
		return -rc;	/* Return positive error codes back to shell. */
	}

	rc = fsd_setup();
	if (rc < 0)
		return rc;

	memset(&sockaddr_srv, 0, sizeof(sockaddr_srv));
	sockaddr_srv.sin_family = AF_INET;
	sockaddr_srv.sin_addr.s_addr = htonl(INADDR_ANY);
	sockaddr_srv.sin_port = htons(opt.o_port);

	sock_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (sock_fd < 0) {
		rc = errno;
		CT_ERROR(rc, "socket");
		goto cleanup;
	}
	rc = setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
			sizeof(reuse));
	if (rc < 0) {
		rc = errno;
		CT_ERROR(rc, "setsockopt");
		goto cleanup;
	}

	rc = bind(sock_fd, (struct sockaddr *)&sockaddr_srv,
		  sizeof(sockaddr_srv));
	if (rc < 0) {
		rc = errno;
		CT_ERROR(rc, "bind");
		goto cleanup;
	}

	rc = listen(sock_fd, BACKLOG);
	if (rc < 0) {
		rc = errno;
		CT_ERROR(rc, "listen");
		goto cleanup;
	}
	CT_MESSAGE("listening on port: %d with %d serving threads",
		   opt.o_port, opt.o_nthreads);

	threads = calloc(opt.o_nthreads, sizeof(pthread_t));
	if (threads == NULL) {
		rc = errno;
		CT_ERROR(rc, "calloc");
		goto cleanup;
	}

	rc = pthread_attr_init(&attr);
	if (rc != 0) {
		CT_ERROR(rc, "pthread_attr_init");
		goto cleanup;
	}
	rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	if (rc != 0) {
		CT_ERROR(rc, "pthread_attr_setdetachstate");
		goto cleanup_attr;
	}

	while (1) {
		/* TODO: Make sure fd is closed. */
		int fd;
		struct sockaddr_in sockaddr_cli;
		socklen_t addrlen;

		memset(&sockaddr_cli, 0, sizeof(sockaddr_cli));
		addrlen = sizeof(sockaddr_cli);
		fd = accept(sock_fd, (struct sockaddr *)&sockaddr_cli,
			    &addrlen);
		if (fd < 0)
			CT_ERROR(errno, "accept");
		else {
			if (thread_cnt >= opt.o_nthreads) {
				CT_WARN("maximum number of serving "
					"threads %d exceeded",
					opt.o_nthreads);
				continue;
			}
			pthread_mutex_lock(&cnt_mutex);
			rc = pthread_create(&threads[thread_cnt], NULL,
					    thread_handle_client,
					    (void *)&fd);
			if (rc != 0)
				CT_ERROR(rc, "cannot create thread for "
					 "client '%s'",
					 inet_ntoa(sockaddr_cli.sin_addr));
			else {
				snprintf(thread_name, sizeof(thread_name),
					 "ltsmfsd/%d", thread_cnt);
				pthread_setname_np(threads[thread_cnt],
						   thread_name);
				CT_MESSAGE("created thread '%s' for client '%s'",
					   thread_name,
					   inet_ntoa(sockaddr_cli.sin_addr));
				thread_cnt++;
			}
			pthread_mutex_unlock(&cnt_mutex);
		}
	}

cleanup_attr:
	pthread_attr_destroy(&attr);

cleanup:
	if (threads)
		free(threads);

	if (sock_fd > -1)
		close(sock_fd);

	list_destroy(&ident_list);

	return rc;
}