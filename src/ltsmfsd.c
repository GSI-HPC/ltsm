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

#include <getopt.h>
#include <pthread.h>
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

static void *thread_handle_io(void *arg)
{
	int *fd;
	ssize_t bytes_recv, bytes_recv_total;
	struct login_t login;
	struct fsd_info_t fsd_info;

	bytes_recv_total = 0;
	fd = (int *)arg;
	CT_INFO("thread serving fd: %d", *fd);

	/* 1.) receive struct login_t. */
	memset(&login, 0, sizeof(struct login_t));
	bytes_recv = read_size(*fd, &login, sizeof(login));
	CT_DEBUG("[fd=%d] bytes_recv: %zd, expected: %zd", *fd, bytes_recv,
		 sizeof(struct login_t));
	if (bytes_recv < 0) {
		CT_ERROR(errno, "recv");
		goto out;
	}
	if (bytes_recv != sizeof(struct login_t)) {
		CT_ERROR(-ENOMSG, "recv");
		goto out;
	}

	/* Check whether received node name is in listed identifier map. */
	list_node_t *node = list_head(&ident_list);
	struct ident_map_t *ident_map;
	bool found = false;
	while (node) {
		ident_map = (struct ident_map_t *)list_data(node);
		if (!strncmp(login.node, ident_map->node, DSM_MAX_NODE_LENGTH)) {
			found = true;
			break;
		}
		node = list_next(node);
	}
	if (!found) {
		CT_ERROR(0, "identifier mapping for node '%s' not found",
			 login.node);
		goto out;
	}
	CT_DEBUG("found node '%s' in identmap, using archive_id %d, "
		 "uid: %d, guid: %d", node->data,
		 ident_map->archive_id, ident_map->uid, ident_map->gid);

	/* 2.) receive struct fsd_info_t. */
	memset(&fsd_info, 0, sizeof(struct fsd_info_t));
	bytes_recv = read_size(*fd, &fsd_info, sizeof(fsd_info));
	CT_DEBUG("[fd=%d] bytes_recv: %zd, expected: %zd", *fd, bytes_recv,
		 sizeof(struct fsd_info_t));
	if (bytes_recv < 0) {
		CT_ERROR(errno, "recv");
		goto out;
	}
	if (bytes_recv != sizeof(struct fsd_info_t)) {
		CT_ERROR(-ENOMSG, "recv");
		goto out;
	}

	CT_INFO("thread serving fd: %d, fpath: '%s', node: '%s'",
		*fd, fsd_info.fpath, login.node);
	/* TODO: To be removed. */
	goto out;

	char buf[0xffff] = {0};
	while (1) {
		bytes_recv = recv(*fd, buf, sizeof(buf), 0);
		bytes_recv_total += bytes_recv;
		if (bytes_recv < 0) {
			CT_ERROR(errno, "recv");
			goto out;
		} else if (bytes_recv == 0) {
			CT_INFO("bytes total received: %zu", bytes_recv_total);
			goto out;
		} else {
			/* TODO: */
		}
	}

out:
	close(*fd);

	pthread_mutex_lock(&cnt_mutex);
	if (thread_cnt > 0)
		thread_cnt--;
	pthread_mutex_unlock(&cnt_mutex);

	return NULL;
}

/* static int fsd_setup(void) */
/* { */
/* 	int rc; */

/* 	rc = llapi_search_fsname(opt.o_local_mount, opt.o_local_mount); */
/* 	if (rc < 0) { */
/* 		CT_ERROR(rc, "cannot find a Lustre filesystem mounted at '%s'", */
/* 			 opt.o_local_mount); */
/* 	} */

/* 	return rc; */
/* } */

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

	/* rc = fsd_setup(); */
	/* if (rc < 0) */
	/* 	return rc; */

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
					    thread_handle_io,
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
