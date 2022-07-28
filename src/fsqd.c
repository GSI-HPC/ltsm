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
 * Copyright (c) 2019-2022 GSI Helmholtz Centre for Heavy Ion Research
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdbool.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <lustre/lustreapi.h>
#include "ltsmapi.h"
#include "fsqapi.h"
#include "xattr.h"
#include "queue.h"

#define N_THREADS_SOCK_DEFAULT	4
#define N_THREADS_SOCK_MAX	64
#define N_THREADS_QUEUE_DEFAULT	4
#define N_THREADS_QUEUE_MAX	64
#define N_TOL_FILE_ERRORS	16
#define BACKLOG			32
#define BUF_SIZE		0xfffff /* 0xfffff = 1MiB, 0x400000 = 4MiB */

struct ident_map_t {
	char node[DSM_MAX_NODE_LENGTH + 1];
	char servername[MAX_OPTIONS_LENGTH + 1];
	uint16_t archive_id;
	uid_t uid;
	gid_t gid;
};

struct options {
	char *o_mnt_lustre;
	char o_local_mount[PATH_MAX + 1];
	char o_file_ident[PATH_MAX + 1];
	int o_port;
	int o_nthreads_sock;
	int o_nthreads_queue;
	int o_ntol_file_errors;
	int o_verbose;
	char o_file_conf[PATH_MAX + 1];
};

static struct options opt   = {
	.o_local_mount	    = {0},
	.o_file_ident	    = {0},
	.o_port		    = FSQ_PORT_DEFAULT,
	.o_nthreads_sock    = N_THREADS_SOCK_DEFAULT,
	.o_nthreads_queue   = N_THREADS_QUEUE_DEFAULT,
	.o_ntol_file_errors = N_TOL_FILE_ERRORS,
	.o_verbose	    = API_MSG_NORMAL,
	.o_file_conf        = {0}
};

static list_t		ident_list;
/* Socket handling. */
static uint16_t		thread_sock_cnt = 0;
static pthread_mutex_t	mutex_sock_cnt	= PTHREAD_MUTEX_INITIALIZER;
static bool		keep_running	= true;

/* Work queue. */
static queue_t		 queue;
static pthread_t	*threads_queue = NULL;
static pthread_mutex_t	 queue_mutex   = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t	 queue_cond    = PTHREAD_COND_INITIALIZER;

/* TSM connect authentication. */
static pthread_mutex_t tsm_connect_mutex = PTHREAD_MUTEX_INITIALIZER;

static void usage(const char *cmd_name, const int rc)
{
	fprintf(stdout, "usage: %s [options] <lustre_mount_point>\n"
		"\t-l, --localfs <string>\n"
		"\t\t""mount point of local file system\n"
		"\t-i, --identmap <file>\n"
		"\t\t""filename of identifier mapping\n"
		"\t-p, --port <int>\n"
		"\t\t""port accepting connections [default: %d]\n"
		"\t-s, --sthreads <int>\n"
		"\t\t""number of socket threads [default: %d]\n"
		"\t-q, --qthreads <int>\n"
		"\t\t""number of queue worker threads [default: %d]\n"
		"\t-t, --tolerr <int>\n"
		"\t\t""number of tolerated file errors before file is "
		"omitted [default: %d]\n"
		"\t-c, --conf <file>\n"
		"\t\t""option conf file\n"
		"\t-v, --verbose {error, warn, message, info, debug}"
		" [default: %s]\n"
		"\t\t""produce more verbose output\n"
		"\t-h, --help\n"
		"\t\t""show this help\n"
		"version: %s, fsq protocol version: %s "
		"© 2022 by GSI Helmholtz Centre for Heavy Ion Research\n",
		cmd_name,
		FSQ_PORT_DEFAULT,
		N_THREADS_SOCK_DEFAULT,
		N_THREADS_QUEUE_DEFAULT,
		N_TOL_FILE_ERRORS,
		LOG_LEVEL_HUMAN_STR(opt.o_verbose),
		PACKAGE_VERSION,
		FSQ_PROTOCOL_VER_STR(FSQ_PROTOCOL_VER));

	exit(rc);
}

static int listen_socket_srv(int port)
{
	int rc;
	int sock_fd = -1;
	int reuse = 1;
	struct sockaddr_in sockaddr_srv;

	memset(&sockaddr_srv, 0, sizeof(sockaddr_srv));
	sockaddr_srv.sin_family	     = AF_INET;
	sockaddr_srv.sin_addr.s_addr = htonl(INADDR_ANY);
	sockaddr_srv.sin_port	     = htons(port);

	sock_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (sock_fd < 0) {
		rc = -errno;
		CT_ERROR(rc, "socket");
		goto cleanup;
	}
	rc = setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
			sizeof(reuse));
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "setsockopt");
		goto cleanup;
	}

	rc = bind(sock_fd, (struct sockaddr *)&sockaddr_srv,
		  sizeof(sockaddr_srv));
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "bind");
		goto cleanup;
	}

	rc = listen(sock_fd, BACKLOG);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "listen");
		goto cleanup;
	}

	return sock_fd;

cleanup:
	if (sock_fd > -1)
		close(sock_fd);

	return rc;
}

static void print_ident(void *data)
{
	struct ident_map_t *ident_map =
		(struct ident_map_t *)data;
	CT_INFO("node: '%s', servername: '%s', archive_id: %d, "
		"uid: %d, gid: %d",
		ident_map->node, ident_map->servername, ident_map->archive_id,
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
	char *saveptr;
	uint16_t cnt_token = 0;
	int rc;
	long int val;

	if (!ident_map || !line)
		return -EINVAL;

	/* Parse node name. */
	token = strtok_r(line, delim, &saveptr);
	if (token) {
		strncpy(ident_map->node, token, sizeof(ident_map->node) - 1);
		cnt_token++;
	}

	/* Parse servername. */
	token = strtok_r(NULL, delim, &saveptr);
	if (token && cnt_token++) {
		strncpy(ident_map->servername,
			token, sizeof(ident_map->servername) - 1);
	}
	/* Parse archive ID. */
	token = strtok_r(NULL, delim, &saveptr);
	if (token && cnt_token++) {
		rc = parse_valid_num(token, &val);
		if (rc || val > UINT16_MAX)
			return -EINVAL;
		ident_map->archive_id = (uint16_t)val;
	}
	/* Parse uid. */
	token = strtok_r(NULL, delim, &saveptr);
	if (token && cnt_token++) {
		rc = parse_valid_num(token, &val);
		if (rc || val > UINT32_MAX)
			return -EINVAL;
		ident_map->uid = (uid_t)val;
	}
	/* Parse gid. */
	token = strtok_r(NULL, delim, &saveptr);
	if (token && cnt_token++) {
		rc = parse_valid_num(token, &val);
		if (rc || val > UINT32_MAX)
			return -EINVAL;
		ident_map->gid = (gid_t)val;
	}
	/* Final verification. */
	token = strtok_r(NULL, delim, &saveptr);
	if (token || cnt_token != 5)
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

	/* Syntax: <node> <servername> <archive_id> <uid> <gid> */
	file = fopen(filename, "r");
	if (!file) {
		rc = -errno;
		CT_ERROR(rc, "fopen '%s'", filename);
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

static void read_conf(const char *filename)
{
	int rc;
	struct kv_opt kv_opt = {
		.N	     = 0,
		.kv	     = NULL
	};

	rc = parse_conf(filename, &kv_opt);
	if (!rc) {
		for (uint8_t n = 0; n < kv_opt.N; n++) {
			if (!strncmp("localfs", kv_opt.kv[n].key, PATH_MAX))
				strncpy(opt.o_local_mount, kv_opt.kv[n].val,
					PATH_MAX);
			else if (!strncmp("identmap", kv_opt.kv[n].key, PATH_MAX))
				strncpy(opt.o_file_ident, kv_opt.kv[n].val,
					PATH_MAX);
			else if (!strncmp("port", kv_opt.kv[n].key, MAX_OPTIONS_LENGTH)) {
				long int port;
				rc = parse_valid_num(kv_opt.kv[n].val, &port);
				if (rc)
					CT_WARN("wrong value '%s' for option '%s'"
						" in conf file '%s'",
						kv_opt.kv[n].val, kv_opt.kv[n].key,
						filename);
				else
					opt.o_port = (int)port;
			} else if (!strncmp("sthreads", kv_opt.kv[n].key, MAX_OPTIONS_LENGTH)) {
				long int sthreads;
				rc = parse_valid_num(kv_opt.kv[n].val, &sthreads);
				if (rc)
					CT_WARN("wrong value '%s' for option '%s'"
						" in conf file '%s'",
						kv_opt.kv[n].val, kv_opt.kv[n].key,
						filename);
				else
					opt.o_nthreads_sock = (int)sthreads;
			} else if (!strncmp("qthreads", kv_opt.kv[n].key, MAX_OPTIONS_LENGTH)) {
				long int qthreads;
				rc = parse_valid_num(kv_opt.kv[n].val, &qthreads);
				if (rc)
					CT_WARN("wrong value '%s' for option '%s'"
						" in conf file '%s'",
						kv_opt.kv[n].val, kv_opt.kv[n].key,
						filename);
				else
					opt.o_nthreads_queue = (int)qthreads;
			} else if (!strncmp("tolerr", kv_opt.kv[n].key, MAX_OPTIONS_LENGTH)) {
				long int tolerr;
				rc = parse_valid_num(kv_opt.kv[n].val, &tolerr);
				if (rc)
					CT_WARN("wrong value '%s' for option '%s'"
						" in conf file '%s'",
						kv_opt.kv[n].val, kv_opt.kv[n].key,
						filename);
				else
					opt.o_ntol_file_errors = (int)tolerr;
			} else if (!strncmp("verbose", kv_opt.kv[n].key, MAX_OPTIONS_LENGTH)) {
				rc = parse_verbose(kv_opt.kv[n].val,
						   &opt.o_verbose);
				if (rc)
					CT_WARN("wrong value '%s' for option '%s'"
						" in conf file '%s'",
						kv_opt.kv[n].val, kv_opt.kv[n].key,
						filename);
			} else
				CT_WARN("unknown option value '%s %s' in conf"
					" file '%s'", kv_opt.kv[n].key,
					kv_opt.kv[n].val, filename);
		}
	}

	if (kv_opt.kv) {
		free(kv_opt.kv);
		kv_opt.kv = NULL;
		kv_opt.N = 0;
	}
}

static void sanity_arg_check(const char *argv)
{
	if (!opt.o_local_mount[0]) {
		fprintf(stdout, "missing argument -l, --localfs <string>\n\n");
		usage(argv, 1);
	}
	if (!opt.o_file_ident[0]) {
		fprintf(stdout, "missing argument -i, --identmap <file>\n\n");
		usage(argv, 1);
	}
	if (opt.o_nthreads_sock > N_THREADS_SOCK_MAX) {
		fprintf(stdout, "maximum number of socket threads %d exceeded\n\n",
			N_THREADS_SOCK_MAX);
		usage(argv, 1);
	}
	if (opt.o_nthreads_queue > N_THREADS_QUEUE_MAX) {
		fprintf(stdout, "maximum number of queue worker threads %d exceeded\n\n",
			N_THREADS_QUEUE_MAX);
		usage(argv, 1);
	}
}

static int parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{.name = "localfs",  .has_arg = required_argument, .flag = NULL, .val = 'l'},
		{.name = "identmap", .has_arg = required_argument, .flag = NULL, .val = 'i'},
		{.name = "port",     .has_arg = required_argument, .flag = NULL, .val = 'p'},
		{.name = "sthreads", .has_arg = required_argument, .flag = NULL, .val = 's'},
		{.name = "qthreads", .has_arg = required_argument, .flag = NULL, .val = 'q'},
		{.name = "tolerr",   .has_arg = required_argument, .flag = NULL, .val = 't'},
		{.name = "conf",     .has_arg = required_argument, .flag = NULL, .val = 'c'},
		{.name = "verbose",  .has_arg = required_argument, .flag = NULL, .val = 'v'},
		{.name = "help",     .has_arg = no_argument,	   .flag = NULL, .val = 'h'},
		{.name = NULL}
	};

	int c, rc;
	optind = 0;

	while ((c = getopt_long(argc, argv, "l:i:p:s:q:t:c:v:h",
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
		case 's': {
			long int t;
			rc = parse_valid_num(optarg, &t);
			if (rc)
				return rc;
			opt.o_nthreads_sock = (int)t;
			break;
		}
		case 'q': {
			long int t;
			rc = parse_valid_num(optarg, &t);
			if (rc)
				return rc;
			opt.o_nthreads_queue = (int)t;
			break;
		}
		case 't': {
			long int t;
			rc = parse_valid_num(optarg, &t);
			if (rc)
				return rc;
			opt.o_ntol_file_errors = (int)t;
			break;
		}
		case 'c': {
			strncpy(opt.o_file_conf, optarg, PATH_MAX);
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

	if (opt.o_file_conf[0])
		read_conf(opt.o_file_conf);

	sanity_arg_check(argv[0]);

	if (argc != optind + 1) {
		rc = -EINVAL;
		CT_ERROR(rc, "no Lustre mount point specified");
		return rc;
	}
	opt.o_mnt_lustre = argv[optind];

	rc = parse_file_ident(opt.o_file_ident);
	if (!rc && opt.o_verbose >= API_MSG_INFO)
		list_for_each(&ident_list, print_ident);

	return rc;
}

static int identmap_entry(struct fsq_login_t *fsq_login,
			  char *servername,
			  int *archive_id,
			  uid_t *uid, gid_t *gid)
{
	int rc = 0;

	/* Check whether received node name is in listed identifier map. */
	list_node_t *node = list_head(&ident_list);
	struct ident_map_t *ident_map;
	bool found = false;

	while (node) {
		ident_map = (struct ident_map_t *)list_data(node);
		if (!strncmp(fsq_login->node, ident_map->node,
			     DSM_MAX_NODE_LENGTH)) {
			found = true;
			break;
		}
		node = list_next(node);
	}
	if (found) {
		CT_INFO("found node '%s' in identmap, using servername '%s', "
			"archive_id %d, uid %d, gid %d", node->data,
			ident_map->servername, ident_map->archive_id,
			ident_map->uid, ident_map->gid);
		strncpy(servername, ident_map->servername, sizeof(ident_map->servername) + 0);
		*archive_id = ident_map->archive_id;
		*uid = ident_map->uid;
		*gid = ident_map->gid;
	} else {
		CT_ERROR(0, "identifier mapping for node '%s' not found",
			 fsq_login->node);
		rc = -EACCES;
	}

	return rc;
}

static int enqueue_fsq_item(struct fsq_action_item_t *fsq_action_item)
{
	int rc;

	/* Lock queue to avoid thread access. */
	pthread_mutex_lock(&queue_mutex);

	rc = queue_enqueue(&queue, fsq_action_item);

	if (rc) {
		rc = -EFAILED;
		CT_ERROR(rc, "failed enqueue operation: "
			 "%p, state '%s', fs '%s', fpath '%s', size %zu, "
			 "errors %d, ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, ts[3] %.3f, queue size %lu",
			 fsq_action_item,
			 FSQ_ACTION_STR(fsq_action_item->fsq_action_state),
			 fsq_action_item->fsq_info.fs,
			 fsq_action_item->fsq_info.fpath,
			 fsq_action_item->size,
			 fsq_action_item->action_error_cnt,
			 fsq_action_item->ts[0],
			 fsq_action_item->ts[1],
			 fsq_action_item->ts[2],
			 fsq_action_item->ts[3],
			 queue_size(&queue));

		free(fsq_action_item);
	} else
		CT_INFO("enqueue operation: "
			"%p, state '%s', fs '%s', fpath '%s', size %zu, "
			"errors %d, ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, ts[3] %.3f, queue size %lu",
			fsq_action_item,
			FSQ_ACTION_STR(fsq_action_item->fsq_action_state),
			fsq_action_item->fsq_info.fs,
			fsq_action_item->fsq_info.fpath,
			fsq_action_item->size,
			fsq_action_item->action_error_cnt,
			fsq_action_item->ts[0],
			fsq_action_item->ts[1],
			fsq_action_item->ts[2],
			fsq_action_item->ts[3],
			queue_size(&queue));

	/* Free the lock of the queue. */
	pthread_mutex_unlock(&queue_mutex);

	/* Wakeup sleeping consumer (worker threads). */
	pthread_cond_signal(&queue_cond);

	return rc;
}

static struct fsq_action_item_t* create_fsq_item(const size_t bytes_recv_total,
						 const struct fsq_info_t *fsq_info,
						 const char *fpath_local, const int archive_id,
						 const uid_t uid, const gid_t gid, const double ts)
{
	struct fsq_action_item_t *fsq_action_item;

	fsq_action_item = calloc(1, sizeof(struct fsq_action_item_t));
	if (!fsq_action_item) {
		CT_ERROR(-errno, "calloc");
		return NULL;
	}

	fsq_action_item->fsq_action_state = STATE_LOCAL_COPY_DONE;
	fsq_action_item->size = bytes_recv_total;
	memcpy(&fsq_action_item->fsq_info, fsq_info, sizeof(struct fsq_info_t));
	fsq_action_item->ts[0] = ts;
	fsq_action_item->ts[1] = time_now();
	fsq_action_item->ts[2] = 0;
	fsq_action_item->ts[3] = 0;
	strncpy(fsq_action_item->fpath_local, fpath_local, PATH_MAX);
	fsq_action_item->archive_id = archive_id;
	fsq_action_item->uid = uid;
	fsq_action_item->gid = gid;

	return fsq_action_item;
}


static int init_fsq_dev_null(char *fpath_local, int *fd_local,
			     const struct fsq_session_t *fsd_session)
{
	int rc = 0;

	/* strlen("/dev/null") + '\0' = 9 + 1. */
	snprintf(fpath_local, 10, "%s", "/dev/null");

	*fd_local = open(fpath_local, O_WRONLY);
	CT_DEBUG("[fd=%d] open '%s'", *fd_local, fpath_local);
	if (*fd_local < 0) {
		rc = -errno;
		CT_ERROR(rc, "open '%s'", fpath_local);
	}

	return rc;
}

static int init_fsq_local(char *fpath_local, int *fd_local,
			  const struct fsq_session_t *fsq_session)
{
	int rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	rc = extract_hl_ll(fsq_session->fsq_packet.fsq_info.fpath,
			   fsq_session->fsq_packet.fsq_info.fs,
			   hl, ll);
	if (rc) {
		rc = -EFAILED;
		CT_ERROR(rc, "extract_hl_ll");
		return rc;
	}

	const size_t L = strlen(opt.o_local_mount) + strlen(hl) + strlen(ll) + 2;
	if (L > PATH_MAX) {
		rc = -ENAMETOOLONG;
		CT_ERROR(rc, "fpath name '%s/%s/%s'", opt.o_local_mount, hl, ll);
		return rc;
	}

	memset(fpath_local, 0, PATH_MAX + 1);
	strncpy(fpath_local, opt.o_local_mount, PATH_MAX);
	snprintf(fpath_local + strlen(fpath_local), PATH_MAX, "/%s", hl);

	/* Make sure the directory exists where to store the file. */
	rc = mkdir_p(fpath_local, S_IRWXU | S_IRGRP | S_IXGRP
		     | S_IROTH | S_IXOTH);
	CT_DEBUG("[rc=%d] mkdir_p '%s'", rc, fpath_local);
	if (rc) {
		CT_ERROR(rc, "mkdir_p '%s'", fpath_local);
		return rc;
	}

	snprintf(fpath_local + strlen(fpath_local), PATH_MAX, "/%s", ll);

	*fd_local = open(fpath_local, O_WRONLY | O_EXCL | O_CREAT,
			 S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
	/* Note, if file exists, then *fd_local < 0 and errno EEXISTS is
	   set. This makes sure we do NOT overwrite existing files. To
	   allow overwriting replace O_EXCL with O_TRUNC. */
	CT_DEBUG("[fd=%d] open '%s'", *fd_local, fpath_local);
	if (*fd_local < 0) {
		rc = -errno;
		CT_ERROR(rc, "open '%s'", fpath_local);
		return rc;
	}

	return rc;
}

static int init_fsq_storage(char *fpath_local, int *fd_local,
			    const struct fsq_session_t *fsq_session)
{
	if (fsq_session->fsq_packet.fsq_info.fsq_storage_dest == FSQ_STORAGE_TSM)
	{
		CT_ERROR(-ENOSYS, "storage destination '%s' not implemented",
			 FSQ_STORAGE_DEST_STR(FSQ_STORAGE_TSM));
		return -ENOSYS;
	}

	if (fsq_session->fsq_packet.fsq_info.fsq_storage_dest == FSQ_STORAGE_NULL)
		return init_fsq_dev_null(fpath_local, fd_local, fsq_session);
	else
		return init_fsq_local(fpath_local, fd_local, fsq_session);
}

static int fsq_recv_data(int *fd_local, struct fsq_session_t *fsq_session,
			 size_t *bytes_recv_total, size_t *bytes_send_total)
{
	int rc;
	uint8_t buf[BUF_SIZE];
	ssize_t bytes_recv, bytes_to_recv;
	ssize_t bytes_send;

	do {
		rc = fsq_recv(fsq_session, (FSQ_DATA | FSQ_CLOSE));
		CT_DEBUG("[rc=%d,fd=%d] fsq_recv state = '%s' size = %zu",
			 rc, fsq_session->fd,
			 FSQ_PROTOCOL_STR(fsq_session->fsq_packet.state),
			 fsq_session->fsq_packet.fsq_data.size);
		if (rc) {
			FSQ_ERROR((*fsq_session), rc, "fsq_recv failed");
			goto out;
		}

		if (fsq_session->fsq_packet.state & FSQ_CLOSE)
			goto out;

		size_t bytes_total = 0;
		bytes_recv = bytes_send = 0;
		memset(buf, 0, sizeof(buf));
		do {
			bytes_to_recv = fsq_session->fsq_packet.fsq_data.size < sizeof(buf) ?
				fsq_session->fsq_packet.fsq_data.size : sizeof(buf);
			if (fsq_session->fsq_packet.fsq_data.size - bytes_total < bytes_to_recv)
				bytes_to_recv = fsq_session->fsq_packet.fsq_data.size - bytes_total;

			bytes_recv = read_size(fsq_session->fd, buf, bytes_to_recv);
			CT_DEBUG("[fd=%d] read_size %zd, expected %zd, max possible %zd",
				 fsq_session->fd, bytes_recv, bytes_to_recv, sizeof(buf));
			if (bytes_recv < 0) {
				rc = -errno;
				FSQ_ERROR((*fsq_session), rc, "read_size error");
				goto out;
			}
			if (bytes_recv == 0) {
				CT_INFO("bytes_read: %zu, bytes_recv_total: %zu",
					bytes_recv, *bytes_recv_total);
				break;
			}
			*bytes_recv_total += bytes_recv;
			bytes_total += bytes_recv;

			bytes_send = write_size(*fd_local, buf, bytes_recv);
			CT_DEBUG("[fd=%d] write_size %zd, expected %zd, max possible %zd",
				 *fd_local, bytes_send, bytes_recv, sizeof(buf));
			if (bytes_send < 0) {
				rc = -errno;
				FSQ_ERROR((*fsq_session), rc, "write_size error");
				goto out;
			}
			*bytes_send_total += bytes_send;
		} while (bytes_total != fsq_session->fsq_packet.fsq_data.size);
		CT_DEBUG("[fd=%d,fd=%d] total read %zu, total written %zu",
			 fsq_session->fd, *fd_local, *bytes_recv_total, *bytes_send_total);

		fsq_send(fsq_session, (FSQ_DATA | FSQ_REPLY));
	} while (fsq_session->fsq_packet.state & FSQ_DATA);

out:
	if (rc)
		fsq_send(fsq_session, (FSQ_ERROR | FSQ_REPLY));
	else
		fsq_send(fsq_session, (FSQ_CLOSE | FSQ_REPLY));

	return rc;
}

static int client_authenticate(struct fsq_session_t *fsq_session,
			       int *archive_id, uid_t *uid, gid_t *gid)
{
	int rc;
	char servername[MAX_OPTIONS_LENGTH + 1] = {0};

	/* Verify node exists in identmap file. */
	rc = identmap_entry(&fsq_session->fsq_packet.fsq_login,
			    servername,
			    archive_id, uid, gid);
	CT_DEBUG("[rc=%d] identmap_entry", rc);
	if (rc) {
		CT_ERROR(rc, "identmap_entry");
		return rc;
	}

	/* Verify node has granted permissions on tsm server. */
	struct login_t login;
	struct session_t session;

	memset(&login, 0, sizeof(struct login_t));
	memset(&session, 0, sizeof(struct session_t));
	login_init(&login,
		   servername,
		   fsq_session->fsq_packet.fsq_login.node,
		   fsq_session->fsq_packet.fsq_login.password,
		   DEFAULT_OWNER,
		   LINUX_PLATFORM,
		   DEFAULT_FSNAME,
		   DEFAULT_FSTYPE);

	pthread_mutex_lock(&tsm_connect_mutex);
	rc = tsm_connect(&login, &session);
	CT_DEBUG("[rc=%d] tsm_connect", rc);
	if (rc)
		CT_ERROR(rc, "tsm_connect");

	tsm_disconnect(&session);
	pthread_mutex_unlock(&tsm_connect_mutex);

	return rc;
}

static void *thread_sock_client(void *arg)
{
	int rc;
	struct fsq_session_t fsq_session;
	char fpath_local[PATH_MAX + 1] = {0};
	int fd_local = -1;
	int *fd_ptr = (int *)arg;

	memset(&fsq_session, 0, sizeof(struct fsq_session_t));
	fsq_session.fd = *fd_ptr;

	/* State 1: Client calls fsq_fconnect(...). Receive fsq_packet with
	   fsq_login_t, check identmap and hand it over to tsm server for
	   authentication. */
	rc = fsq_recv(&fsq_session, FSQ_CONNECT);
	CT_DEBUG("[rc=%d,fd=%d] fsq_recv state '%s' = 0x%.4X node '%s' hostname '%s' "
		 "port %d", rc, fsq_session.fd,
		 FSQ_PROTOCOL_STR(fsq_session.fsq_packet.state),
		 fsq_session.fsq_packet.state,
		 fsq_session.fsq_packet.fsq_login.node,
		 fsq_session.fsq_packet.fsq_login.hostname,
		 fsq_session.fsq_packet.fsq_login.port);
	if (rc) {
		CT_ERROR(rc, "fsq_recv failed");
		goto out;
	}

	/* Verify FSQ protocol matches between client and server. */
	if (fsq_session.fsq_packet.ver != FSQ_PROTOCOL_VER) {
		rc = -ENOPROTOOPT;
		FSQ_ERROR(fsq_session, rc,
			  "fsq protocol mismatch used: %u, expected: %u",
			  fsq_session.fsq_packet.ver, FSQ_PROTOCOL_VER);
		rc = fsq_send(&fsq_session, FSQ_ERROR | FSQ_REPLY);
		goto out;
	}

	uid_t uid = 65534;	/* User: Nobody. */
	gid_t gid = 65534;	/* Group: Nobody. */
	int archive_id = -1;
	rc = client_authenticate(&fsq_session, &archive_id, &uid, &gid);
	if (rc) {
		FSQ_ERROR(fsq_session, rc,
			  "client_authenticate failed "
			  "node: '%s', passwd: '%s', "
			  "uid: %u, gid: %u",
			  fsq_session.fsq_packet.fsq_login.node,
			  fsq_session.fsq_packet.fsq_login.password,
			  uid, gid);
		rc = fsq_send(&fsq_session, FSQ_ERROR | FSQ_REPLY);
		goto out;
	}
	rc = fsq_send(&fsq_session, FSQ_CONNECT | FSQ_REPLY);
	if (rc)
		goto out;

	do {
		/* State 2: Client calls fsq_fopen(...) or fsq_disconnect(...)
		   and receives fsq_packet with fsq_info_t. */
		rc = fsq_recv(&fsq_session, (FSQ_OPEN | FSQ_DISCONNECT));
		CT_DEBUG("[rc=%d,fd=%d] fsq_recv state '%s':0x%.4X fs '%s' "
			 "fpath '%s' desc '%s' storage dest '%s'",
			 rc, fsq_session.fd,
			 FSQ_PROTOCOL_STR(fsq_session.fsq_packet.state),
			 fsq_session.fsq_packet.state,
			 fsq_session.fsq_packet.state == FSQ_OPEN
			 ? fsq_session.fsq_packet.fsq_info.fs : "",
			 fsq_session.fsq_packet.state == FSQ_OPEN
			 ? fsq_session.fsq_packet.fsq_info.fpath : "",
			 fsq_session.fsq_packet.state == FSQ_OPEN
			 ? fsq_session.fsq_packet.fsq_info.desc : "",
			 fsq_session.fsq_packet.state == FSQ_OPEN
			 ? FSQ_STORAGE_DEST_STR(
				 fsq_session.fsq_packet.fsq_info.fsq_storage_dest) : "");
		if (rc) {
			CT_ERROR(rc, "recv_fsq failed");
			goto out;
		}

		if (fsq_session.fsq_packet.state & FSQ_DISCONNECT)
			goto out;

		rc = init_fsq_storage(fpath_local, &fd_local, &fsq_session);
		if (rc) {
			FSQ_ERROR(fsq_session, rc,
				  "init_fsq_storage failed '%s'",
				  FSQ_STORAGE_DEST_STR(
					  fsq_session.fsq_packet.fsq_info.fsq_storage_dest));
			rc = fsq_send(&fsq_session, FSQ_ERROR | FSQ_REPLY);
			goto out;
		}
		rc = fsq_send(&fsq_session, FSQ_OPEN | FSQ_REPLY);
		if (rc)
			goto out;

		struct fsq_info_t fsq_info;
		size_t bytes_recv_total = 0;
		size_t bytes_send_total = 0;
		double ts = time_now();

		/* Note, subsequent fsq_recv_data(...) overwrites fsq_packet.fsq_info,
		   due to union structure, thus copy it here for later use. */
		memcpy(&fsq_info, &fsq_session.fsq_packet.fsq_info,
		       sizeof(struct fsq_info_t));

		/* State 3: Client calls fsq_fwrite(...) or fsq_close(...) and
		   receives fsq_packet with fsq_data_t. */
		rc = fsq_recv_data(&fd_local, &fsq_session, &bytes_recv_total,
				   &bytes_send_total);
		CT_DEBUG("[rc=%d,fd=%d] fsq_recv_data", rc, fsq_session.fd);
		if (rc) {
			CT_ERROR(rc, "fsq_recv_data failed");
			goto out;
		}

		/* Sanity check. */
		if (bytes_recv_total != bytes_send_total) {
			rc = -EFAILED;
			CT_ERROR(rc, "total number of bytes recv and send "
				 "differs, recv: %lu and send: %lu",
				 bytes_recv_total, bytes_send_total);
			goto out;
		}
		CT_INFO("[fd=%d,fd=%d] data buffer for fpath '%s' of size %zd "
			"successfully received in seconds %.3f",
			fsq_session.fd, fd_local,
			fsq_info.fpath,
			bytes_recv_total, time_now() - ts);

		/* Storage destination is FSQ_STORAGE_NULL, thus there
		   is no need to set extended attribute and leverage queue
		   for processing a fsq_action_item. */
		if (fsq_info.fsq_storage_dest == FSQ_STORAGE_NULL)
			goto finish_dev_null;

		rc = xattr_set_fsq(fpath_local, STATE_LOCAL_COPY_DONE, archive_id,
				   &fsq_info);
		if (rc)
			goto out;

		/* Producer. */
		struct fsq_action_item_t *fsq_action_item = NULL;

		fsq_action_item = create_fsq_item(bytes_recv_total,
						  &fsq_info,
						  fpath_local, archive_id,
						  uid, gid, ts);
		if (fsq_action_item == NULL)
			goto out;

		rc = enqueue_fsq_item(fsq_action_item);
		if (rc) {
			free(fsq_action_item);
			goto out;
		}

	finish_dev_null:
		rc = close(fd_local);
		CT_DEBUG("[rc=%d,fd=%d] close", rc, fd_local);
		if (rc < 0) {
			rc = -errno;
			CT_ERROR(rc, "close");
			goto out;
		}
		fd_local = -1;

	} while (1);

out:
	if (!(fd_local < 0))
		close(fd_local);

	if (!(fsq_session.fd < 0)) {
		close(fsq_session.fd);
		free(fd_ptr);
		fd_ptr = NULL;
	}

	pthread_mutex_lock(&mutex_sock_cnt);
	if (thread_sock_cnt > 0)
		thread_sock_cnt--;
	pthread_mutex_unlock(&mutex_sock_cnt);

	return NULL;
}

static int fsq_setup(void)
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

	/* Verify we have a valid Lustre mount point. */
	rc = llapi_search_fsname(opt.o_mnt_lustre, opt.o_mnt_lustre);
	if (rc < 0) {
		CT_ERROR(rc, "cannot find a Lustre filesystem mounted at '%s'",
			 opt.o_mnt_lustre);
	}

	return rc;
}

static void re_enqueue(const char *dpath)
{
	DIR *dir;
	struct dirent *entry;
	char npath[PATH_MAX] = {0};

	dir = opendir(dpath);
	if (!dir) {
		CT_ERROR(-errno, "opendir '%s'", dpath);
		return;
	}
	while (1) {
		errno = 0;
		entry = readdir(dir);

		if (!entry)
			break;

		switch (entry->d_type) {
		case DT_REG: {

			int rc;
			uint32_t fsq_action_state = 0;
			int archive_id = 0;
			char fpath_local[PATH_MAX + 1] = {0};
			struct fsq_info_t fsq_info = {
				.fs		   = {0},
				.fpath		   = {0},
				.desc		   = {0}
			};

			snprintf(fpath_local, PATH_MAX, "%s/%s", dpath, entry->d_name);
			rc = xattr_get_fsq(fpath_local, &fsq_action_state,
					   &archive_id, &fsq_info);
			if (rc)
				CT_ERROR(rc, "xattr_get_fsq '%s', "
					 "file cannot be re-enqueued", fpath_local);
			else {
				struct stat st;
				struct fsq_action_item_t *fsq_action_item = NULL;

				rc = stat(fpath_local, &st);
				if (rc) {
					CT_ERROR(-errno, "stat '%s'", fpath_local);
					break;
				}

				fsq_action_item = create_fsq_item(st.st_size,
								  &fsq_info,
								  fpath_local,
								  archive_id,
								  st.st_uid,
								  st.st_gid,
								  0);
				if (!fsq_action_item) {
					CT_WARN("create_fsq_item '%s' failed", fpath_local);
					break;
				}
				/* Set state of omitted file back to
				   STATE_LOCAL_COPY_DONE and start over. */
				if (fsq_action_state & STATE_FILE_OMITTED) {
					fsq_action_item->fsq_action_state = STATE_LOCAL_COPY_DONE;

					rc = enqueue_fsq_item(fsq_action_item);
					if (rc) {
						free(fsq_action_item);
						break;
					}
					CT_INFO("re-enqueue '%s'", fpath_local);
				}
			}
			break;
		}
		case DT_DIR: {
			if (!strcmp(entry->d_name, ".") ||
			    !strcmp(entry->d_name, ".."))
				continue;

			snprintf(npath, PATH_MAX, "%s/%s", dpath, entry->d_name);
			re_enqueue(npath);
			break;
		}
		default:{
			CT_WARN("skipping '%s', no regular file or directory",
				entry->d_name);
		}
		}
	} /* End while. */
	closedir(dir);
}


static void signal_handler(int signal)
{
	keep_running = false;
}

static int copy_action(struct fsq_action_item_t *fsq_action_item)
{
	int rc;
	int fd_read = -1;
	int fd_write = -1;
	char fpath_sub[PATH_MAX + 1] = {0};
	uint16_t i = 0;

	fd_read = open(fsq_action_item->fpath_local, O_RDONLY);
	if (fd_read < 0) {
		rc = -errno;
		CT_ERROR(rc, "open '%s'", fsq_action_item->fpath_local);
		return rc;
	}

	while (fsq_action_item->fsq_info.fpath[i] != '\0') {
		if (fsq_action_item->fsq_info.fpath[i] == '/' && i > 0) {
			strncpy(fpath_sub, fsq_action_item->fsq_info.fpath, i);
			rc = mkdir(fpath_sub,
				   S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
			if (rc < 0) {
				if (errno != EEXIST) {
					rc = -errno;
					CT_ERROR(rc, "mkdir '%s'", fpath_sub);
					goto out;
				}
				goto next; /* Directory exists, skip it. */
                        }
			rc = chown(fpath_sub, fsq_action_item->uid, fsq_action_item->gid);
			if (rc < 0) {
				rc = -errno;
				CT_ERROR(rc, "chown '%s', uid %zu, gid %zu",
					 fpath_sub, fsq_action_item->uid, fsq_action_item->gid);
				goto out;
			}
		}
	next:
		i++;
	}

	fd_write = open(fsq_action_item->fsq_info.fpath,
			O_WRONLY | O_CREAT | O_EXCL,
			S_IRUSR | S_IWUSR | S_IRGRP);
	if (fd_write < 0) {
		/* Note, if file exists, then fd_write < 0 and errno EEXISTS is
		   set. This makes sure we do NOT overwrite existing files. To
		   allow overwriting replace O_EXCL with O_TRUNC. */
		rc = -errno;
		CT_ERROR(rc, "open '%s'", fsq_action_item->fsq_info.fpath);
		goto out;
	}

	/* Sanity check. */
	struct stat statbuf = {0};
	rc = fstat(fd_read, &statbuf);
	if (rc) {
		rc = -errno;
		CT_ERROR(rc, "stat");
		goto out;
	}
	if (statbuf.st_size != (off_t)fsq_action_item->size) {
		rc = -ERANGE;
		CT_ERROR(rc, "'%s' "
			 "fstat.st_size %zu != fsq_action_item->size %zu",
			 fsq_action_item->fsq_info.fpath,
			 statbuf.st_size, fsq_action_item->size);
		goto out;
	}

	uint8_t buf[BUF_SIZE];
	ssize_t bytes_read, bytes_read_total = 0;
	ssize_t bytes_write, bytes_write_total = 0;
	double ts = time_now();
	do {
		bytes_read = read_size(fd_read, buf, sizeof(buf));
		bytes_read_total += bytes_read;
		CT_DEBUG("[fd=%d] read_size %zd, total read size",
			 fd_read, bytes_read, bytes_read_total);
		if (bytes_read < 0) {
			rc = -errno;
			CT_ERROR(rc, "read_size");
			goto out;
		}
		if (bytes_read == 0) {
			CT_INFO("[fd=%d] bytes_read: %zu, bytes_read_total: %zu",
				fd_read, bytes_read, bytes_read_total);
			break;
		}

		bytes_write = write_size(fd_write, buf, bytes_read);
		bytes_write_total += bytes_write;
		CT_DEBUG("[fd=%d] write_size %zd, total write size",
			 fd_write, bytes_write, bytes_write_total);
		if (bytes_write < 0) {
			rc = -errno;
			CT_ERROR(rc, "write_size");
			goto out;
		}
	} while (statbuf.st_size != bytes_read_total);

	/* Sanity check. */
	if (bytes_read_total != bytes_write_total) {
		rc = -ERANGE;
		CT_ERROR(rc, "total number of bytes read and written differs, "
			 "read: %zu and send: %zu", bytes_read_total, bytes_write_total);
		goto out;
	}
	CT_INFO("[fd_read=(%d,'%s'),fd_write=(%d,'%s')] data buffer of "
		"size %zd successfully read and written seconds %.3f",
		fd_read, fsq_action_item->fpath_local,
		fd_write, fsq_action_item->fsq_info.fpath,
		bytes_read_total, time_now() - ts);

	/* Change owner and group. */
	rc = fchown(fd_write, fsq_action_item->uid, fsq_action_item->gid);
	CT_DEBUG("[rc=%d,fd=%d] fchown '%s', uid %zu gid %zu", rc, fd_write,
		 fsq_action_item->fsq_info.fpath,
		 fsq_action_item->uid, fsq_action_item->gid);
	if (rc) {
		rc = -errno;
		CT_ERROR(rc, "fchown '%s', uid %zu, gid %zu",
			 fsq_action_item->fsq_info.fpath,
			 fsq_action_item->uid, fsq_action_item->gid);
	}

out:
	if (fd_read != -1)
		close(fd_read);

	if (fd_write != -1)
		close(fd_write);

	return rc;
}

#ifdef LTSMFSQ_POLL_ARCHIVE_FINISHED
static int archive_state(const struct fsq_action_item_t *fsq_action_item,
			 uint32_t *states)
{
	int rc;
	struct hsm_user_state hus;

	rc = llapi_hsm_state_get(fsq_action_item->fsq_info.fpath, &hus);
	CT_DEBUG("[rc=%d] llapi_hsm_state_get '%s'", rc,
		 fsq_action_item->fsq_info.fpath);
	if (rc) {
		CT_ERROR(rc, "llapi_hsm_state_get '%s'",
			 fsq_action_item->fsq_info.fpath);
		return rc;
	}

	*states = hus.hus_states;

	return rc;
}
#endif

static int archive_action(struct fsq_action_item_t *fsq_action_item)
{
	int rc;
	struct hsm_user_request	*hur = NULL;
	struct hsm_user_item	*hui = NULL;
	struct lu_fid		fid;

	rc = llapi_path2fid(fsq_action_item->fsq_info.fpath, &fid);
	CT_DEBUG("[rc=%d] llapi_path2fid '%s' "DFID"",
		 rc, fsq_action_item->fsq_info.fpath,
		 PFID(&fid));
	if (rc) {
		CT_ERROR(rc, "llapi_path2fid '%s'",
			 fsq_action_item->fsq_info.fpath);
		return rc;
	}

	hur = llapi_hsm_user_request_alloc(1, 0);
	if (hur == NULL) {
		rc = -errno;
		CT_ERROR(rc, "llapi_hsm_user_request_alloc failed '%s'",
			 fsq_action_item->fsq_info.fpath);
		return rc;
	}
	hur->hur_request.hr_action = HUA_ARCHIVE;
	hur->hur_request.hr_archive_id = fsq_action_item->archive_id;
	hur->hur_request.hr_flags = 0;
	hur->hur_request.hr_itemcount = 1;
	hur->hur_request.hr_data_len = 0;

	hui = &hur->hur_user_item[0];
	hui->hui_fid = fid;

	rc = llapi_hsm_request(fsq_action_item->fsq_info.fpath, hur);

	free(hur);

	return rc;
}

static int finalize_fsq_action_item(struct fsq_action_item_t *fsq_action_item)
{
	int rc = -EINPROGRESS;
	enum fsq_action_state_t fsq_action_state = fsq_action_item->fsq_action_state;
	bool storage_dest_reached =
		(fsq_action_item->fsq_info.fsq_storage_dest == FSQ_STORAGE_LOCAL &&
		 fsq_action_item->fsq_action_state == STATE_LOCAL_COPY_DONE) ||
		(fsq_action_item->fsq_info.fsq_storage_dest == FSQ_STORAGE_LUSTRE &&
		 fsq_action_item->fsq_action_state == STATE_LUSTRE_COPY_DONE) ||
		((fsq_action_item->fsq_info.fsq_storage_dest == FSQ_STORAGE_TSM ||
		  fsq_action_item->fsq_info.fsq_storage_dest == FSQ_STORAGE_LUSTRE_TSM) &&
		 fsq_action_item->fsq_action_state == STATE_TSM_ARCHIVE_DONE);

	if (storage_dest_reached) {
		rc = xattr_update_fsq_state(fsq_action_item, STATE_FILE_KEEP);
		CT_DEBUG("[rc=%d] setting state from '%s' to '%s'",
			 rc,
			 FSQ_ACTION_STR(fsq_action_state),
			 FSQ_ACTION_STR(STATE_FILE_KEEP));
		if (rc) {
			fsq_action_item->action_error_cnt++;
			fsq_action_item->fsq_action_state = fsq_action_state;
			CT_WARN("setting state from '%s' to '%s' failed, "
				"going back to state '%s'",
				FSQ_ACTION_STR(fsq_action_state),
				FSQ_ACTION_STR(STATE_FILE_KEEP),
				FSQ_ACTION_STR(fsq_action_item->fsq_action_state));
		} else {
			double ts = 0;
			if (fsq_action_item->fsq_info.fsq_storage_dest == FSQ_STORAGE_LOCAL)
				ts = fsq_action_item->ts[1] - fsq_action_item->ts[0];
			else if (fsq_action_item->fsq_info.fsq_storage_dest == FSQ_STORAGE_LUSTRE)
				ts = fsq_action_item->ts[2] - fsq_action_item->ts[0];
			else if (fsq_action_item->fsq_info.fsq_storage_dest == FSQ_STORAGE_LUSTRE_TSM ||
				 fsq_action_item->fsq_info.fsq_storage_dest == FSQ_STORAGE_TSM)
				ts = fsq_action_item->ts[3] - fsq_action_item->ts[0];
			CT_MESSAGE("file '%s' of size %zu stored at target destination '%s' in %.3f seconds",
				   fsq_action_item->fpath_local,
				   fsq_action_item->size,
				   FSQ_STORAGE_DEST_STR(fsq_action_item->fsq_info.fsq_storage_dest),
				   ts);

			/* Remove file from Lustre file system. */
			if (fsq_action_item->fsq_info.fsq_storage_dest == FSQ_STORAGE_TSM &&
			    fsq_action_item->fsq_action_state == STATE_TSM_ARCHIVE_DONE) {
				rc = unlink(fsq_action_item->fsq_info.fpath);
				CT_DEBUG("[rc=%d] unlink '%s'", rc, fsq_action_item->fsq_info.fpath);
				if (rc < 0) {
					rc = -errno;
					CT_ERROR(rc, "unlink '%s'", fsq_action_item->fsq_info.fpath);

					return rc;
				} else {
					CT_INFO("unlink '%s' and action item %p",
						fsq_action_item->fsq_info.fpath, fsq_action_item);
				}
			}

			/* Remove file from local storage. */
			if (fsq_action_item->fsq_info.fsq_storage_dest != FSQ_STORAGE_LOCAL) {
				rc = unlink(fsq_action_item->fpath_local);
				CT_DEBUG("[rc=%d] unlink '%s'", rc, fsq_action_item->fpath_local);
				if (rc < 0) {
					rc = -errno;
					CT_ERROR(rc, "unlink '%s'", fsq_action_item->fpath_local);

					return rc;
				} else {
					CT_INFO("unlink '%s' and action item %p",
						fsq_action_item->fpath_local, fsq_action_item);
				}
			}
		}
	}

	return rc;
}

/*
  The following state diagram is implemented.

  +-----------------------+      +-----------------------+
->| STATE_LOCAL_COPY_DONE +----->+ STATE_LUSTRE_COPY_RUN |
  +--------+--------------+      +------------+----------+
           ^                                  |
           |   +-------------------------+    |
           +---+ STATE_LUSTRE_COPY_ERROR +<---+
               +-------------------------+    |
                                              v
  +-----------------------+         +---------+--------------+
  | STATE_TSM_ARCHIVE_RUN +<--------+ STATE_LUSTRE_COPY_DONE |
  +--------+--------------+         +-----------+------------+
           |                                    ^
           |     +-------------------------+    |
           +---->+ STATE_TSM_ARCHIVE_ERROR +----+
           |     +-------------------------+
           v
 +---------+--------------+
 | STATE_TSM_ARCHIVE_DONE |
 +------------------------+
 */
static int process_fsq_action_item(struct fsq_action_item_t *fsq_action_item)
{
	int rc = 0;

	CT_DEBUG("process_fsq_action_item %p, state '%s', fs '%s', fpath '%s', "
		 "storage dest '%s', size %zu, errors %d, "
		 "ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, queue size %lu",
		 fsq_action_item,
		 FSQ_ACTION_STR(fsq_action_item->fsq_action_state),
		 fsq_action_item->fsq_info.fs,
		 fsq_action_item->fsq_info.fpath,
		 FSQ_STORAGE_DEST_STR(fsq_action_item->fsq_info.fsq_storage_dest),
		 fsq_action_item->size,
		 fsq_action_item->action_error_cnt,
		 fsq_action_item->ts[0],
		 fsq_action_item->ts[1],
		 fsq_action_item->ts[2],
		 queue_size(&queue));

	if (fsq_action_item->action_error_cnt > (size_t)opt.o_ntol_file_errors) {
		CT_WARN("file '%s' reached maximum number of tolerated errors, "
			"and is omitted", fsq_action_item->fpath_local);
		rc = xattr_update_fsq_state(fsq_action_item,
					    STATE_FILE_OMITTED);
		free(fsq_action_item);
		return 0;
	}

	rc = finalize_fsq_action_item(fsq_action_item);
	if (!rc) {
		free(fsq_action_item);
		return 0;
	}

	switch (fsq_action_item->fsq_action_state) {
	case STATE_LOCAL_COPY_DONE: {

		rc = xattr_update_fsq_state(fsq_action_item,
					    STATE_LUSTRE_COPY_RUN);
		CT_DEBUG("[rc=%d] setting state from '%s' to '%s'",
			 rc,
			 FSQ_ACTION_STR(STATE_LOCAL_COPY_DONE),
			 FSQ_ACTION_STR(STATE_LUSTRE_COPY_RUN));
		if (rc) {
			fsq_action_item->action_error_cnt++;
			fsq_action_item->fsq_action_state = STATE_LOCAL_COPY_DONE;
			CT_WARN("setting state from '%s' to '%s' failed, "
				"going back to state '%s'",
				FSQ_ACTION_STR(STATE_LOCAL_COPY_DONE),
				FSQ_ACTION_STR(STATE_LUSTRE_COPY_RUN),
				FSQ_ACTION_STR(fsq_action_item->fsq_action_state));
			break;
		}

		double ts = time_now();
		rc = copy_action(fsq_action_item);
		if (rc) {
			CT_WARN("file '%s' copying to '%s' failed, will "
				"try again",
				fsq_action_item->fpath_local,
				fsq_action_item->fsq_info.fpath);
			fsq_action_item->action_error_cnt++;
			fsq_action_item->fsq_action_state = STATE_LUSTRE_COPY_ERROR;
			break;
		}
		CT_MESSAGE("file '%s' copied to '%s' of size %zu in seconds %.3f",
			   fsq_action_item->fpath_local,
			   fsq_action_item->fsq_info.fpath,
			   fsq_action_item->size,
			   time_now() - ts);

		rc = xattr_update_fsq_state(fsq_action_item,
					      STATE_LUSTRE_COPY_DONE);
		CT_DEBUG("[rc=%d] setting state from '%s' to '%s'",
			 rc,
			 FSQ_ACTION_STR(STATE_LUSTRE_COPY_RUN),
			 FSQ_ACTION_STR(STATE_LUSTRE_COPY_DONE));
		if (rc) {
			fsq_action_item->action_error_cnt++;
			fsq_action_item->fsq_action_state = STATE_LOCAL_COPY_DONE;
			CT_WARN("setting state from '%s' to '%s' failed, "
				"going back to state '%s'",
				FSQ_ACTION_STR(STATE_LOCAL_COPY_DONE),
				FSQ_ACTION_STR(STATE_LUSTRE_COPY_DONE),
				FSQ_ACTION_STR(fsq_action_item->fsq_action_state));
			break;
		}
		fsq_action_item->ts[2] = time_now();
		break;
	}
	case STATE_LUSTRE_COPY_RUN: {
		/* TODO: Update fsq_action_item->progress_size. */
		break;
	}
	case STATE_LUSTRE_COPY_ERROR: {
		CT_WARN("fsq to lustre copy error, try to copy "
			"file '%s' to '%s' again",
			fsq_action_item->fpath_local,
			fsq_action_item->fsq_info.fpath);
		rc = xattr_update_fsq_state(fsq_action_item,
					    STATE_LOCAL_COPY_DONE);
		if (rc)
			fsq_action_item->action_error_cnt++;
		break;
	}
	case STATE_LUSTRE_COPY_DONE: {
		rc = xattr_update_fsq_state(fsq_action_item,
					    STATE_TSM_ARCHIVE_RUN);
		CT_DEBUG("[rc=%d] setting state from '%s' to '%s'",
			 rc,
			 FSQ_ACTION_STR(STATE_LUSTRE_COPY_DONE),
			 FSQ_ACTION_STR(STATE_TSM_ARCHIVE_RUN));
		if (rc) {
			fsq_action_item->action_error_cnt++;
			fsq_action_item->fsq_action_state = STATE_LUSTRE_COPY_DONE;
			CT_WARN("setting state from '%s' to '%s' failed, "
				"going back to state '%s'",
				FSQ_ACTION_STR(STATE_LUSTRE_COPY_DONE),
				FSQ_ACTION_STR(STATE_TSM_ARCHIVE_RUN),
				FSQ_ACTION_STR(fsq_action_item->fsq_action_state));
			break;
		}
		rc = archive_action(fsq_action_item);
		if (rc) {
			CT_WARN("file '%s' archiving failed, will try again",
				fsq_action_item->fpath_local);
			fsq_action_item->action_error_cnt++;
			fsq_action_item->fsq_action_state = STATE_TSM_ARCHIVE_ERROR;
		}
		break;
	}
	case STATE_TSM_ARCHIVE_RUN: {
		uint32_t states = 0;

#ifdef LTSMFSQ_POLL_ARCHIVE_FINISHED
		/* We stay in STATE_TSM_ARCHIVE_RUN and poll every 50ms to check,
		   whether the archive operation finishes successfully. This approach,
		   however, is not efficient and in addition congests the queue, as the
		   the fsq_action_item is enqueued/dequeued over and over until the file
		   is finally archived. To overcome this problem, we change immediately
		   (via undefined LTSMFSQ_POLL_ARCHIVE_FINISHED) the state from
		   STATE_TSM_ARCHIVE_RUN to STATE_TSM_ARCHIVE_DONE and assume:
		   If archive_action(fsq_action_item) returns success, then the file
		   is also successfully archived. To reject this assumption, just define
		   directive LTSMFSQ_POLL_ARCHIVE_FINISHED to activate the poll mechanism. */

		/* The archive_state check is based on polling, thus employ
		   sleep(50ms) to avoid high CPU load. */
		nanosleep(&(struct timespec){0, 50000000UL}, NULL);

		rc = archive_state(fsq_action_item, &states);
		CT_DEBUG("[rc=%d] archive_state: %d", states);
		if (rc) {
			/* If state cannot be obtained, stay in state
			   STATE_TSM_ARCHIVE_RUN and try again. */
			fsq_action_item->action_error_cnt++;
			CT_ERROR(rc, "archive state '%s'",
				 fsq_action_item->fsq_info.fpath);
			break;
		}
#else
		states = HS_EXISTS | HS_ARCHIVED;
#endif

		/* Verify whether file is finally archived. Note, this check also
		   returns true when file exists, is archived and is e.g. dirty.
		   For an exact match on HS_EXISTS and HS_ARCHIVED, do:
		   (states == (HS_EXISTS | HS_ARCHIVED)) */
		if ((states & HS_EXISTS) && (states & HS_ARCHIVED)) {
			rc = xattr_update_fsq_state(fsq_action_item,
						    STATE_TSM_ARCHIVE_DONE);
			CT_DEBUG("[rc=%d] setting state from '%s' to '%s'",
				 rc,
				 FSQ_ACTION_STR(STATE_TSM_ARCHIVE_RUN),
				 FSQ_ACTION_STR(STATE_TSM_ARCHIVE_DONE));
			if (rc) {
				fsq_action_item->action_error_cnt++;
				fsq_action_item->fsq_action_state = STATE_LUSTRE_COPY_DONE;
				CT_WARN("setting state from '%s' to '%s' failed, "
					"going back to state '%s'",
					FSQ_ACTION_STR(STATE_TSM_ARCHIVE_RUN),
					FSQ_ACTION_STR(STATE_TSM_ARCHIVE_DONE),
					FSQ_ACTION_STR(fsq_action_item->fsq_action_state));
				break;
			}
			fsq_action_item->ts[3] = time_now();
			CT_MESSAGE("file '%s' of size %zu in queue archived "
				   "in %.3f seconds",
				   fsq_action_item->fpath_local,
				   fsq_action_item->size,
				   fsq_action_item->ts[3] - fsq_action_item->ts[2]);
		}

		/* Stay in state STATE_TSM_ARCHIVE_RUN. */
		break;
	}
	case STATE_TSM_ARCHIVE_ERROR: {
		CT_WARN("tsm archive error, try to archive file '%s' again",
			fsq_action_item->fpath_local);
		rc = xattr_update_fsq_state(fsq_action_item,
					    STATE_LUSTRE_COPY_DONE);
		if (rc)
			fsq_action_item->action_error_cnt++;

		break;
	}
	case STATE_TSM_ARCHIVE_DONE: {
		break;
	}
	case STATE_FILE_OMITTED: {
		CT_MESSAGE("file '%s' is omitted and removed from queue",
			   fsq_action_item->fpath_local);
		free(fsq_action_item);
		return 0;
	}
	default:		/* We should never be here. */
		rc = -ERANGE;
		CT_ERROR(rc, "unknown action state");
		return rc;
	}

	rc = enqueue_fsq_item(fsq_action_item);

	return rc;
}

static void *thread_queue_consumer(void *data)
{
	int rc;
	struct fsq_action_item_t *fsq_action_item;

	for (;;) {
		/* Critical region, lock. */
		pthread_mutex_lock(&queue_mutex);

		/* While queue is empty wait for new FSQ action item. */
		while (queue_size(&queue) == 0)
			pthread_cond_wait(&queue_cond, &queue_mutex);

		rc = queue_dequeue(&queue, (void **)&fsq_action_item);

		/* Unlock. */
		pthread_mutex_unlock(&queue_mutex);

		if (rc) {
			rc = -EFAILED;
			CT_ERROR(rc, "failed dequeue operation: "
				 "%p, state '%s', fs '%s', fpath '%s', size %zu, "
				 "errors %d, ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, ts[3] %.3f, queue size %lu",
				 fsq_action_item,
				 FSQ_ACTION_STR(fsq_action_item->fsq_action_state),
				 fsq_action_item->fsq_info.fs,
				 fsq_action_item->fsq_info.fpath,
				 fsq_action_item->size,
				 fsq_action_item->action_error_cnt,
				 fsq_action_item->ts[0],
				 fsq_action_item->ts[1],
				 fsq_action_item->ts[2],
				 fsq_action_item->ts[3],
				 queue_size(&queue));
		} else {
			CT_INFO("dequeue operation: "
				"%p, state '%s', fs '%s', fpath '%s', size %zu, "
				"errors %d, ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, ts[3] %.3f, queue size %lu",
				fsq_action_item,
				FSQ_ACTION_STR(fsq_action_item->fsq_action_state),
				fsq_action_item->fsq_info.fs,
				fsq_action_item->fsq_info.fpath,
				fsq_action_item->size,
				fsq_action_item->action_error_cnt,
				fsq_action_item->ts[0],
				fsq_action_item->ts[1],
				fsq_action_item->ts[2],
				fsq_action_item->ts[3],
				queue_size(&queue));

			rc = process_fsq_action_item(fsq_action_item);
		}
	}

	return NULL;
}

static int start_queue_consumer_threads(void)
{
	int rc;
	pthread_attr_t attr;

	/* Initialize queue worker threads. */
	threads_queue = calloc(opt.o_nthreads_queue, sizeof(pthread_t));
	if (threads_queue == NULL) {
		rc = -errno;
		CT_ERROR(rc, "calloc");
		return rc;
	}

	rc = pthread_attr_init(&attr);
	if (rc != 0) {
		CT_ERROR(rc, "pthread_attr_init");
		goto cleanup;
	}

	rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	if (rc != 0) {
		CT_ERROR(rc, "pthread_attr_setdetachstate");
		goto cleanup;
	}

	for (uint16_t n = 0; n < opt.o_nthreads_queue; n++) {
		rc = pthread_create(&threads_queue[n], &attr, thread_queue_consumer, NULL);
		if (rc != 0)
			CT_ERROR(rc, "cannot create queue consumer thread '%d'", n);
		else
			CT_MESSAGE("created queue consumer thread fsq_queue/%d", n);
	}
	pthread_attr_destroy(&attr);
	return rc;

cleanup:
	free(threads_queue);
	threads_queue = NULL;

	return rc;
}

int main(int argc, char *argv[])
{
	int rc;
	int srv_sock_fd = -1;
	pthread_t *threads_sock = NULL;
	pthread_attr_t attr;
	struct sigaction sig_act;

	sig_act.sa_handler = signal_handler;
	sig_act.sa_flags = 0;
	sigemptyset(&sig_act.sa_mask);
	sigaction(SIGINT, &sig_act, NULL);
	sigaction(SIGTERM, &sig_act, NULL);

	list_init(&ident_list, free);
	queue_init(&queue, free);

	rc = parseopts(argc, argv);
	if (rc < 0) {
		CT_WARN("try '%s --help' for more information", argv[0]);
		goto cleanup;
	}

	rc = fsq_setup();
	if (rc < 0)
		goto cleanup;

	/* Re-enqueue files stored in opt.o_local_mount. */
	re_enqueue(opt.o_local_mount);

	/* Bind and listen to port opt.o_port. */
	srv_sock_fd = listen_socket_srv(opt.o_port);
	if (srv_sock_fd < 0)
		goto cleanup;

	CT_MESSAGE("listening on port %d with %d socket threads, %d queue "
		   "worker threads, local fs '%s' and number of tolerated "
		   "file errors %d",
		   opt.o_port, opt.o_nthreads_sock, opt.o_nthreads_queue,
		   opt.o_local_mount, opt.o_ntol_file_errors);

	/* Initialize socket processing threads. */
	threads_sock = calloc(opt.o_nthreads_sock, sizeof(pthread_t));
	if (threads_sock == NULL) {
		rc = -errno;
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

	rc = start_queue_consumer_threads();
	if (rc != 0)
		goto cleanup_attr;

	while (keep_running) {

		int fd;
		struct sockaddr_in sockaddr_cli;
		socklen_t addrlen;

		memset(&sockaddr_cli, 0, sizeof(sockaddr_cli));
		addrlen = sizeof(sockaddr_cli);
		fd = accept(srv_sock_fd, (struct sockaddr *)&sockaddr_cli,
			    &addrlen);
		if (fd < 0)
			CT_ERROR(errno, "accept");
		else {
			if (thread_sock_cnt >= opt.o_nthreads_sock) {
				CT_WARN("maximum number %d of serving "
					"socket threads exceeded",
					opt.o_nthreads_sock);
				close(fd);
				continue;
			}

			int *fd_sock = NULL;
			fd_sock = malloc(sizeof(int));
			if (!fd_sock) {
				rc = -errno;
				CT_ERROR(rc, "malloc");
				goto cleanup_attr;
			}
			*fd_sock = fd;

			pthread_mutex_lock(&mutex_sock_cnt);
			rc = pthread_create(&threads_sock[thread_sock_cnt], &attr,
					    thread_sock_client, fd_sock);
			if (rc != 0)
				CT_ERROR(rc, "cannot create thread for "
					 "client '%s'",
					 inet_ntoa(sockaddr_cli.sin_addr));
			else {
				CT_MESSAGE("created socket thread 'fsq_sock/%d' for "
					   "client '%s' and fd %d",
					   thread_sock_cnt,
					   inet_ntoa(sockaddr_cli.sin_addr),
					   *fd_sock);
				thread_sock_cnt++;
			}
			pthread_mutex_unlock(&mutex_sock_cnt);
		}
	}

cleanup_attr:
	pthread_attr_destroy(&attr);

cleanup:
	if (threads_sock)
		free(threads_sock);

	if (threads_queue)
		free(threads_queue);

	if (srv_sock_fd > -1)
		close(srv_sock_fd);

	queue_destroy(&queue);
	list_destroy(&ident_list);

	return rc;
}
