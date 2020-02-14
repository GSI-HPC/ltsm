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
#include "tsmapi.h"
#include "fsdapi.h"
#include "xattr.h"
#include "queue.h"

#define PORT_DEFAULT_FSD	7625
#define N_THREADS_SOCK_DEFAULT	4
#define N_THREADS_SOCK_MAX	64
#define N_THREADS_QUEUE_DEFAULT	4
#define N_THREADS_QUEUE_MAX	64
#define N_TOL_FILE_ERRORS	16
#define BACKLOG			32
#define BUF_SIZE		0xfffff	/* 0xfffff = 1MiB, 0x400000 = 4MiB */

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
	.o_port		    = PORT_DEFAULT_FSD,
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
		" [default: message]\n"
		"\t\t""produce more verbose output\n"
		"\t-h, --help\n"
		"\t\t""show this help\n"
		"version: %s © 2020 by GSI Helmholtz Centre for Heavy Ion Research\n",
		cmd_name,
		PORT_DEFAULT_FSD,
		N_THREADS_SOCK_DEFAULT,
		N_THREADS_QUEUE_DEFAULT,
		N_TOL_FILE_ERRORS,
		PACKAGE_VERSION);
	exit(rc);
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
		{"localfs",	required_argument, 0,	'l'},
		{"identmap",	required_argument, 0,	'i'},
		{"port",	required_argument, 0,	'p'},
		{"sthreads",	required_argument, 0,	's'},
		{"qthreads",	required_argument, 0,	'q'},
		{"tolerr",	required_argument, 0,	't'},
		{"conf",	required_argument, 0,	'c'},
		{"verbose",	required_argument, 0,	'v'},
		{"help",	no_argument,	   0,	'h'},
		{0, 0, 0, 0}
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

	list_init(&ident_list, free);
	rc = parse_file_ident(opt.o_file_ident);
	if (!rc && opt.o_verbose >= API_MSG_INFO)
		list_for_each(&ident_list, print_ident);

	return rc;
}

static int identmap_entry(struct fsd_login_t *fsd_login,
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
		if (!strncmp(fsd_login->node, ident_map->node,
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
			 fsd_login->node);
		rc = -EACCES;
	}

	return rc;
}

static int enqueue_fsd_item(struct fsd_action_item_t *fsd_action_item)
{
	int rc;

	/* Lock queue to avoid thread access. */
	pthread_mutex_lock(&queue_mutex);

	rc = queue_enqueue(&queue, fsd_action_item);

	if (rc) {
		rc = -EFAILED;
		CT_ERROR(rc, "failed enqueue operation: "
			 "%p, state '%s', fs '%s', fpath '%s', size %zu, "
			 "errors %d, ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, queue size %lu",
			 fsd_action_item,
			 FSD_ACTION_STR(fsd_action_item->fsd_action_state),
			 fsd_action_item->fsd_info.fs,
			 fsd_action_item->fsd_info.fpath,
			 fsd_action_item->size,
			 fsd_action_item->action_error_cnt,
			 fsd_action_item->ts[0],
			 fsd_action_item->ts[1],
			 fsd_action_item->ts[2],
			 queue_size(&queue));

		free(fsd_action_item);
	} else
		CT_INFO("enqueue operation: "
			"%p, state '%s', fs '%s', fpath '%s', size %zu, "
			"errors %d, ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, queue size %lu",
			fsd_action_item,
			FSD_ACTION_STR(fsd_action_item->fsd_action_state),
			fsd_action_item->fsd_info.fs,
			fsd_action_item->fsd_info.fpath,
			fsd_action_item->size,
			fsd_action_item->action_error_cnt,
			fsd_action_item->ts[0],
			fsd_action_item->ts[1],
			fsd_action_item->ts[2],
			queue_size(&queue));

	/* Free the lock of the queue. */
	pthread_mutex_unlock(&queue_mutex);

	/* Wakeup sleeping consumer (worker threads). */
	pthread_cond_signal(&queue_cond);

	return rc;
}

static struct fsd_action_item_t* create_fsd_item(const size_t bytes_recv_total,
						 const struct fsd_info_t *fsd_info,
						 const char *fpath_local, const int archive_id,
						 const uid_t uid, const gid_t gid)
{
	struct fsd_action_item_t *fsd_action_item;

	fsd_action_item = calloc(1, sizeof(struct fsd_action_item_t));
	if (!fsd_action_item) {
		CT_ERROR(-errno, "calloc");
		return NULL;
	}

	fsd_action_item->fsd_action_state = STATE_FSD_COPY_DONE;
	fsd_action_item->size = bytes_recv_total;
	memcpy(&fsd_action_item->fsd_info, fsd_info, sizeof(struct fsd_info_t));
	fsd_action_item->ts[0] = time_now();
	fsd_action_item->ts[1] = 0;
	fsd_action_item->ts[2] = 0;
	strncpy(fsd_action_item->fpath_local, fpath_local, PATH_MAX);
	fsd_action_item->archive_id = archive_id;
	fsd_action_item->uid = uid;
	fsd_action_item->gid = gid;

	return fsd_action_item;
}

static int init_fsd_local(char **fpath_local, int *fd_local,
			  const struct fsd_session_t *fsd_session)
{
	int rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	rc = extract_hl_ll(fsd_session->fsd_info.fpath, fsd_session->fsd_info.fs,
			   hl, ll);
	if (rc) {
		rc = -EFAILED;
		CT_ERROR(rc, "extract_hl_ll");
		return rc;
	}

	const size_t L = strlen(opt.o_local_mount) + strlen(hl) + strlen(ll);
	const size_t L_MAX = PATH_MAX + DSM_MAX_HL_LENGTH +
		DSM_MAX_LL_LENGTH + 1;
	if (L > PATH_MAX) {
		rc = -ENAMETOOLONG;
		CT_ERROR(rc, "fpath name '%s/%s/%s'",
			 opt.o_local_mount, hl, ll);
		return rc;
	}
	/* This twist is necessary to avoid error:
	   ‘%s’ directive output may be truncated writing up to ... */
	*fpath_local = calloc(1, L_MAX);
	if (!*fpath_local) {
		rc = -errno;
		CT_ERROR(rc, "calloc");
		return rc;
	}
	snprintf(*fpath_local, L_MAX, "%s/%s", opt.o_local_mount, hl);

	/* Make sure the directory exists where to store the file. */
	rc = mkdir_p(*fpath_local, S_IRWXU | S_IRGRP | S_IXGRP
		     | S_IROTH | S_IXOTH);
	CT_DEBUG("[rc=%d] mkdir_p '%s'", rc, *fpath_local);
	if (rc) {
		CT_ERROR(rc, "mkdir_p '%s'", *fpath_local);
		return rc;
	}

	snprintf(*fpath_local + strlen(*fpath_local), PATH_MAX, "/%s", ll);

	*fd_local = open(*fpath_local, O_WRONLY | O_TRUNC | O_CREAT,
			 S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	CT_DEBUG("[fd=%d] open '%s'", *fd_local, *fpath_local);
	if (*fd_local < 0) {
		rc = -errno;
		CT_ERROR(rc, "open '%s'", *fpath_local);
		return rc;
	}

	return rc;
}

static int recv_fsd_data(int *fd_sock, int *fd_local,
			 struct fsd_session_t *fsd_session,
			 size_t *bytes_recv_total, size_t *bytes_send_total)
{
	int rc;
	uint8_t buf[BUF_SIZE];
	ssize_t bytes_recv, bytes_to_recv;
	ssize_t bytes_send;

	do {
		rc = fsd_recv(*fd_sock, fsd_session, (FSD_DATA | FSD_CLOSE));
		CT_DEBUG("[rc=%d,fd=%d] fsd_recv, size %zu", rc, *fd_sock,
			 fsd_session->size);
		if (rc) {
			CT_ERROR(rc, "fsd_recv failed");
			goto out;
		}

		if (fsd_session->state & FSD_CLOSE)
			goto out;

		size_t bytes_total = 0;
		bytes_recv = bytes_send = 0;
		memset(buf, 0, sizeof(buf));
		do {
			bytes_to_recv = fsd_session->size < sizeof(buf) ?
				fsd_session->size : sizeof(buf);
			if (fsd_session->size - bytes_total < bytes_to_recv)
				bytes_to_recv = fsd_session->size - bytes_total;

			bytes_recv = read_size(*fd_sock, buf, bytes_to_recv);
			CT_DEBUG("[fd=%d] read_size %zd, expected %zd, max possible %zd",
				 *fd_sock, bytes_recv, bytes_to_recv, sizeof(buf));
			if (bytes_recv < 0) {
				CT_ERROR(errno, "recv");
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
				CT_ERROR(errno, "send");
				goto out;
			}
			*bytes_send_total += bytes_send;
		} while (bytes_total != fsd_session->size);
		CT_DEBUG("[fd=%d,fd=%d] total read %zu, total written %zu",
			 *fd_sock, *fd_local, *bytes_recv_total, *bytes_send_total);
	} while (fsd_session->state & FSD_DATA);

out:
	return rc;
}

static void *thread_sock_client(void *arg)
{
	int rc;
	int *fd_sock  = NULL;
	struct fsd_session_t fsd_session;
	char *fpath_local = NULL;
	int fd_local = -1;
	int archive_id = -1;

	fd_sock = (int *)arg;
	memset(&fsd_session, 0, sizeof(struct fsd_session_t));

	/* State 1: Client calls fsd_fconnect(...). */
	rc = fsd_recv(*fd_sock, &fsd_session, FSD_CONNECT);
	CT_DEBUG("[rc=%d,fd=%d] fsd_recv", rc, *fd_sock);
	if (rc) {
		CT_ERROR(rc, "fsd_recv failed");
		goto out;
	}

	char servername[MAX_OPTIONS_LENGTH + 1] = {0};
	uid_t uid = 65534;	/* User: Nobody. */
	gid_t gid = 65534;	/* Group: Nobody. */

	/* Verify node exists in identmap file. */
	rc = identmap_entry(&fsd_session.fsd_login, servername,
			    &archive_id, &uid, &gid);
	CT_DEBUG("[rc=%d] identmap_entry", rc);
	if (rc) {
		CT_ERROR(rc, "identmap_entry");
		goto out;
	}

	/* Verify node has granted permissions on tsm server. */
	struct login_t login;
	struct session_t session;

	memset(&login, 0, sizeof(struct login_t));
	memset(&session, 0, sizeof(struct session_t));
	login_init(&login,
		   servername,
		   fsd_session.fsd_login.node,
		   fsd_session.fsd_login.password,
		   DEFAULT_OWNER,
		   LINUX_PLATFORM,
		   DEFAULT_FSNAME,
		   DEFAULT_FSTYPE);

	pthread_mutex_lock(&tsm_connect_mutex);
	rc = tsm_connect(&login, &session);
	CT_DEBUG("[rc=%d] tsm_connect", rc);
	if (rc) {
		CT_ERROR(rc, "tsm_connect");
		tsm_disconnect(&session);
		pthread_mutex_unlock(&tsm_connect_mutex);
		goto out;
	}
	tsm_disconnect(&session);
	pthread_mutex_unlock(&tsm_connect_mutex);

	do {
		/* State 2: Client calls fsd_fopen(...) or fsd_disconnect(...). */
		rc = fsd_recv(*fd_sock, &fsd_session, (FSD_OPEN | FSD_DISCONNECT));
		CT_DEBUG("[rc=%d,fd=%d] recv_fsd node '%s' fpath '%s'",
			 rc, *fd_sock, fsd_session.fsd_login.node,
			 fsd_session.fsd_info.fpath);
		if (rc) {
			CT_ERROR(rc, "recv_fsd failed");
			goto out;
		}

		if (fsd_session.state & FSD_DISCONNECT)
			goto out;

		rc = init_fsd_local(&fpath_local, &fd_local, &fsd_session);
		if (rc) {
			CT_ERROR(rc, "init_fsd_local");
			goto out;
		}

		size_t bytes_recv_total = 0;
		size_t bytes_send_total = 0;
		double ts = time_now();
		/* State 3: Client calls fsd_fwrite(...) or fsd_fclose(...). */
		rc = recv_fsd_data(fd_sock,
				   &fd_local,
				   &fsd_session,
				   &bytes_recv_total,
				   &bytes_send_total);
		CT_DEBUG("[rc=%d,fd=%d] recv_fsd_data", rc, *fd_sock);
		if (rc) {
			CT_ERROR(rc, "recv_fsd_data failed");
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
			*fd_sock, fd_local,
			fsd_session.fsd_info.fpath,
			bytes_recv_total, time_now() - ts);

		rc = xattr_set_fsd(fpath_local,
				   STATE_FSD_COPY_DONE,
				   archive_id,
				   &fsd_session.fsd_info);
		if (rc)
			goto out;

		/* Producer. */
		struct fsd_action_item_t *fsd_action_item = NULL;

		fsd_action_item = create_fsd_item(bytes_recv_total,
						  &fsd_session.fsd_info,
						  fpath_local, archive_id,
						  uid, gid);
		if (fsd_action_item == NULL)
			goto out;

		rc = enqueue_fsd_item(fsd_action_item);
		if (rc) {
			free(fsd_action_item);
			goto out;
		}

		rc = close(fd_local);
		CT_DEBUG("[rc=%d,fd=%d] close", rc, fd_local);
		if (rc < 0) {
			rc = -errno;
			CT_ERROR(rc, "close");
			goto out;
		}
		fd_local = -1;

		if (fpath_local) {
			free(fpath_local);
			fpath_local = NULL;
		}
	} while (fsd_session.state != FSD_DISCONNECT);

out:
	if (fpath_local) {
		free(fpath_local);
		fpath_local = NULL;
	}

	if (!(fd_local < 0))
		close(fd_local);

	if (fd_sock) {
		close(*fd_sock);
		free(fd_sock);
		fd_sock = NULL;
	}

	pthread_mutex_lock(&mutex_sock_cnt);
	if (thread_sock_cnt > 0)
		thread_sock_cnt--;
	pthread_mutex_unlock(&mutex_sock_cnt);

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

	/* Verify we have a valid Lustre mount point. */
	rc = llapi_search_fsname(opt.o_mnt_lustre, opt.o_mnt_lustre);
	if (rc < 0) {
		CT_ERROR(rc, "cannot find a Lustre filesystem mounted at '%s'",
			 opt.o_mnt_lustre);
	}

	queue_init(&queue, free);

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
			uint32_t fsd_action_state = 0;
			int archive_id = 0;
			char fpath_local[PATH_MAX + 1] = {0};
			struct fsd_info_t fsd_info = {
				.fs		   = {0},
				.fpath		   = {0},
				.desc		   = {0}
			};

			snprintf(fpath_local, PATH_MAX, "%s/%s", dpath, entry->d_name);
			rc = xattr_get_fsd(fpath_local, &fsd_action_state,
					   &archive_id, &fsd_info);
			if (rc)
				CT_ERROR(rc, "xattr_get_fsd '%s', "
					 "file cannot be re-enqueued", fpath_local);
			else {
				struct stat st;
				struct fsd_action_item_t *fsd_action_item = NULL;

				rc = stat(fpath_local, &st);
				if (rc) {
					CT_ERROR(-errno, "stat '%s'", fpath_local);
					break;
				}

				fsd_action_item = create_fsd_item(st.st_size,
								  &fsd_info,
								  fpath_local,
								  archive_id,
								  st.st_uid,
								  st.st_uid);
				if (!fsd_action_item) {
					CT_WARN("create_fsd_item '%s' failed", fpath_local);
					break;
				}
				if (fsd_action_state & STATE_FILE_OMITTED) {
					fsd_action_state = STATE_FSD_COPY_DONE;
				}
				fsd_action_item->fsd_action_state = fsd_action_state;

				rc = enqueue_fsd_item(fsd_action_item);
				if (rc) {
					free(fsd_action_item);
					break;
				}
				CT_INFO("re-enqueue '%s'", fpath_local);
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

static int copy_action(struct fsd_action_item_t *fsd_action_item)
{
	int rc;
	int fd_read = -1;
	int fd_write = -1;
	char fpath_sub[PATH_MAX + 1] = {0};
	uint16_t i = 0;

	fd_read = open(fsd_action_item->fpath_local, O_RDONLY);
	if (fd_read < 0) {
		rc = -errno;
		CT_ERROR(rc, "open '%s'", fsd_action_item->fpath_local);
		return rc;
	}

	while (fsd_action_item->fsd_info.fpath[i] != '\0') {
		if (fsd_action_item->fsd_info.fpath[i] == '/' && i > 0) {
			strncpy(fpath_sub, fsd_action_item->fsd_info.fpath, i);
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
			rc = chown(fpath_sub, fsd_action_item->uid, fsd_action_item->gid);
			if (rc < 0) {
				rc = -errno;
				CT_ERROR(rc, "chown '%s', uid %zu, gid %zu",
					 fpath_sub, fsd_action_item->uid, fsd_action_item->gid);
				goto out;
			}
		}
	next:
		i++;
	}

	fd_write = open(fsd_action_item->fsd_info.fpath,
			O_WRONLY | O_CREAT | O_EXCL, 00664);
	if (fd_write < 0) {
		/* Note, if file exists, then fd_write < 0 and errno EEXISTS is
		   set. This makes sure we do NOT overwrite existing files. To
		   allow overwriting replace O_EXCL with O_TRUNC. */
		rc = -errno;
		CT_ERROR(rc, "open '%s'", fsd_action_item->fsd_info.fpath);
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
	if (statbuf.st_size != fsd_action_item->size) {
		rc = -ERANGE;
		CT_ERROR(rc, "fsd_action_item->size %zu != fstat.st_size %zu",
			 fsd_action_item->size, statbuf.st_size);
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
		fd_read, fsd_action_item->fpath_local,
		fd_write, fsd_action_item->fsd_info.fpath,
		bytes_read_total, time_now() - ts);

	/* Change owner and group. */
	rc = fchown(fd_write, fsd_action_item->uid, fsd_action_item->gid);
	CT_DEBUG("[rc=%d,fd=%d] fchown '%s', uid %zu gid %zu", rc, fd_write,
		 fsd_action_item->fsd_info.fpath,
		 fsd_action_item->uid, fsd_action_item->gid);
	if (rc) {
		rc = -errno;
		CT_ERROR(rc, "fchown '%s', uid %zu, gid %zu",
			 fsd_action_item->fsd_info.fpath,
			 fsd_action_item->uid, fsd_action_item->gid);
	}

out:
	if (fd_read != -1)
		close(fd_read);

	if (fd_write != -1)
		close(fd_write);

	return rc;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
static int archive_state(const struct fsd_action_item_t *fsd_action_item,
			 uint32_t *states)
{
	int rc;
	struct hsm_user_state hus;

	rc = llapi_hsm_state_get(fsd_action_item->fsd_info.fpath, &hus);
	CT_DEBUG("[rc=%d] llapi_hsm_state_get '%s'", rc,
		 fsd_action_item->fsd_info.fpath);
	if (rc) {
		CT_ERROR(rc, "llapi_hsm_state_get '%s'",
			 fsd_action_item->fsd_info.fpath);
		return rc;
	}

	*states = hus.hus_states;

	return rc;
}
#pragma GCC diagnostic pop

static int archive_action(struct fsd_action_item_t *fsd_action_item)
{
	int rc;
	struct hsm_user_request	*hur = NULL;
	struct hsm_user_item	*hui = NULL;
	struct lu_fid		fid;

	rc = llapi_path2fid(fsd_action_item->fsd_info.fpath, &fid);
	CT_DEBUG("[rc=%d] llapi_path2fid '%s' "DFID"",
		 rc, fsd_action_item->fsd_info.fpath,
		 PFID(&fid));
	if (rc) {
		CT_ERROR(rc, "llapi_path2fid '%s'",
			 fsd_action_item->fsd_info.fpath);
		return rc;
	}

	hur = llapi_hsm_user_request_alloc(1, 0);
	if (hur == NULL) {
		rc = -errno;
		CT_ERROR(rc, "llapi_hsm_user_request_alloc failed '%s'",
			 fsd_action_item->fsd_info.fpath);
		return rc;
	}
	hur->hur_request.hr_action = HUA_ARCHIVE;
	hur->hur_request.hr_archive_id = fsd_action_item->archive_id;
	hur->hur_request.hr_flags = 0;
	hur->hur_request.hr_itemcount = 1;
	hur->hur_request.hr_data_len = 0;

	hui = &hur->hur_user_item[0];
	hui->hui_fid = fid;

	rc = llapi_hsm_request(fsd_action_item->fsd_info.fpath, hur);

	free(hur);

	return rc;
}

/*
  The following state diagram is implemented.

  +---------------------+        +-----------------------+
->| STATE_FSD_COPY_DONE +------->+ STATE_LUSTRE_COPY_RUN |
  +--------+------------+        +------------+----------+
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
static int process_fsd_action_item(struct fsd_action_item_t *fsd_action_item)
{
	int rc = 0;

	CT_DEBUG("process_fsd_action_item %p, state '%s', fs '%s', fpath '%s', size %zu, "
		 "errors %d, ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, queue size %lu",
		 fsd_action_item,
		 FSD_ACTION_STR(fsd_action_item->fsd_action_state),
		 fsd_action_item->fsd_info.fs,
		 fsd_action_item->fsd_info.fpath,
		 fsd_action_item->size,
		 fsd_action_item->action_error_cnt,
		 fsd_action_item->ts[0],
		 fsd_action_item->ts[1],
		 fsd_action_item->ts[2],
		 queue_size(&queue));

	if (fsd_action_item->action_error_cnt > opt.o_ntol_file_errors) {
		CT_WARN("file '%s' reached maximum number of tolerated errors, "
			"and is omitted", fsd_action_item->fpath_local);
		rc = xattr_update_fsd_state(fsd_action_item,
					    STATE_FILE_OMITTED);
		free(fsd_action_item);
		return 0;
	}

	switch (fsd_action_item->fsd_action_state) {
	case STATE_FSD_COPY_DONE: {
		rc = xattr_update_fsd_state(fsd_action_item,
					    STATE_LUSTRE_COPY_RUN);
		CT_DEBUG("[rc=%d] setting state from '%s' to '%s'",
			 rc,
			 FSD_ACTION_STR(STATE_FSD_COPY_DONE),
			 FSD_ACTION_STR(STATE_LUSTRE_COPY_RUN));
		if (rc) {
			fsd_action_item->action_error_cnt++;
			fsd_action_item->fsd_action_state = STATE_FSD_COPY_DONE;
			CT_WARN("setting state from '%s' to '%s' failed, "
				"going back to state '%s'",
				FSD_ACTION_STR(STATE_FSD_COPY_DONE),
				FSD_ACTION_STR(STATE_LUSTRE_COPY_RUN),
				FSD_ACTION_STR(fsd_action_item->fsd_action_state));
			break;
		}

		double ts = time_now();
		rc = copy_action(fsd_action_item);
		if (rc) {
			CT_WARN("file '%s' copying to '%s' failed, will "
				"try again",
				fsd_action_item->fpath_local,
				fsd_action_item->fsd_info.fpath);
			fsd_action_item->action_error_cnt++;
			fsd_action_item->fsd_action_state = STATE_LUSTRE_COPY_ERROR;
			break;
		}
		CT_MESSAGE("file '%s' copied to '%s' of size %zu in seconds %.3f",
			   fsd_action_item->fpath_local,
			   fsd_action_item->fsd_info.fpath,
			   fsd_action_item->size,
			   time_now() - ts);

		rc = xattr_update_fsd_state(fsd_action_item,
					      STATE_LUSTRE_COPY_DONE);
		CT_DEBUG("[rc=%d] setting state from '%s' to '%s'",
			 rc,
			 FSD_ACTION_STR(STATE_LUSTRE_COPY_RUN),
			 FSD_ACTION_STR(STATE_LUSTRE_COPY_DONE));
		if (rc) {
			fsd_action_item->action_error_cnt++;
			fsd_action_item->fsd_action_state = STATE_FSD_COPY_DONE;
			CT_WARN("setting state from '%s' to '%s' failed, "
				"going back to state '%s'",
				FSD_ACTION_STR(STATE_FSD_COPY_DONE),
				FSD_ACTION_STR(STATE_LUSTRE_COPY_DONE),
				FSD_ACTION_STR(fsd_action_item->fsd_action_state));
			break;
		}
		fsd_action_item->ts[1] = time_now();
		break;
	}
	case STATE_LUSTRE_COPY_RUN: {
		/* TODO: Update fsd_action_item->progress_size. */
		break;
	}
	case STATE_LUSTRE_COPY_ERROR: {
		CT_WARN("fsd to lustre copy error, try to copy "
			"file '%s' to '%s' again",
			fsd_action_item->fpath_local,
			fsd_action_item->fsd_info.fpath);
		rc = xattr_update_fsd_state(fsd_action_item,
					    STATE_FSD_COPY_DONE);
		if (rc)
			fsd_action_item->action_error_cnt++;
		break;
	}
	case STATE_LUSTRE_COPY_DONE: {
		rc = xattr_update_fsd_state(fsd_action_item,
					    STATE_TSM_ARCHIVE_RUN);
		CT_DEBUG("[rc=%d] setting state from '%s' to '%s'",
			 rc,
			 FSD_ACTION_STR(STATE_LUSTRE_COPY_DONE),
			 FSD_ACTION_STR(STATE_TSM_ARCHIVE_RUN));
		if (rc) {
			fsd_action_item->action_error_cnt++;
			fsd_action_item->fsd_action_state = STATE_LUSTRE_COPY_DONE;
			CT_WARN("setting state from '%s' to '%s' failed, "
				"going back to state '%s'",
				FSD_ACTION_STR(STATE_LUSTRE_COPY_DONE),
				FSD_ACTION_STR(STATE_TSM_ARCHIVE_RUN),
				FSD_ACTION_STR(fsd_action_item->fsd_action_state));
			break;
		}
		rc = archive_action(fsd_action_item);
		if (rc) {
			CT_WARN("file '%s' archiving failed, will try again",
				fsd_action_item->fpath_local);
			fsd_action_item->action_error_cnt++;
			fsd_action_item->fsd_action_state = STATE_TSM_ARCHIVE_ERROR;
		}
		break;
	}
	case STATE_TSM_ARCHIVE_RUN: {
		uint32_t states = 0;

#ifdef LTSMFSD_POLL_ARCHIVE_FINISHED
		/* We stay in STATE_TSM_ARCHIVE_RUN and poll every 50ms to check,
		   whether the archive operation finishes successfully. This approach,
		   however, is not efficient and in addition congests the queue, as the
		   the fsd_action_item is enqueued/dequeued over and over until the file
		   is finally archived. To overcome this problem, we change immediately
		   (via undefined LTSMFSD_POLL_ARCHIVE_FINISHED) the state from
		   STATE_TSM_ARCHIVE_RUN to STATE_TSM_ARCHIVE_DONE and assume:
		   If archive_action(fsd_action_item) returns success, then the file
		   is also successfully archived. To reject this assumption, just define
		   directive LTSMFSD_POLL_ARCHIVE_FINISHED to activate the poll mechanism. */

		/* The archive_state check is based on polling, thus employ
		   sleep(50ms) to avoid high CPU load. */
		nanosleep(&(struct timespec){0, 50000000UL}, NULL);

		rc = archive_state(fsd_action_item, &states);
		CT_DEBUG("[rc=%d] archive_state: %d", states);
		if (rc) {
			/* If state cannot be obtained, stay in state
			   STATE_TSM_ARCHIVE_RUN and try again. */
			fsd_action_item->action_error_cnt++;
			CT_ERROR(rc, "archive state '%s'",
				 fsd_action_item->fsd_info.fpath);
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
			rc = xattr_update_fsd_state(fsd_action_item,
						    STATE_TSM_ARCHIVE_DONE);
			CT_DEBUG("[rc=%d] setting state from '%s' to '%s'",
				 rc,
				 FSD_ACTION_STR(STATE_TSM_ARCHIVE_RUN),
				 FSD_ACTION_STR(STATE_TSM_ARCHIVE_DONE));
			if (rc) {
				fsd_action_item->action_error_cnt++;
				fsd_action_item->fsd_action_state = STATE_LUSTRE_COPY_DONE;
				CT_WARN("setting state from '%s' to '%s' failed, "
					"going back to state '%s'",
					FSD_ACTION_STR(STATE_TSM_ARCHIVE_RUN),
					FSD_ACTION_STR(STATE_TSM_ARCHIVE_DONE),
					FSD_ACTION_STR(fsd_action_item->fsd_action_state));
				break;
			}
			fsd_action_item->ts[2] = time_now();
			CT_MESSAGE("file '%s' of size %zu in queue archived "
				   "in %.3f seconds",
				   fsd_action_item->fpath_local,
				   fsd_action_item->size,
				   fsd_action_item->ts[2] - fsd_action_item->ts[1]);
		}

		/* Stay in state STATE_TSM_ARCHIVE_RUN. */
		break;
	}
	case STATE_TSM_ARCHIVE_ERROR: {
		CT_WARN("tsm archive error, try to archive file '%s' again",
			fsd_action_item->fpath_local);
		rc = xattr_update_fsd_state(fsd_action_item,
					    STATE_LUSTRE_COPY_DONE);
		if (rc)
			fsd_action_item->action_error_cnt++;

		break;
	}
	case STATE_TSM_ARCHIVE_DONE: {
		CT_MESSAGE("file '%s' of size %zu in queue successfully copied "
			   "and archived in %.3f seconds",
			   fsd_action_item->fpath_local,
			   fsd_action_item->size,
			   fsd_action_item->ts[2] - fsd_action_item->ts[1]);
		rc = unlink(fsd_action_item->fpath_local);
		CT_DEBUG("[rc=%d] unlink '%s'", rc, fsd_action_item->fpath_local);
		if (rc < 0) {
			rc = -errno;
			CT_ERROR(rc, "unlink '%s'", fsd_action_item->fpath_local);
			break;
		}
		CT_INFO("unlink '%s' and remove action item %p",
			fsd_action_item->fpath_local, fsd_action_item);
		free(fsd_action_item);
		return 0;
	}
	case STATE_FILE_OMITTED: {
		CT_MESSAGE("file '%s' is omitted and removed from queue",
			   fsd_action_item->fpath_local);
		free(fsd_action_item);
		return 0;
	}
	default:		/* We should never be here. */
		rc = -ERANGE;
		CT_ERROR(rc, "unknown action state");
		return rc;
	}

	rc = enqueue_fsd_item(fsd_action_item);

	return rc;
}

static void *thread_queue_consumer(void *data)
{
	int rc;
	struct fsd_action_item_t *fsd_action_item;

	for (;;) {
		/* Critical region, lock. */
		pthread_mutex_lock(&queue_mutex);

		/* While queue is empty wait for new FSD action item. */
		while (queue_size(&queue) == 0)
			pthread_cond_wait(&queue_cond, &queue_mutex);

		rc = queue_dequeue(&queue, (void **)&fsd_action_item);

		/* Unlock. */
		pthread_mutex_unlock(&queue_mutex);

		if (rc) {
			rc = -EFAILED;
			CT_ERROR(rc, "failed dequeue operation: "
				 "%p, state '%s', fs '%s', fpath '%s', size %zu, "
				 "errors %d, ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, queue size %lu",
				 fsd_action_item,
				 FSD_ACTION_STR(fsd_action_item->fsd_action_state),
				 fsd_action_item->fsd_info.fs,
				 fsd_action_item->fsd_info.fpath,
				 fsd_action_item->size,
				 fsd_action_item->action_error_cnt,
				 fsd_action_item->ts[0],
				 fsd_action_item->ts[1],
				 fsd_action_item->ts[2],
				 queue_size(&queue));
		} else {
			CT_INFO("dequeue operation: "
				"%p, state '%s', fs '%s', fpath '%s', size %zu, "
				"errors %d, ts[0] %.3f, ts[1] %.3f, ts[2] %.3f, queue size %lu",
				fsd_action_item,
				FSD_ACTION_STR(fsd_action_item->fsd_action_state),
				fsd_action_item->fsd_info.fs,
				fsd_action_item->fsd_info.fpath,
				fsd_action_item->size,
				fsd_action_item->action_error_cnt,
				fsd_action_item->ts[0],
				fsd_action_item->ts[1],
				fsd_action_item->ts[2],
				queue_size(&queue));

			rc = process_fsd_action_item(fsd_action_item);
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
			CT_MESSAGE("created queue consumer thread fsd_queue/%d", n);
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
	int sock_fd = -1;
	struct sockaddr_in sockaddr_srv;
	pthread_t *threads_sock = NULL;
	pthread_attr_t attr;
	int reuse = 1;
	struct sigaction sig_act;

	sig_act.sa_handler = signal_handler;
	sig_act.sa_flags = 0;
	sigemptyset(&sig_act.sa_mask);
	sigaction(SIGINT, &sig_act, NULL);
	sigaction(SIGTERM, &sig_act, NULL);

	rc = parseopts(argc, argv);
	if (rc < 0) {
		CT_WARN("try '%s --help' for more information", argv[0]);
		return -rc;	/* Return positive error codes back to shell. */
	}

	rc = fsd_setup();
	if (rc < 0)
		return rc;

	/* Re-enqueue files caused e.g. daemon shutdown. */
	re_enqueue(opt.o_local_mount);

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
	CT_MESSAGE("listening on port %d with %d socket threads, %d queue "
		   "worker threads, local fs '%s' and number of tolerated "
		   "file errors %d",
		   opt.o_port, opt.o_nthreads_sock, opt.o_nthreads_queue,
		   opt.o_local_mount, opt.o_ntol_file_errors);

	/* Initialize socket processing threads. */
	threads_sock = calloc(opt.o_nthreads_sock, sizeof(pthread_t));
	if (threads_sock == NULL) {
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

	rc = start_queue_consumer_threads();
	if (rc != 0)
		goto cleanup_attr;

	while (keep_running) {

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
				CT_ERROR(-errno, "malloc");
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
				CT_MESSAGE("created socket thread 'fsd_sock/%d' for "
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

	if (sock_fd > -1)
		close(sock_fd);

	list_destroy(&ident_list);
	queue_destroy(&queue);

	return rc;
}
