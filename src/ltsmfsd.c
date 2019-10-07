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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/xattr.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <lustre/lustreapi.h>
#include "tsmapi.h"
#include "queue.h"

#define PORT_DEFAULT_FSD	7625
#define N_THREADS_SOCK_DEFAULT	4
#define N_THREADS_SOCK_MAX	64
#define N_THREADS_QUEUE_DEFAULT	4
#define N_THREADS_QUEUE_MAX	64
#define BACKLOG			32
#define BUF_SIZE		0xfffff	/* 0xfffff = 1MiB, 0x400000 = 4MiB */

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
	int o_nthreads_sock;
	int o_nthreads_queue;
	int o_daemonize;
	int o_verbose;
};

static struct options opt = {
	.o_local_mount	  = {0},
	.o_file_ident	  = {0},
	.o_port		  = PORT_DEFAULT_FSD,
	.o_nthreads_sock  = N_THREADS_SOCK_DEFAULT,
	.o_nthreads_queue = N_THREADS_QUEUE_DEFAULT,
	.o_daemonize	  = 0,
	.o_verbose	  = API_MSG_NORMAL
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

/* Extended attributes handling. */
static pthread_mutex_t xattr_mutex = PTHREAD_MUTEX_INITIALIZER;

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
		N_THREADS_SOCK_DEFAULT,
		N_THREADS_QUEUE_DEFAULT,
		PACKAGE_VERSION);
	exit(rc);
}

static char *char_ctime(const time_t *t)
{
	static char ctime_buf[32] = {0};

	if (ctime_r(t, ctime_buf))
		ctime_buf[strlen(ctime_buf) - 1] = '\0'; /* Remove '\n' at end. */

	return ctime_buf;
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
		{"localfs",	required_argument, 0,		     'l'},
		{"identmap",	required_argument, 0,		     'i'},
		{"port",	required_argument, 0,		     'p'},
		{"sthreads",	required_argument, 0,		     's'},
		{"qthreads",	required_argument, 0,		     'q'},
		{"daemon",	no_argument,	   &opt.o_daemonize,   1},
		{"verbose",	required_argument, 0,		     'v'},
		{"help",	no_argument,	   0,		     'h'},
		{0, 0, 0, 0}
	};

	int c, rc;
	optind = 0;

	while ((c = getopt_long(argc, argv, "l:i:p:s:q:v:h",
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

static int verify_node(struct login_t *login, uid_t *uid, gid_t *gid)
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
	if (found) {
		CT_INFO("found node '%s' in identmap, using archive_id %d, "
			"uid: %d, guid: %d", node->data,
			ident_map->archive_id, ident_map->uid, ident_map->gid);
		*uid = ident_map->uid;
		*gid = ident_map->gid;
	} else {
		CT_ERROR(0, "identifier mapping for node '%s' not found",
			 login->node);
		rc = -EPERM;
	}

	return rc;
}

static int xattr_set_fsd_desc(const char *fpath_local,
			      const char *desc)
{
	int rc;

	pthread_mutex_lock(&xattr_mutex);
	rc = setxattr(fpath_local, XATTR_FSD_DESC, desc,
		      DSM_MAX_DESCR_LENGTH, 0);
	pthread_mutex_unlock(&xattr_mutex);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "setxattr '%s %s'", fpath_local, XATTR_FSD_DESC);
	}

	return rc;
}

static int xattr_set_fsd_state(const char *fpath_local,
			       const uint32_t fsd_action_state)
{
	int rc;

	pthread_mutex_lock(&xattr_mutex);
	rc = setxattr(fpath_local, XATTR_FSD_FLAGS,
		      (uint32_t *)&fsd_action_state, sizeof(uint32_t), 0);
	pthread_mutex_unlock(&xattr_mutex);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "setxattr '%s %s'", fpath_local, XATTR_FSD_FLAGS);
	}

	return rc;
}


static int xattr_set_fsd_desc_state(const char *fpath_local,
				    const char *desc,
				    const uint32_t fsd_action_state)
{
	int rc;

	rc = xattr_set_fsd_desc(fpath_local, desc);
	if (rc)
		return rc;

	rc = xattr_set_fsd_state(fpath_local, fsd_action_state);

	return rc;
}

static int enqueue_fsd_item(const size_t bytes_recv_total,
			    struct fsd_protocol_t *fsd_protocol,
			    char *fpath_local)
{
	int rc;
	struct fsd_action_item_t *fsd_action_item;

	fsd_action_item = calloc(1, sizeof(struct fsd_action_item_t));
	if (!fsd_action_item) {
		rc = -errno;
		CT_ERROR(rc, "calloc");

		return rc;
	}

	fsd_action_item->fsd_action_state = STATE_FSD_COPY_DONE;
	fsd_action_item->size = bytes_recv_total;
	memcpy(&fsd_action_item->fsd_info, &fsd_protocol->fsd_info,
	       sizeof(struct fsd_info_t));
	fsd_action_item->ts[0] = time(NULL);
	strncpy(fsd_action_item->fpath_local, fpath_local, PATH_MAX);

	/* Lock queue to avoid thread access. */
	pthread_mutex_lock(&queue_mutex);

	/* Enqueue FSD action item in queue. */
	rc = queue_enqueue(&queue, fsd_action_item);

	if (rc) {
		rc = -EFAILED;
		CT_ERROR(rc, "enqueue action failed: state='%s', fs='%s', "
			 "fpath='%s', size=%lu, ts='%s', queue size=%lu",
			 FSD_ACTION_STR(fsd_action_item->fsd_action_state),
			 fsd_action_item->fsd_info.fs,
			 fsd_action_item->fsd_info.fpath,
			 fsd_action_item->size,
			 char_ctime(&fsd_action_item->ts[0]),
			 queue_size(&queue));
		free(fsd_action_item);
	} else {
		CT_INFO("enqueue action: state='%s', fs='%s', fpath='%s', "
			"size=%lu, ts='%s', queue size=%lu",
			FSD_ACTION_STR(fsd_action_item->fsd_action_state),
			fsd_action_item->fsd_info.fs,
			fsd_action_item->fsd_info.fpath,
			fsd_action_item->size,
			char_ctime(&fsd_action_item->ts[0]),
			queue_size(&queue));
	}

	/* Free the lock of the queue. */
	pthread_mutex_unlock(&queue_mutex);

	/* Wakeup sleeping consumer (worker threads). */
	pthread_cond_signal(&queue_cond);

	return rc;
}

static int init_fsd_local(char **fpath_local, int *fd_local,
			  const uid_t uid, const gid_t gid,
			  const struct fsd_protocol_t *fsd_protocol)
{
	int rc;
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	rc = extract_hl_ll(fsd_protocol->fsd_info.fpath, fsd_protocol->fsd_info.fs,
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

	/* Change owner and group. */
	rc = fchown(*fd_local, uid, gid);
	CT_DEBUG("[rc=%d,fd=%d] fchown uid %zu gid %zu", rc, *fd_local,
		 uid, gid);
	if (rc) {
		rc = -errno;
		CT_ERROR(rc, "fchown uid %zu gid %zu", uid, gid);
	}

	return rc;
}

static int recv_fsd_data(int *fd_sock, int *fd_local,
			 struct fsd_protocol_t *fsd_protocol,
			 size_t *bytes_recv_total, size_t *bytes_send_total)
{
	int rc;
	uint8_t buf[BUF_SIZE];
	ssize_t bytes_recv, bytes_to_recv;
	ssize_t bytes_send;

	do {
		rc = recv_fsd_protocol(*fd_sock, fsd_protocol, (FSD_DATA | FSD_CLOSE));
		CT_DEBUG("[rc=%d,fd=%d] recv_fsd_protocol, size %zu", rc, *fd_sock,
			 fsd_protocol->size);
		if (rc) {
			CT_ERROR(rc, "recv_fsd_protocol failed");
			goto out;
		}

		if (fsd_protocol->state & FSD_CLOSE)
			goto out;

		size_t bytes_total = 0;
		bytes_recv = bytes_send = 0;
		memset(buf, 0, sizeof(buf));
		do {
			bytes_to_recv = fsd_protocol->size < sizeof(buf) ?
				fsd_protocol->size : sizeof(buf);
			if (fsd_protocol->size - bytes_total < bytes_to_recv)
				bytes_to_recv = fsd_protocol->size - bytes_total;

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
		} while (bytes_total != fsd_protocol->size);
		CT_DEBUG("[fd=%d,fd=%d] total read %zu, total written %zu",
			 *fd_sock, *fd_local, *bytes_recv_total, *bytes_send_total);
	} while (fsd_protocol->state & FSD_DATA);

out:
	return rc;
}

static void *thread_handle_client(void *arg)
{
	int rc;
	int *fd_sock  = NULL;
	struct fsd_protocol_t fsd_protocol;

	fd_sock = (int *)arg;
	memset(&fsd_protocol, 0, sizeof(struct fsd_protocol_t));

	/* State 1: Client calls fsd_tsm_fconnect(...). */
	rc = recv_fsd_protocol(*fd_sock, &fsd_protocol, FSD_CONNECT);
	CT_DEBUG("[rc=%d,fd=%d] recv_fsd_protocol", rc, *fd_sock);
	if (rc) {
		CT_ERROR(rc, "recv_fsd_protocol failed");
		goto out;
	}

	uid_t uid = 65534;	/* User: Nobody. */
	gid_t gid = 65534;	/* Group: Nobody. */

	rc = verify_node(&fsd_protocol.login, &uid, &gid);
	CT_DEBUG("[rc=%d] verify_node", rc);
	if (rc) {
		CT_ERROR(rc, "verify_node");
		goto out;
	}

	char *fpath_local = NULL;
	int fd_local = -1;
	do {
		/* State 2: Client calls fsd_tsm_fopen(...) or fsd_tsm_disconnect(...). */
		rc = recv_fsd_protocol(*fd_sock, &fsd_protocol, (FSD_OPEN | FSD_DISCONNECT));
		CT_DEBUG("[rc=%d,fd=%d] recv_fsd_protocol", rc, *fd_sock);
		if (rc) {
			CT_ERROR(rc, "recv_fsd_protocol failed");
			goto out;
		}

		if (fsd_protocol.state & FSD_DISCONNECT)
			goto out;

		CT_INFO("[fd=%d] recv_fsd_protocol node: '%s' with fpath: '%s'",
			*fd_sock, fsd_protocol.login.node, fsd_protocol.fsd_info.fpath);

		rc = init_fsd_local(&fpath_local, &fd_local, uid, gid, &fsd_protocol);
		if (rc) {
			CT_ERROR(rc, "init_fsd_local");
			goto out;
		}

		size_t  bytes_recv_total = 0;
		size_t bytes_send_total = 0;
		/* State 3: Client calls fsd_tsm_fwrite(...) or fsd_tsm_fclose(...). */
		rc = recv_fsd_data(fd_sock,
				   &fd_local,
				   &fsd_protocol,
				   &bytes_recv_total,
				   &bytes_send_total);

		if (bytes_recv_total != bytes_send_total) {
			rc = -EFAILED;
			CT_ERROR(rc, "total number of bytes recv and send "
				 "differs, recv: %lu and send: %lu",
				 bytes_recv_total, bytes_send_total);
			goto out;
		}
		CT_INFO("[fd=%d,fd=%d] data buffer of size: %zd successfully "
			"received and sent", *fd_sock, fd_local, bytes_recv_total);

		rc = xattr_set_fsd_desc_state(fpath_local,
					      fsd_protocol.fsd_info.desc,
					      STATE_FSD_COPY_DONE);
		if (rc)
			goto out;

		/* Producer. */
		rc = enqueue_fsd_item(bytes_recv_total, &fsd_protocol, fpath_local);
		if (rc)
			goto out;

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
	} while (fsd_protocol.state != FSD_DISCONNECT);

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
#if 0   /* TODO: Deactivated for testing. */
	/* Verify we have a valid Lustre mount point. */
	rc = llapi_search_fsname(opt.o_mnt_lustre, opt.o_mnt_lustre);
	if (rc < 0) {
		CT_ERROR(rc, "cannot find a Lustre filesystem mounted at '%s'",
			 opt.o_mnt_lustre);
	}
#endif

	queue_init(&queue, free);

	return rc;
}

static void signal_handler(int signal)
{
	keep_running = false;
}

static int write_to_lustre(struct fsd_action_item_t *fsd_action_item)
{
	int fd_read = -1;
	int fd_write = -1;
	int rc;

	fd_read = open(fsd_action_item->fpath_local, O_RDONLY);
	if (fd_read < 0) {
		rc = -errno;
		CT_ERROR(rc, "open '%s'", fsd_action_item->fpath_local);
		return rc;
	}
	fd_write = open(fsd_action_item->fsd_info.fpath, O_WRONLY | O_CREAT | O_TRUNC, 00664);
	if (fd_write < 0) {
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
		"size: %zd successfully read and written",
		fd_read, fsd_action_item->fpath_local,
		fd_write, fsd_action_item->fsd_info.fpath,
		bytes_read_total);

out:
	if (fd_read != -1)
		close(fd_read);

	if (fd_write != -1)
		close(fd_write);

	return rc;
}

static int process_fsd_action_item(struct fsd_action_item_t *fsd_action_item)
{
	/* TODO: Free memory fsd_action_item. */
	int rc;

	switch (fsd_action_item->fsd_action_state) {
	case STATE_FSD_COPY_DONE: {

		fsd_action_item->fsd_action_state = STATE_LUSTRE_COPY_RUN;
		rc = xattr_set_fsd_state(fsd_action_item->fpath_local,
					 fsd_action_item->fsd_action_state);
		if (rc)
			break;

		rc = write_to_lustre(fsd_action_item);
		if (rc) {
			CT_WARN("file '%s' writing to '%s' failed, will "
				"try again",
				fsd_action_item->fpath_local,
				fsd_action_item->fsd_info.fpath);
			fsd_action_item->fsd_action_state = STATE_LUSTRE_COPY_ERROR;
		} else {
			CT_MESSAGE("file '%s' writing to '%s' was successful",
				   fsd_action_item->fpath_local,
				   fsd_action_item->fsd_info.fpath);
			fsd_action_item->fsd_action_state = STATE_LUSTRE_COPY_DONE;
		}
		fsd_action_item->ts[1] = time(NULL);
		rc = xattr_set_fsd_state(fsd_action_item->fpath_local,
					 fsd_action_item->fsd_action_state);
		break;
	}
	case STATE_LUSTRE_COPY_RUN: {
		rc = -ENOSYS;
		break;
	}
	case STATE_LUSTRE_COPY_ERROR: {
		rc = -ENOSYS;
		break;
	}
	case STATE_LUSTRE_COPY_DONE: {
		rc = -ENOSYS;
		break;
	}
	case STATE_TSM_COPY_RUN: {
		rc = -ENOSYS;
		break;
	}
	case STATE_TSM_COPY_ERROR: {
		rc = -ENOSYS;
		break;
	}
	case STATE_TSM_COPY_DONE: {
		rc = -ENOSYS;
		break;
	}
	default:
		rc = -ERANGE;
		break;
	}

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
			CT_ERROR(rc, "queue_dequeue");
		} else {
			CT_INFO("dequeue action: state='%s', fs='%s', fpath='%s', "
				"size=%lu, ts='%s', queue size=%lu",
				FSD_ACTION_STR(fsd_action_item->fsd_action_state),
				fsd_action_item->fsd_info.fs,
				fsd_action_item->fsd_info.fpath,
				fsd_action_item->size,
				char_ctime(&fsd_action_item->ts[0]),
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
	char thread_queue_name[16];

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
		else {
			snprintf(thread_queue_name, sizeof(thread_queue_name),
				 "fsd_queue/%d", n);
			pthread_setname_np(threads_queue[n], thread_queue_name);
			CT_MESSAGE("created queue consumer thread '%s'",
				   thread_queue_name);
		}
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
	char thread_sock_name[16] = {0};
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
	CT_MESSAGE("listening on port: %d with %d serving socket threads",
		   opt.o_port, opt.o_nthreads_sock);

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
				CT_WARN("maximum number of serving "
					"socket threads %d exceeded",
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
					    thread_handle_client, fd_sock);
			if (rc != 0)
				CT_ERROR(rc, "cannot create thread for "
					 "client '%s'",
					 inet_ntoa(sockaddr_cli.sin_addr));
			else {
				snprintf(thread_sock_name, sizeof(thread_sock_name),
					 "fsd_sock/%d", thread_sock_cnt);
				pthread_setname_np(threads_sock[thread_sock_cnt],
						   thread_sock_name);
				CT_MESSAGE("created socket thread '%s' for "
					   "client '%s' and fd %d",
					   thread_sock_name,
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
