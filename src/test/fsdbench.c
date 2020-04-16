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
 * Copyright (c) 2020, GSI Helmholtz Centre for Heavy Ion Research
 */

#define _GNU_SOURCE

#if HAVE_CONFIG_H
#include <config.h>
#endif

#include <getopt.h>
#include <pthread.h>
#include "fsdapi.h"
#include "common.h"
#include "test_utils.h"
#include "measurement.h"

MSRT_DECLARE(fsd_fwrite);

#define BUF_SIZE		0x100000 /* 1MiB */
#define LEN_FILENAME_RND	32
#define DEFAULT_FPATH_NAME	"/lustre/fsdbench/"
#define DEFAULT_FSSPACE_NAME    "/lustre"

static char			**fpaths       = NULL;
static struct fsd_session_t	 *fsd_sessions = NULL;
static pthread_t		 *threads      = NULL;
static pthread_mutex_t		  mutex;
static uint32_t			  next_idx     = 0;

struct options {
	int		o_verbose;
	uint32_t	o_nfiles;
	size_t		o_filesize;
	uint16_t	o_nthreads;
	uint32_t        o_wdelay;
	char		o_servername[HOST_NAME_MAX + 1];
	char		o_node[DSM_MAX_NODE_LENGTH + 1];
	char		o_password[DSM_MAX_VERIFIER_LENGTH + 1];
	char		o_fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char		o_fpath[PATH_MAX + 1];
};

static struct options opt = {
	.o_verbose	  = API_MSG_NORMAL,
	.o_nfiles	  = 16,
	.o_filesize	  = 16777216,
	.o_nthreads	  = 1,
	.o_wdelay	  = 0,
	.o_servername	  = {0},
	.o_node		  = {0},
	.o_password	  = {0},
	.o_fpath	  = {0},
	.o_fsname	  = {0},
};

static void usage(const char *cmd_name, const int rc)
{
	fprintf(stdout, "usage: %s [options]\n"
		"\t-z, --size <long> [default: %lu bytes]\n"
		"\t-b, --number <int> [default: %u]\n"
		"\t-t, --threads <int> [default: %u]\n"
		"\t-d, --wdelay <int> [default: %u]\n"
		"\t-f, --fsname <string> [default: '%s']\n"
		"\t-a, --fpath <string> [default: '%s']\n"
		"\t-n, --node <string>\n"
		"\t-p, --password <string>\n"
		"\t-s, --servername <string>\n"
		"\t-v, --verbose {error, warn, message, info, debug} [default: message]\n"
		"\t-h, --help\n"
		"version: %s © 2017 by GSI Helmholtz Centre for Heavy Ion Research\n",
		cmd_name,
		opt.o_filesize,
		opt.o_nfiles,
		opt.o_nthreads,
		opt.o_wdelay,
		DEFAULT_FSSPACE_NAME,
		DEFAULT_FPATH_NAME,
		PACKAGE_VERSION);

	exit(rc);
}

static void sanity_arg_check(const char *argv)
{
	/* Required arguments. */
	if (!opt.o_node[0]) {
		fprintf(stdout, "missing argument -n, --node <string>\n\n");
		usage(argv, 1);
	}
	if (!opt.o_password[0]) {
		fprintf(stdout, "missing argument -p, --password <string>\n\n");
		usage(argv, 1);
	}
	if (!opt.o_servername[0]) {
		fprintf(stdout, "missing argument -s, --servername "
			"<string>\n\n");
		usage(argv, 1);
	}
	/* Optional arguments, otherwise use defaults. */
	if (!opt.o_fpath[0])
		strncpy(opt.o_fpath, DEFAULT_FPATH_NAME, PATH_MAX);
	if (!opt.o_fsname[0])
		strncpy(opt.o_fsname, DEFAULT_FSSPACE_NAME, DSM_MAX_FSNAME_LENGTH);
}

static int parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{"size",	required_argument, 0, 'z'},
		{"number",	required_argument, 0, 'b'},
		{"threads",	required_argument, 0, 't'},
		{"wdelay",	required_argument, 0, 'd'},
		{"fsname",	required_argument, 0, 'f'},
		{"fpath",	required_argument, 0, 'a'},
		{"node",	required_argument, 0, 'n'},
		{"password",	required_argument, 0, 'p'},
		{"servername",	required_argument, 0, 's'},
		{"verbose",	required_argument, 0, 'v'},
		{"help",	      no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};
	int c;
	while ((c = getopt_long(argc, argv, "z:b:t:d:f:a:n:p:s:v:h",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'z': {
			opt.o_filesize = atol(optarg);
			break;
		}
		case 'b': {
			opt.o_nfiles = atoi(optarg);
			break;
		}
		case 't': {
			opt.o_nthreads = atoi(optarg);
			break;
		}
		case 'd': {
			opt.o_wdelay = atol(optarg);
			break;
		}
		case 'f': {
			strncpy(opt.o_fsname, optarg, DSM_MAX_FSNAME_LENGTH);
			break;
		}
		case 'a': {
			strncpy(opt.o_fpath, optarg, PATH_MAX);
			break;
		}
		case 'n': {
			strncpy(opt.o_node, optarg, DSM_MAX_NODE_LENGTH);
			break;
		}
		case 'p': {
			strncpy(opt.o_password, optarg,
				DSM_MAX_VERIFIER_LENGTH);
			break;
		}
		case 's': {
			strncpy(opt.o_servername, optarg,
				HOST_NAME_MAX);
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

	sanity_arg_check(argv[0]);

	return 0;
}

static void *perform_task(void *thread_data)
{
	struct fsd_session_t *session;
	char fpath[PATH_MAX + 1] = {0};
	uint8_t *buf = NULL;
	int rc = 0;
	uint32_t crc32sum_buf = 0;

	session = (struct fsd_session_t *)thread_data;

	buf = malloc(sizeof(uint8_t) * opt.o_filesize);
	if (!buf) {
		rc = -errno;
		pthread_exit((void *)&rc);
	}
#ifdef WITH_RAND
	/* Fill buffer each 16th position initially with random data. */
	for (size_t r = 0; r < opt.o_filesize; r += 16)
		buf[r] = (uint8_t)mrand48();
#endif
	while (next_idx < opt.o_nfiles) {
		pthread_mutex_lock(&mutex);
		strncpy(fpath, fpaths[next_idx++], PATH_MAX);
		pthread_mutex_unlock(&mutex);

		/* Pick random position in buf and overwrite
		   at that position. Note this is a layman shuffling. */
		size_t pos = rand() % opt.o_filesize;
		memcpy(buf, buf + pos, opt.o_filesize - pos);

		rc = fsd_fopen(opt.o_fsname, fpath, NULL, session);
		CT_DEBUG("[rc=%d] fsd_fopen '%s' '%s' %p",
			 rc, opt.o_fsname, fpath, session);
		if (rc)
			goto cleanup;
		CT_INFO("[rc=%d] fsd_fopen '%s' '%s' %p",
			rc, opt.o_fsname, fpath, session);

		ssize_t twritten = 0; /* Total written. */
		ssize_t cwritten = 0; /* Current written. */
		size_t buf_size = BUF_SIZE < opt.o_filesize ?
			BUF_SIZE : opt.o_filesize;

		while (twritten < (ssize_t)opt.o_filesize) {
			if (opt.o_filesize - twritten < buf_size)
				buf_size = opt.o_filesize - twritten;
			cwritten = fsd_fwrite(buf, buf_size, 1, session);
			if (cwritten < 0)
				goto cleanup;

			twritten += cwritten;
			CT_DEBUG("fsd_fwrite %lu %lu %lu",
				 buf_size, cwritten, twritten);
			if (opt.o_wdelay) {
				CT_DEBUG("usleep %u", opt.o_wdelay);
				usleep(opt.o_wdelay);
			}
			crc32sum_buf = crc32(crc32sum_buf, buf, cwritten);
		}
		if (twritten != (ssize_t)opt.o_filesize) {
			rc = -EIO;
			goto cleanup;
		}

		rc = fsd_fclose(session);
		CT_DEBUG("[rc=%d] fsd_fclose '%s' '%s' crc32 0x%08x %p",
			 rc, opt.o_fsname, fpath, crc32sum_buf, session);
		if (rc)
			goto cleanup;
		CT_INFO("[rc=%d] fsd_fclose '%s' '%s' crc32 0x%08x %p",
			rc, opt.o_fsname, fpath, crc32sum_buf, session);
		crc32sum_buf = 0;
	}

cleanup:
	if (buf)
		free(buf);

	if (rc)
		pthread_exit((void *)&rc);

	return NULL;
}

static int create_rnd_fnames(void)
{
	int rc;
	char fpath[PATH_MAX + 1] = {0};

	if (fpaths)
		return -EINVAL;

	fpaths = calloc(opt.o_nfiles, sizeof(char *));
	if (!fpaths) {
		CT_ERROR(errno, "malloc");
		return -ENOMEM;
	}

	strncpy(fpath, opt.o_fpath, PATH_MAX + 1);
	if (strlen(fpath) > 1 &&
	    (fpath[strlen(fpath) - 1] != '/'))
		strncat(fpath, "/", PATH_MAX);

	for (uint32_t n = 0; n < opt.o_nfiles; n++) {
		fpaths[n] = calloc(PATH_MAX + 1, sizeof(char));
		if (!fpaths[n]) {
			rc = -ENOMEM;
			goto cleanup;
		}
		strncpy(fpaths[n], fpath, PATH_MAX + 1);

		char rnd_s[LEN_FILENAME_RND + 1] = {0};
		rnd_str(rnd_s, LEN_FILENAME_RND);
		strncat(fpaths[n], rnd_s, PATH_MAX);
	}

	return 0;

cleanup:
	if (fpaths) {
		for (uint32_t n = 0; n < opt.o_nfiles; n++)
			if (fpaths[n])
				free(fpaths[n]);
		free(fpaths);
		fpaths = NULL;
	}

	return rc;
}

static int run_threads(void)
{
	int rc;
	pthread_attr_t attr;
	char thread_name[32] = {0};

	threads = calloc(opt.o_nthreads, sizeof(pthread_t));
	if (threads == NULL) {
		rc = -errno;
		CT_ERROR(rc, "malloc");
		return rc;
	}

	rc = pthread_attr_init(&attr);
	if (rc) {
		CT_ERROR(rc, "pthread_attr_init failed");
		goto cleanup;
	}

	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	for (uint16_t n = 0; n < opt.o_nthreads; n++) {
		rc = pthread_create(&threads[n], &attr, perform_task, &fsd_sessions[n]);

		if (rc)
			CT_WARN("[rc=%d] pthread_create failed thread '%d'", rc, n);
		else {
			snprintf(thread_name, sizeof(thread_name),
				 "fsdbench/%d", n);
			pthread_setname_np(threads[n], thread_name);
			CT_INFO("created thread '%s'", thread_name);
		}
	}

	rc = pthread_attr_destroy(&attr);
	if (rc)
		CT_ERROR(rc, "pthread_attr_destroy");

	void *status = NULL;
	for (uint16_t n = 0; n < opt.o_nthreads; n++) {
		rc = pthread_join(threads[n], &status);
		if (rc)
			CT_WARN("[rc=%d] pthread_join failed thread '%d'", rc, n);
		else
			CT_INFO("[rc=%d] pthread_join thread '%d'", rc, n);
	}

	return rc;

cleanup:
	if (threads) {
		free(threads);
		threads = NULL;
	}

	return rc;
}

int main(int argc, char *argv[])
{
	int rc;
	api_msg_set_level(opt.o_verbose);
	rc = parseopts(argc, argv);
	if (rc) {
		CT_WARN("try '%s --help' for more information", argv[0]);
		return -EINVAL;
	}

	srand(time(NULL));

	rc = create_rnd_fnames();
	if (rc)
		goto cleanup;

	if (opt.o_nthreads > opt.o_nfiles) {
		CT_WARN("number of threads > num of files, reducing number of "
			"threads to '%d'", opt.o_nfiles);
		opt.o_nthreads = opt.o_nfiles;
	}

	fsd_sessions = calloc(opt.o_nthreads, sizeof(struct fsd_session_t));
	if (!fsd_sessions) {
		rc = -ENOMEM;
		CT_ERROR(rc, "calloc");
		goto cleanup;
	}

	struct fsd_login_t fsd_login;

	memset(&fsd_login, 0, sizeof(fsd_login));
	strncpy(fsd_login.node, opt.o_node, DSM_MAX_NODE_LENGTH + 1);
	strncpy(fsd_login.password, opt.o_password, DSM_MAX_VERIFIER_LENGTH + 1);
	strncpy(fsd_login.hostname, opt.o_servername, HOST_NAME_MAX + 1);
	fsd_login.port = 7625;

	/* Each thread connects to a session. */
	for (int n = 0; n < opt.o_nthreads; n++) {
		rc = fsd_fconnect(&fsd_login, &fsd_sessions[n]);
		if (rc)
			goto cleanup;
	}

	pthread_mutex_init(&mutex, NULL);

	MSRT_START(fsd_fwrite);
	MSRT_DATA(fsd_fwrite,
		  (uint64_t)opt.o_nfiles * (uint64_t)opt.o_filesize);

	/* Perform fsd_fopen(...), fsd_fwrite(...) and fsd_fclose(...). */
	rc = run_threads();
	CT_DEBUG("[rc=%d] run_threads", rc);
	if (rc)
		goto cleanup;

	MSRT_STOP(fsd_fwrite);
	MSRT_DISPLAY_RESULT(fsd_fwrite);

cleanup:
	pthread_mutex_destroy(&mutex);

	if (fsd_sessions) {
		for (int n = 0; n < opt.o_nthreads; n++)
			fsd_fdisconnect(&fsd_sessions[n]);
		free(fsd_sessions);
		fsd_sessions = NULL;
	}

	return rc;
}
