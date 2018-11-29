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
 * Copyright (c) 2017, GSI Helmholtz Centre for Heavy Ion Research
 */

#if HAVE_CONFIG_H
#include <config.h>
#endif

#include <getopt.h>
#include <pthread.h>
#include "tsmapi.h"
#include "test_utils.h"
#include "measurement.h"

#define LEN_FILENAME_RND 32

MSRT_DECLARE(tsm_archive_fpath);
MSRT_DECLARE(tsm_retrieve_fpath);

static char **fpaths = NULL;
static struct session_t *sessions = NULL;
static pthread_t *threads = NULL;
static pthread_mutex_t mutex;
static uint16_t next_idx = 0;
static enum task_e {ARCHIVE, RETRIEVE} task;

struct options {
	int o_verbose;
	int o_nfiles;
	size_t o_filesize;
	int o_nthreads;
	char o_servername[DSM_MAX_SERVERNAME_LENGTH + 1];
	char o_node[DSM_MAX_NODE_LENGTH + 1];
	char o_password[DSM_MAX_VERIFIER_LENGTH + 1];
	char o_fsname[DSM_MAX_FSNAME_LENGTH + 1];
};

struct options opt = {
	.o_verbose = API_MSG_NORMAL,
	.o_nfiles = 16,
	.o_filesize = 16777216,
	.o_nthreads = 1,
	.o_servername = {0},
	.o_node = {0},
	.o_password = {0},
	.o_fsname = {0},
};

static void usage(const char *cmd_name, const int rc)
{
	dsmApiVersionEx libapi_ver = get_libapi_ver();
	dsmAppVersion appapi_ver = get_appapi_ver();

	fprintf(stdout, "usage: %s [options]\n"
		"\t-z, --size <long> [default: 16777216 bytes]\n"
		"\t-b, --number <int> [default: 16]\n"
		"\t-t, --threads <int> [default: 1]\n"
		"\t-f, --fsname <string> [default: '/']\n"
		"\t-n, --node <string>\n"
		"\t-p, --password <string>\n"
		"\t-s, --servername <string>\n"
		"\t-v, --verbose {error, warn, message, info, debug} [default: message]\n"
		"\t-h, --help\n"
		"\nIBM API library version: %d.%d.%d.%d, "
		"IBM API application client version: %d.%d.%d.%d\n"
		"version: %s © 2017 by GSI Helmholtz Centre for Heavy Ion Research\n",
		cmd_name,
		libapi_ver.version, libapi_ver.release, libapi_ver.level,
		libapi_ver.subLevel,
		appapi_ver.applicationVersion, appapi_ver.applicationRelease,
		appapi_ver.applicationLevel, appapi_ver.applicationSubLevel,
		PACKAGE_VERSION);
	exit(rc);
}

static void sanity_arg_check(const char *argv)
{
	/* Required arguments. */
	if (!strlen(opt.o_node)) {
		fprintf(stdout, "missing argument -n, --node <string>\n\n");
		usage(argv, 1);
	} else if (!strlen(opt.o_password)) {
		fprintf(stdout, "missing argument -p, --password <string>\n\n");
		usage(argv, 1);
	} else if (!strlen(opt.o_servername)) {
		fprintf(stdout, "missing argument -s, --servername "
			"<string>\n\n");
		usage(argv, 1);
	} else if (!strlen(opt.o_fsname))
		strncpy(opt.o_fsname, DEFAULT_FSNAME, DSM_MAX_FSNAME_LENGTH);
}

static int parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{"size",	required_argument, 0, 'z'},
		{"number",	required_argument, 0, 'b'},
		{"threads",	required_argument, 0, 't'},
		{"fsname",	required_argument, 0, 'f'},
		{"node",	required_argument, 0, 'n'},
		{"password",	required_argument, 0, 'p'},
		{"servername",	required_argument, 0, 's'},
		{"verbose",	required_argument, 0, 'v'},
		{"help",	      no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};
	int c;
	while ((c = getopt_long(argc, argv, "z:b:t:f:n:p:s:v:h",
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
		case 'f': {
			strncpy(opt.o_fsname, optarg, DSM_MAX_FSNAME_LENGTH);
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
				DSM_MAX_SERVERNAME_LENGTH);
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
					"--verbose '%s'\n", optarg);
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
	struct session_t *session = (struct session_t *)thread_data;;
	char _fpath[6 + LEN_FILENAME_RND + 1] = {0};

	int rc = 0;

	do {
		pthread_mutex_lock(&mutex);
		strncpy(_fpath, fpaths[next_idx++], 5 + LEN_FILENAME_RND + 1);
		pthread_mutex_unlock(&mutex);

		if (task == ARCHIVE)
			rc = tsm_archive_fpath(DEFAULT_FSNAME, _fpath,
					       NULL, -1, NULL,
					       session);
		else
			rc = tsm_retrieve_fpath(DEFAULT_FSNAME, _fpath,
						NULL, -1, session);

		if (rc)
			pthread_exit((void *)&rc);
	} while (next_idx < opt.o_nfiles);

	return NULL;
}

static int create_rnd_fnames(void)
{
	int rc = 0;

	if (fpaths)
		return -EINVAL;

	fpaths = calloc(opt.o_nfiles, sizeof(char *));
	if (!fpaths) {
		CT_ERROR(errno, "malloc");
		return -ENOMEM;
	}
	for (int n = 0; n < opt.o_nfiles; n++) {
		fpaths[n] = calloc(5 + LEN_FILENAME_RND + 1, sizeof(char));
		if (!fpaths[n]) {
			rc = -ENOMEM;
			goto cleanup;
		}

		char rnd_s[LEN_FILENAME_RND + 1] = {0};
		rnd_str(rnd_s, LEN_FILENAME_RND);
		snprintf(fpaths[n], 5 + LEN_FILENAME_RND + 1, "/tmp/%s", rnd_s);
	}

	return rc;
cleanup:
	if (fpaths) {
		for (int n = 0; n < opt.o_nfiles; n++)
			if (fpaths[n])
				free(fpaths[n]);
		free(fpaths);
		fpaths = NULL;
	}

	return rc;
}

static int fill_fnames(void)
{
	int rc = 0;
	FILE *file = NULL;
	unsigned char *buf = NULL;

	buf = malloc(sizeof(unsigned char) * opt.o_filesize);
	if (!buf) {
		rc = -ENOMEM;
		CT_ERROR(rc, "malloc");
		return rc;
	}

	for (size_t r = 0; r < opt.o_filesize; r++)
		buf[r] = rand() % 256;

	for (int n = 0; n < opt.o_nfiles; n++) {
		file = fopen(fpaths[n], "w+");
		if (!file) {
			rc = -errno;
			CT_ERROR(rc, "fopen '%s'", fpaths[n]);
			goto cleanup;
		}

		size_t written = 0;
		written = fwrite(buf, 1, opt.o_filesize, file);
		if (written != opt.o_filesize) {
			rc = -EIO;
			CT_ERROR(rc, "fwrite '%s'", fpaths[n]);
			fclose(file);
			goto cleanup;
		}
		fclose(file);
	}

cleanup:
	if (buf) {
		free(buf);
		buf = NULL;
	}

	return rc;
}

static int remove_fnames(void)
{
	int rc = 0;

	for (int n = 0; n < opt.o_nfiles; n++) {
		rc = unlink(fpaths[n]);
		if (rc)
			CT_WARN("[rc=%d] unlink '%s'", rc, fpaths[n]);
	}

	if (fpaths) {
		for (int n = 0; n < opt.o_nfiles; n++)
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

	for (int n = 0; n < opt.o_nthreads; n++) {
		rc = pthread_create(&threads[n], &attr, perform_task, &sessions[n]);

		if (rc)
			CT_WARN("[rc=%d] pthread_create failed thread '%d'", rc, n);
		else {
			snprintf(thread_name, sizeof(thread_name),
				 "ltsmbench/%d", n);
			pthread_setname_np(threads[n], thread_name);
		}
	}

	rc = pthread_attr_destroy(&attr);
	if (rc)
		CT_ERROR(rc, "pthread_attr_destroy");

	void *status = NULL;
	for (int n = 0; n < opt.o_nthreads; n++) {
		rc = pthread_join(threads[n], &status);
		if (rc)
			CT_WARN("[rc=%d] pthread_join failed thread '%d'", rc, n);
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

	rc = fill_fnames();
	if (rc)
		goto cleanup;

	if (opt.o_nthreads > opt.o_nfiles) {
		CT_WARN("number of threads > num of files, reduce number of"
			"threads to '%d'", opt.o_nfiles);
		opt.o_nthreads = opt.o_nfiles;
	}

	sessions = calloc(opt.o_nthreads, sizeof(struct session_t));
	if (!sessions) {
		rc = -ENOMEM;
		CT_ERROR(rc, "calloc");
		goto cleanup;
	}

	rc = tsm_init(DSM_MULTITHREAD);
	if (rc)
		goto cleanup;

	struct login_t login;
	login_fill(&login, opt.o_servername,
		   opt.o_node, opt.o_password,
		   NULL, LINUX_PLATFORM,
		   opt.o_fsname, DEFAULT_FSTYPE);

	for (int n = 0; n < opt.o_nthreads; n++) {
		rc = tsm_fconnect(&login, &sessions[n]);
		if (rc)
			goto cleanup;
	}

	pthread_mutex_init(&mutex, NULL);

	task = ARCHIVE;
	/* Run archive threads and measure threaded archive performance. */
	MSRT_START(tsm_archive_fpath);
	MSRT_DATA(tsm_archive_fpath, opt.o_nfiles * opt.o_filesize);
	rc = run_threads();
	if (rc)
		goto cleanup;
	MSRT_STOP(tsm_archive_fpath);
	MSRT_DISPLAY_RESULT(tsm_archive_fpath);

	next_idx = 0;
	task = RETRIEVE;
	/* Run retrieve threads and measure threaded retrieve performance. */
	MSRT_START(tsm_retrieve_fpath);
	MSRT_DATA(tsm_retrieve_fpath, opt.o_nfiles * opt.o_filesize);
	rc = run_threads();
	if (rc)
		goto cleanup;
	MSRT_STOP(tsm_retrieve_fpath);
	MSRT_DISPLAY_RESULT(tsm_retrieve_fpath);

cleanup:
	pthread_mutex_destroy(&mutex);

	if (sessions) {
		for (int n = 0; n < opt.o_nfiles; n++) {
			rc = tsm_delete_fpath(DEFAULT_FSNAME, fpaths[n], &sessions[0]);
			if (rc)
				CT_WARN("[rc=%d] tsm_delete_fpath '%s'", rc, fpaths[n]);
		}

		for (int n = 0; n < opt.o_nthreads; n++)
			tsm_disconnect(&sessions[n]);
		free(sessions);
		sessions = NULL;
	}
	tsm_cleanup(DSM_MULTITHREAD);

	int rc_minor;
	rc_minor = remove_fnames();

	return (rc ? rc : rc_minor);
}
