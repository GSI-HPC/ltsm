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
 * Copyright (c) 2016, 2017, Thomas Stibor <t.stibor@gsi.de>
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <pthread.h>
#include <stdint.h>
#include <linux/limits.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <lustre/lustre_idl.h>
#include <lustre/lustreapi.h>
#include "tsmapi.h"
#include "queue.h"

struct options {
	int o_daemonize;
	int o_dry_run;
	int o_nthreads;
	int o_verbose;
	int o_abort_on_err;
        int o_archive_cnt;
        int o_archive_id[LL_HSM_MAX_ARCHIVE];
	char *o_mnt;
	int o_mnt_fd;
	char o_servername[DSM_MAX_SERVERNAME_LENGTH + 1];
	char o_node[DSM_MAX_NODE_LENGTH + 1];
	char o_owner[DSM_MAX_OWNER_LENGTH + 1];
	char o_password[DSM_MAX_VERIFIER_LENGTH + 1];
	char o_fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char o_fstype[DSM_MAX_FSTYPE_LENGTH + 1];
};

struct options opt = {
	.o_verbose = API_MSG_NORMAL,
	.o_servername = {0},
	.o_node = {0},
	.o_owner = {0},
	.o_password = {0},
	.o_fsname = {0},
	.o_fstype = {0},
};

static uint16_t nthreads = 1;
static pthread_t **thread = NULL;
static pthread_mutex_t queue_mutex;
static pthread_cond_t queue_cond;
static struct session_t **session = NULL;
static queue_t queue;

static int err_major;
static int err_minor;

static char fs_name[MAX_OBD_NAME + 1] = {0};
static struct hsm_copytool_private *ctdata = NULL;

static void usage(const char *cmd_name, const int rc)
{
	dsmApiVersionEx libapi_ver = get_libapi_ver();
	dsmAppVersion appapi_ver = get_appapi_ver();

	fprintf(stdout, "usage: %s [options] <lustre_mount_point>\n"
		"\t-a, --archive-id <int> [default: 1]\n"
		"\t\t""archive id number\n"
		"\t-t, --threads <int>\n"
		"\t\t""number of processing threads [default: 2]\n"
		"\t-n, --node <string>\n"
		"\t\t""node name registered on tsm server\n"
		"\t-p, --password <string>\n"
		"\t\t""password of tsm node/owner\n"
		"\t-o, --owner <string>\n"
		"\t\t""owner of tsm node\n"
		"\t-s, --servername <string>\n"
		"\t\t""hostname of tsm server\n"
		"\t-f, --fsname <string>\n"
		"\t\t""filespace name on tsm server [default: '/']\n"
		"\t-v, --verbose {error, warn, message, info, debug}"
		" [default: message]\n"
		"\t\t""produce more verbose output\n"
		"\t--abort-on-error\n"
		"\t\t""abort operation on major error\n"
		"\t--daemon\n"
		"\t\t""daemon mode run in background\n"
		"\t--dry-run\n"
		"\t\t""don't run, just show what would be done\n"
		"\t-h, --help\n"
		"\t\t""show this help\n"
		"\nIBM API library version: %d.%d.%d.%d, "
		"IBM API application client version: %d.%d.%d.%d\n"
		"version: %s © 2017 by Thomas Stibor <t.stibor@gsi.de>,"
		" Jörg Behrendt <j.behrendt@gsi.de>\n",
		cmd_name,
		libapi_ver.version, libapi_ver.release, libapi_ver.level,
		libapi_ver.subLevel,
		appapi_ver.applicationVersion, appapi_ver.applicationRelease,
		appapi_ver.applicationLevel, appapi_ver.applicationSubLevel,
		PACKAGE_VERSION);
	exit(rc);
}

static void sanity_arg_check(const struct options *opts, const char *argv)
{
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
	} else if (!strlen(opt.o_fsname)) {
		strncpy(opt.o_fsname, DEFAULT_FSNAME, strlen(DEFAULT_FSNAME));
	}
}

static int ct_parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{"abort-on-error", no_argument,       &opt.o_abort_on_err,   1},
		{"archive-id",	   required_argument, 0,	           'a'},
		{"daemon",	   no_argument,       &opt.o_daemonize,      1},
		{"threads",        required_argument, 0,	           't'},
		{"node",           required_argument, 0,                   'n'},
		{"password",       required_argument, 0,                   'p'},
		{"owner",          required_argument, 0,                   'o'},
		{"servername",     required_argument, 0,                   's'},
		{"fsname",         required_argument, 0,                   'f'},
		{"verbose",        required_argument, 0,                   'v'},
		{"dry-run",	   no_argument,	      &opt.o_dry_run,        1},
		{"help",           no_argument,       0,		   'h'},
		{0, 0, 0, 0}
	};

	int c, rc;
	optind = 0;

	while ((c = getopt_long(argc, argv, "a:t:n:p:o:s:f:v:h",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'a': {
                        if ((opt.o_archive_cnt >= LL_HSM_MAX_ARCHIVE) ||
                            (atoi(optarg) >= LL_HSM_MAX_ARCHIVE)) {
                                rc = -E2BIG;
                                CT_ERROR(rc, "archive number must be less"
                                         "than %zu", LL_HSM_MAX_ARCHIVE);
                                return rc;
                        }
                        opt.o_archive_id[opt.o_archive_cnt] = atoi(optarg);
                        opt.o_archive_cnt++;
                        break;
                }
		case 't': {
			nthreads = atoi(optarg);
			break;
		}
		case 'n': {
			strncpy(opt.o_node, optarg,
				strlen(optarg) < DSM_MAX_NODE_LENGTH ?
				strlen(optarg) : DSM_MAX_NODE_LENGTH);
			break;
		}
		case 'p': {
			strncpy(opt.o_password, optarg,
				strlen(optarg) < DSM_MAX_VERIFIER_LENGTH ?
				strlen(optarg) : DSM_MAX_VERIFIER_LENGTH);
			break;
		}
		case 'o': {
			strncpy(opt.o_owner, optarg,
				strlen(optarg) < DSM_MAX_OWNER_LENGTH ?
				strlen(optarg) : DSM_MAX_OWNER_LENGTH);
			break;
		}
		case 's': {
			strncpy(opt.o_servername, optarg,
				strlen(optarg) < DSM_MAX_SERVERNAME_LENGTH ?
				strlen(optarg) : DSM_MAX_SERVERNAME_LENGTH);
			break;
		}
		case 'f': {
			strncpy(opt.o_fsname, optarg,
				strlen(optarg) < DSM_MAX_FSNAME_LENGTH ?
				strlen(optarg) : DSM_MAX_FSNAME_LENGTH);
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
		CT_ERROR(rc, "no mount point specified");
		return rc;
	}

	opt.o_mnt = argv[optind];
	opt.o_mnt_fd = -1;

	return 0;
}

static int progress_callback(struct progress_size_t *pg_size,
			     struct session_t *session)
{
	int rc;

	session->hai->hai_extent.length = pg_size->cur;
	session->hai->hai_extent.offset = pg_size->cur_total - pg_size->cur;
	rc = llapi_hsm_action_progress(session->hcp, &session->hai->hai_extent,
				       pg_size->total, 0);
	if (rc)
		CT_ERROR(rc, "llapi_hsm_action_progress failed");

	return rc;
}

static int fid_realpath(const char *mnt, const lustre_fid *fid,
			char *resolved_path, size_t resolved_path_len)
{
	int rc;
	int linkno = 0;
	long long recno = -1;
	char file[PATH_MAX];
	char strfid[FID_NOBRACE_LEN + 1];

	snprintf(strfid, sizeof(strfid), DFID_NOBRACE, PFID(fid));

	rc = llapi_fid2path(mnt, strfid, file, sizeof(file),
			    &recno, &linkno);
	if (rc < 0)
		return rc;

	/* fid2path returns a relative path */
	rc = snprintf(resolved_path, resolved_path_len, "%s/%s", mnt, file);
	if (rc >= resolved_path_len)
		return -ENAMETOOLONG;

	return rc;
}

static int ct_finish(struct session_t *session, int ct_rc, char *fpath)
{
	int rc;

	CT_MESSAGE("action completed, notifying coordinator "
		   "cookie=%#jx, FID="DFID", err=%d",
		   (uintmax_t)session->hai->hai_cookie,
		   PFID(&session->hai->hai_fid),
		   -ct_rc);

	if (session->hcp == NULL) {
		rc = llapi_hsm_action_begin(&session->hcp, ctdata, session->hai,
					    -1, 0, true);
		CT_DEBUG("[rc=%d] llapi_hsm_action_begin() on '%s'", rc, fpath);
		if (rc < 0) {
			CT_ERROR(rc, "llapi_hsm_action_begin() on '%s' failed",
				 fpath);
			return rc;
		}
	}
	rc = llapi_hsm_action_end(&session->hcp, &session->hai->hai_extent, 0,
				  abs(ct_rc));
	if (rc == -ECANCELED)
		CT_ERROR(rc, "completed action on '%s' has been canceled: "
			 "cookie=%#jx, FID="DFID, fpath,
			 (uintmax_t)session->hai->hai_cookie,
			 PFID(&session->hai->hai_fid));
	else if (rc < 0)
		CT_ERROR(rc, "llapi_hsm_action_end() on '%s' failed", fpath);
	else
		CT_DEBUG("[rc=%d] llapi_hsm_action_end() on '%s' ok", rc,
			 fpath);

	return rc;
}

static int ct_archive(struct session_t *session)
{
	char fpath[PATH_MAX + 1] = {0};
	int rc;
	int src_fd = -1;

	rc = fid_realpath(opt.o_mnt, &session->hai->hai_fid, fpath, sizeof(fpath));
	if (rc < 0) {
		CT_ERROR(rc, "fid_realpath failed");
		goto cleanup;
	}

	rc = llapi_hsm_action_begin(&session->hcp, ctdata, session->hai, -1, 0, false);
	CT_DEBUG("[rc=%d] llapi_hsm_action_begin() on '%s'", rc, fpath);
	if (rc < 0) {
		CT_ERROR(rc, "llapi_hsm_action_begin on '%s' failed", fpath);
		goto cleanup;
	}
	CT_MESSAGE("archiving '%s' to TSM storage", fpath);

	if (opt.o_dry_run) {
		CT_MESSAGE("running in dry-run mode, skipping effective"
			   " archiving TSM operation");
		rc = 0;
		goto cleanup;
	}

	src_fd = llapi_hsm_action_get_fd(session->hcp);
	CT_DEBUG("[fd=%d] llapi_hsm_action_get_fd()", src_fd);
	if (src_fd < 0) {
		rc = src_fd;
		CT_ERROR(rc, "cannot open '%s' for read", fpath);
		goto cleanup;
	}

	rc = tsm_archive_fpath(DEFAULT_FSNAME, fpath, NULL, -1,
			       (const void *)&session->hai->hai_fid, session);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_archive_fpath_fid on '%s' failed", fpath);
		goto cleanup;
	}
	CT_MESSAGE("archiving '%s' to TSM storage done", fpath);

cleanup:
	if (!(src_fd < 0))
		close(src_fd);

	rc = ct_finish(session, rc, fpath);

	return rc;
}

static int ct_restore(struct session_t *session)
{
	int rc;
	int dst_fd = -1;
	int mdt_index = -1;
	int open_flags = 0;
	char fpath[PATH_MAX + 1] = {0};
	struct lu_fid dfid;

	rc = fid_realpath(opt.o_mnt, &session->hai->hai_fid, fpath,
			  sizeof(fpath));
	if (rc < 0) {
		CT_ERROR(rc, "fid_realpath failed");
		return rc;
	}

	rc = llapi_get_mdt_index_by_fid(opt.o_mnt_fd, &session->hai->hai_fid,
					&mdt_index);
	if (rc < 0) {
		CT_ERROR(rc, "cannot get mdt index "DFID"",
			 PFID(&session->hai->hai_fid));
		return rc;
	}

	rc = llapi_hsm_action_begin(&session->hcp, ctdata, session->hai,
				    mdt_index, open_flags, false);
	if (rc < 0) {
		CT_ERROR(rc, "llapi_hsm_action_begin on '%s' failed", fpath);
		return rc;
	}

	rc = llapi_hsm_action_get_dfid(session->hcp, &dfid);
	if (rc < 0) {
		CT_ERROR(rc, "restoring "DFID
			 ", cannot get FID of created volatile file",
			 PFID(&session->hai->hai_fid));
		goto cleanup;
	}
	CT_MESSAGE("restoring data from TSM storage to '%s'", fpath);

	if (opt.o_dry_run) {
		CT_MESSAGE("running in dry-run mode, skipping effective"
			   " restoring TSM operation");
		rc = 0;
		goto cleanup;
	}

	dst_fd = llapi_hsm_action_get_fd(session->hcp);
	if (dst_fd < 0) {
		rc = dst_fd;
		CT_ERROR(rc, "cannot open '%s' for write", fpath);
		goto cleanup;
	}

	rc = tsm_retrieve_fpath(DEFAULT_FSNAME, fpath, NULL, dst_fd, session);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_retrieve_fpath on '%s' failed", fpath);
		goto cleanup;
	}
	CT_MESSAGE("data restore from TSM storage to '%s' done", fpath);

cleanup:
	rc = ct_finish(session, rc, fpath);

	if (!(dst_fd < 0))
		close(dst_fd);

	return rc;
}

static int ct_remove(struct session_t *session)
{
	char fpath[PATH_MAX + 1] = {0};
	int rc;

	rc = fid_realpath(opt.o_mnt, &session->hai->hai_fid, fpath, sizeof(fpath));
	if (rc < 0) {
		CT_ERROR(rc, "fid_realpath()");
		goto cleanup;
	}

	rc = llapi_hsm_action_begin(&session->hcp, ctdata, session->hai, -1, 0, false);
	if (rc < 0) {
		CT_ERROR(rc, "llapi_hsm_action_begin() on '%s' failed", fpath);
		goto cleanup;
	}
	CT_MESSAGE("removing from TSM storage file '%s'", fpath);

	if (opt.o_dry_run) {
		CT_MESSAGE("running in dry-run mode, skipping effective"
			   " removing TSM operation");
		rc = 0;
		goto cleanup;
	}
	rc = tsm_delete_fpath(DEFAULT_FSNAME, fpath, session);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_delete_fpath on '%s' failed", fpath);
		goto cleanup;
	}

cleanup:
	rc = ct_finish(session, rc, fpath);

	return rc;
}

static int ct_process_item(struct session_t *session)
{
	int rc = 0;

	if (opt.o_verbose >= API_MSG_NORMAL) {

		char fid[128];
		char path[PATH_MAX];
		long long recno = -1;
		int linkno = 0;

		sprintf(fid, DFID, PFID(&session->hai->hai_fid));
		CT_MESSAGE("'%s' action %s reclen %d, cookie=%#jx",
			   fid,
			   hsm_copytool_action2name(session->hai->hai_action),
			   session->hai->hai_len,
			   (uintmax_t)session->hai->hai_cookie);
		rc = llapi_fid2path(opt.o_mnt, fid, path,
				    sizeof(path), &recno, &linkno);
		if (rc < 0)
			CT_ERROR(rc, "cannot get path of FID %s", fid);
	}

	switch (session->hai->hai_action) {
		/* set err_major, minor inside these functions */
	case HSMA_ARCHIVE:
		rc = ct_archive(session);
		break;
	case HSMA_RESTORE:
		rc = ct_restore(session);
		break;
	case HSMA_REMOVE:
		rc = ct_remove(session);
		break;
	default:
		rc = -EINVAL;
		CT_ERROR(rc, "unknown action %d, on '%s'", session->hai->hai_action,
			 opt.o_mnt);
		err_minor++;
		ct_finish(session, rc, NULL);
	}
	free(session->hai);

	return 0;
}

static void *ct_thread(void *data)
{
	struct session_t *session = data;
	int rc;

	for (;;) {
		/* Crticial region, lock. */
		pthread_mutex_lock(&queue_mutex);
		while (queue_size(&queue) == 0)
			pthread_cond_wait(&queue_cond, &queue_mutex);

		rc = queue_dequeue(&queue, (void **)&session->hai);
		CT_DEBUG("dequeue action '%s' cookie=%#jx, FID="DFID"",
			 hsm_copytool_action2name(session->hai->hai_action),
			 (uintmax_t)session->hai->hai_cookie,
			 PFID(&session->hai->hai_fid));
		if (rc)
			CT_ERROR(ECANCELED, "dequeue action '%s'"
				 "cookie=%#jx, FID="DFID" failed",
				 hsm_copytool_action2name(session->hai->hai_action),
				 (uintmax_t)session->hai->hai_cookie,
				 PFID(&session->hai->hai_fid));
		pthread_mutex_unlock(&queue_mutex);
		/* Unlock. */

		rc = ct_process_item(session);
	}

	pthread_exit((void *)(intptr_t)rc);
}

static void handler(int signal)
{
	psignal(signal, "exiting");

	/* TODO: Cleanup TSM sessions */

	/* If we don't clean up upon interrupt, umount thinks there's a ref
	 * and doesn't remove us from mtab (EINPROGRESS). The lustre client
	 * does successfully unmount and the mount is actually gone, but the
	 * mtab entry remains. So this just makes mtab happier. */
	llapi_hsm_copytool_unregister(&ctdata);

	_exit(1);
}

/* Daemon waits for messages from the kernel; run it in the background. */
static int ct_run(void)
{
	struct sigaction cleanup_sigaction;
	int rc;

	if (opt.o_daemonize) {
		rc = daemon(1, 1);
		if (rc < 0) {
			rc = -errno;
			CT_ERROR(rc, "cannot daemonize");
			return rc;
		}
	}

	setbuf(stdout, NULL);

	rc = llapi_hsm_copytool_register(&ctdata, opt.o_mnt,
					 opt.o_archive_cnt,
					 opt.o_archive_id, 0);
	if (rc < 0) {
		CT_ERROR(rc, "cannot start copytool interface");
		return rc;
	}

	memset(&cleanup_sigaction, 0, sizeof(cleanup_sigaction));
	cleanup_sigaction.sa_handler = handler;
	sigemptyset(&cleanup_sigaction.sa_mask);
	sigaction(SIGINT, &cleanup_sigaction, NULL);
	sigaction(SIGTERM, &cleanup_sigaction, NULL);

	while (1) {
		struct hsm_action_list *hal;
		struct hsm_action_item *hai;
		int msgsize;
		int i = 0;

		CT_MESSAGE("waiting for message from kernel");

		rc = llapi_hsm_copytool_recv(ctdata, &hal, &msgsize);
		if (rc == -ESHUTDOWN) {
			CT_MESSAGE("shutting down");
			break;
		} else if (rc < 0) {
			CT_WARN("cannot receive action list: %s",
				strerror(-rc));
			err_major++;
			if (opt.o_abort_on_err)
				break;
			else
				continue;
		}

		CT_MESSAGE("copytool fs=%s archive#=%d item_count=%d",
			   hal->hal_fsname, hal->hal_archive_id, hal->hal_count);

		if (strcmp(hal->hal_fsname, fs_name) != 0) {
			rc = -EINVAL;
			CT_ERROR(rc, "'%s' invalid fs name, expecting: %s",
				 hal->hal_fsname, fs_name);
			err_major++;
			if (opt.o_abort_on_err)
				break;
			else
				continue;
		}

		hai = hai_first(hal);
		while (++i <= hal->hal_count) {
			if ((char *)hai - (char *)hal > msgsize) {
				rc = -EPROTO;
				CT_ERROR(rc,
					 "'%s' item %d past end of message!",
					 opt.o_mnt, i);
				err_major++;
				break;
			}

			struct hsm_action_item *work_hai;
			work_hai = malloc(sizeof(struct hsm_action_item));
			if (work_hai == NULL) {
				CT_ERROR(errno, "malloc failed");
				break;
			}
			memcpy(work_hai, hai, sizeof(struct hsm_action_item));

			/* Lock queue to avoid thread access. */
			pthread_mutex_lock(&queue_mutex);

			/* Insert hsm action into queue. */
			rc = queue_enqueue(&queue, work_hai);
			CT_MESSAGE("enqueue action '%s' cookie=%#jx, FID="DFID"",
				   hsm_copytool_action2name(work_hai->hai_action),
				   (uintmax_t)work_hai->hai_cookie,
				   PFID(&work_hai->hai_fid));
			if (rc) {
				rc = -ECANCELED;
				CT_ERROR(rc, "enqueue action '%s'"
					 "cookie=%#jx, FID="DFID" failed",
					 hsm_copytool_action2name(work_hai->hai_action),
					 (uintmax_t)work_hai->hai_cookie,
					 PFID(&work_hai->hai_fid));
				err_major++;
				if (opt.o_abort_on_err)
					break;
			}

			/* Free the lock of the queue. */
			pthread_mutex_unlock(&queue_mutex);

			/* Signal a thread that it should check for new work. */
			pthread_cond_signal(&queue_cond);

			hai = hai_next(hai);
		}
		if (opt.o_abort_on_err && err_major)
			break;
	}

	llapi_hsm_copytool_unregister(&ctdata);

	return rc;
}

static int ct_connect_sessions(void)
{
	int rc;
	struct login_t login;
	uint16_t n;

	rc = tsm_init(DSM_MULTITHREAD);
	if (rc) {
		rc = -ECANCELED;
		CT_ERROR(rc, "tsm_init failed");
		return rc;
	}

	bzero(&login, sizeof(login));
	login_fill(&login, opt.o_servername,
		   opt.o_node, opt.o_password,
		   opt.o_owner, LINUX_PLATFORM,
		   opt.o_fsname, DEFAULT_FSTYPE);

	session = calloc(nthreads, sizeof(struct session_t *));
	if (session == NULL) {
		rc = -errno;
		CT_ERROR(rc, "malloc failed");
		return rc;
	}

	for (n = 0; n < nthreads; n++) {
		session[n] = calloc(1, sizeof(struct session_t));
		if (session[n] == NULL) {
			rc = -errno;
			CT_ERROR(rc, "malloc failed");
			goto cleanup;
		}
		session[n]->progress = progress_callback;
		CT_MESSAGE("tsm_init: session: %d", n + 1);

		rc = tsm_connect(&login, session[n]);
		if (rc) {
			rc = -ECANCELED;
			CT_ERROR(rc, "tsm_init failed");
			goto cleanup;
		}
		/* Querying session is optional. */
		rc = tsm_query_session(session[n]);
		if (rc) {
			rc = -ECANCELED;
			CT_ERROR(rc, "tsm_query_session failed");
			goto cleanup;
		}
	}
	return rc;

cleanup:
	for (uint16_t i = 0; i < n; i++) {
		if (session[i]) {
			tsm_disconnect(session[i]);
			free(session[i]);
		}
	}
	free(session);
	tsm_cleanup(DSM_MULTITHREAD);

	return rc;
}

static int ct_start_threads(void)
{
	int rc;
	uint16_t n;
	pthread_attr_t attr;

	thread = calloc(nthreads, sizeof(pthread_t *));
	if (thread == NULL) {
		rc = -errno;
		CT_ERROR(rc, "malloc failed");
		return rc;
	}
	for (n = 0; n < nthreads; n++) {
		thread[n] = calloc(1, sizeof(pthread_t));
		if (thread[n] == NULL) {
			rc = -errno;
			CT_ERROR(rc, "malloc failed");
			goto cleanup;
		}
	}

	rc = pthread_attr_init(&attr);
	if (rc != 0) {
		CT_ERROR(rc, "pthread_attr_init failed");
		return rc;
	}

	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	for (n = 0; n < nthreads; n++) {
		rc = pthread_create(thread[n], &attr, ct_thread, session[n]);
		if (rc != 0)
			CT_ERROR(rc, "cannot create worker thread '%d' for"
				 "'%s'", n, opt.o_mnt);
	}
	pthread_attr_destroy(&attr);

	return rc;

cleanup:
	for (uint16_t i = 0; i < n; i++) {
		if (thread[i])
			free(thread[i]);
	}
	free(thread);

	return rc;
}

static int ct_setup(void)
{
	int rc;

	rc = llapi_search_fsname(opt.o_mnt, fs_name);
	if (rc < 0) {
		CT_ERROR(rc, "cannot find a Lustre filesystem mounted at '%s'",
			 opt.o_mnt);
		return rc;
	}

	opt.o_mnt_fd = open(opt.o_mnt, O_RDONLY);
	if (opt.o_mnt_fd < 0) {
		rc = -errno;
		CT_ERROR(rc, "cannot open mount point at '%s'",
			 opt.o_mnt);
		return rc;
	}

	pthread_mutex_init(&queue_mutex, NULL);
	pthread_cond_init(&queue_cond, NULL);
	queue_init(&queue, free);

	/* Create nthreads sessions to TSM server. */
	rc = ct_connect_sessions();
	if (rc) {
		CT_ERROR(rc, "ct_connect_sessions failed");
		return rc;
	}

	rc = ct_start_threads();
	if (rc) {
		CT_ERROR(rc, "ct_start_threads failed");
		return rc;
	}

	return rc;
}

static int ct_cleanup(void)
{
	int rc;
	int rc_minor;

	if (opt.o_mnt_fd >= 0) {
		rc = close(opt.o_mnt_fd);
		if (rc < 0) {
			rc = -errno;
			CT_ERROR(rc, "cannot close mount point");
		}
	}
	rc_minor = pthread_mutex_destroy(&queue_mutex);
	if (rc_minor)
		CT_ERROR(rc_minor, "pthread_mutex_destroy failed");
	rc_minor = pthread_cond_destroy(&queue_cond);
	if (rc_minor)
		CT_ERROR(rc_minor, "pthread_cond_destroy failed");

	for (uint16_t n = 0; n < nthreads && session && thread; n++) {
		if (session[n]) {
			tsm_disconnect(session[n]);
			free(session[n]);
		}
		if (thread[n])
			free(thread[n]);
	}
	if (session)
		free(session);
	if (thread)
		free(thread);
	tsm_cleanup(DSM_MULTITHREAD);

	return rc;
}

int main(int argc, char *argv[])
{
	int rc;

	rc = ct_parseopts(argc, argv);
	if (rc < 0) {
		CT_WARN("try '%s --help' for more information", argv[0]);
		return -rc;
	}

	rc = ct_setup();
	if (rc < 0)
		goto error_cleanup;

	rc = ct_run();
	CT_MESSAGE("process finished, rc=%d (%s)", rc, strerror(-rc));

error_cleanup:
	ct_cleanup();

	return -rc;
}
