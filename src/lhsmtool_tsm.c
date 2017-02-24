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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Copyright (c) 2016, Thomas Stibor <t.stibor@gsi.de>
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
	int o_verbose;
	int o_abort_on_error;
	int o_archive_cnt;
	int o_archive_id[LL_HSM_MAX_ARCHIVE];
	char *o_mnt;
	char *o_event_fifo;
	int o_mnt_fd;
	char o_servername[DSM_MAX_SERVERNAME_LENGTH + 1];
	char o_node[DSM_MAX_NODE_LENGTH + 1];
	char o_owner[DSM_MAX_OWNER_LENGTH + 1];
	char o_password[DSM_MAX_VERIFIER_LENGTH + 1];
	char o_fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char o_fstype[DSM_MAX_FSTYPE_LENGTH + 1];
};

struct options opt = {
	.o_verbose = LLAPI_MSG_INFO,
	.o_servername = {0},
	.o_node = {0},
	.o_owner = {0},
	.o_password = {0},
	.o_fsname = {0},
	.o_fstype = {0},
};

static uint16_t N_THREADS = 2;
static pthread_t **thread = NULL;
static pthread_mutex_t queue_mutex;
static pthread_cond_t queue_cond;
static session_t **session = NULL;
static queue_t queue;

static int err_major;
static int err_minor;

static char cmd_name[PATH_MAX] = {0};
static char fs_name[MAX_OBD_NAME + 1] = {0};

static struct hsm_copytool_private *ctdata = NULL;

static void usage(int rc)
{
	fprintf(stdout, "Usage: %s [options]... <lustre_mount_point>\n"
		"\t--abort-on-error\t\tAbort operation on major error\n"
		"\t-A, --archive <#>\t\tArchive number (repeatable)\n"
		"\t--daemon\t\t\tDaemon mode, run in background\n"
		"\t--dry-run\t\t\tDon't run, just show what would be done\n"
		"\t-f, --event-fifo <path>\t\tWrite events stream to fifo\n"
		"\t-n, --node <string>\t\tNode registered on TSM server\n"
		"\t-p, --password <string>\t\tPassword of TSM node/owner\n"
		"\t-s, --servername <string>\tHostname of TSM server\n"
		"\t-u, --owner <string>\t\tOwner of TSM node\n"
		"\t-v, --verbose\t\t\tProduce more verbose output\n",
		cmd_name);
	exit(rc);
}

static int ct_parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{"abort-on-error", no_argument, &opt.o_abort_on_error, 1},
		{"abort_on_error", no_argument, &opt.o_abort_on_error, 1},
		{"archive",	   required_argument, NULL,	     'A'},
		{"daemon",	   no_argument, &opt.o_daemonize,      1},
		{"event-fifo",	   required_argument, NULL,	     'f'},
		{"event_fifo",	   required_argument, NULL,	     'f'},
		{"dry-run",	   no_argument,	      &opt.o_dry_run, 1},
		{"help",           no_argument, NULL,		     'h'},
		{"node",           no_argument, NULL,                'n'},
		{"password",       no_argument, NULL,                'p'},
		{"quiet",          no_argument, NULL,                'q'},
		{"servername",     no_argument, NULL,                's'},
		{"owner",          no_argument, NULL,                'o'},
		{"verbose",        no_argument, NULL,                'v'},
		{0, 0, 0, 0}
	};

	int c, rc;
	optind = 0;

	while ((c = getopt_long(argc, argv, "A:f:hn:p:qs:o:v",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'A': {
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
		case 'f': {
			opt.o_event_fifo = optarg;
			break;
		}
		case 'h': {
			usage(0);
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
		case 'q': {
			opt.o_verbose--;
			break;
		}
		case 's': {
			strncpy(opt.o_servername, optarg,
				strlen(optarg) < DSM_MAX_SERVERNAME_LENGTH ?
				strlen(optarg) : DSM_MAX_SERVERNAME_LENGTH);
			break;
		}
		case 'o': {
			strncpy(opt.o_owner, optarg,
				strlen(optarg) < DSM_MAX_OWNER_LENGTH ?
				strlen(optarg) : DSM_MAX_OWNER_LENGTH);
			break;
		}
		case 'v': {
			opt.o_verbose++;
			break;
		}
		case 0: {
			break;
		}
		default:
			return -EINVAL;
		}
	}

	if (argc != optind + 1) {
		rc = -EINVAL;
		CT_ERROR(rc, "no mount point specified");
		return rc;
	}

	opt.o_mnt = argv[optind];
	opt.o_mnt_fd = -1;

	return 0;
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

static int ct_finish(session_t *session, int ct_rc, char *fpath)
{
	int rc;

	CT_TRACE("Action completed, notifying coordinator "
		 "cookie=%#jx, FID="DFID", err=%d",
		 (uintmax_t)session->hai->hai_cookie, PFID(&session->hai->hai_fid),
		 -ct_rc);

	if (session->hcp == NULL) {
		rc = llapi_hsm_action_begin(&session->hcp, ctdata, session->hai,
					    -1, 0, true);
		if (rc < 0) {
			CT_ERROR(rc, "llapi_hsm_action_begin() on '%s' failed",
				 fpath);
			return rc;
		}
	}
	rc = llapi_hsm_action_end(&session->hcp, &session->hai->hai_extent, 0, abs(ct_rc));
	if (rc == -ECANCELED)
		CT_ERROR(rc, "completed action on '%s' has been canceled: "
			 "cookie=%#jx, FID="DFID, fpath,
			 (uintmax_t)session->hai->hai_cookie, PFID(&session->hai->hai_fid));
	else if (rc < 0)
		CT_ERROR(rc, "llapi_hsm_action_end() on '%s' failed", fpath);
	else
		CT_TRACE("llapi_hsm_action_end() on '%s' ok (rc=%d)",
			 fpath, rc);

	return rc;
}

static int ct_archive(session_t *session)
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
	if (rc < 0) {
		CT_ERROR(rc, "llapi_hsm_action_begin on '%s' failed", fpath);
		goto cleanup;
	}

	CT_TRACE("archiving '%s' to TSM storage", fpath);

	if (opt.o_dry_run) {
		rc = 0;
		goto cleanup;
	}

	src_fd = llapi_hsm_action_get_fd(session->hcp);
	if (src_fd < 0) {
		rc = src_fd;
		CT_ERROR(rc, "cannot open '%s' for read", fpath);
		goto cleanup;
	}

	rc = tsm_archive_fpath(FSNAME, fpath, NULL, -1,
			       (const void *)&session->hai->hai_fid, session);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_archive_fpath_fid on '%s' failed", fpath);
		goto cleanup;
	}

	CT_TRACE("archiving '%s' to TSM storage done", fpath);

cleanup:
	if (!(src_fd < 0))
		close(src_fd);

	rc = ct_finish(session, rc, fpath);

	return rc;
}

static int ct_restore(session_t *session)
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

	rc = llapi_hsm_action_begin(&session->hcp, ctdata, session->hai, mdt_index, open_flags,
				    false);
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

	CT_TRACE("restoring data from TSM storage to '%s'", fpath);
	if (opt.o_dry_run) {
	    rc = 0;
	    goto cleanup;
	}

	dst_fd = llapi_hsm_action_get_fd(session->hcp);
	if (dst_fd < 0) {
		rc = dst_fd;
		CT_ERROR(rc, "cannot open '%s' for write", fpath);
		goto cleanup;
	}

	rc = tsm_retrieve_fpath(FSNAME, fpath, NULL, dst_fd, session);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_retrieve_fpath on '%s' failed", fpath);
		goto cleanup;
	}
	CT_TRACE("data restore from TSM storage to '%s' done", fpath);

cleanup:
	rc = ct_finish(session, rc, fpath);

	if (!(dst_fd < 0))
		close(dst_fd);

	return rc;
}

static int ct_remove(session_t *session)
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

	CT_TRACE("removing from TSM storage file '%s'", fpath);

	if (opt.o_dry_run) {
		rc = 0;
		goto cleanup;
	}
	rc = tsm_delete_fpath(FSNAME, fpath, session);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_delete_fpath on '%s' failed", fpath);
		goto cleanup;
	}

cleanup:
	rc = ct_finish(session, rc, fpath);

	return rc;
}

static int ct_process_item(session_t *session)
{
	int rc = 0;

	if (opt.o_verbose >= LLAPI_MSG_INFO || opt.o_dry_run) {
		/* Print the original path */
		char fid[128];
		char path[PATH_MAX];
		long long recno = -1;
		int linkno = 0;

		sprintf(fid, DFID, PFID(&session->hai->hai_fid));
		CT_TRACE("'%s' action %s reclen %d, cookie=%#jx",
			 fid, hsm_copytool_action2name(session->hai->hai_action),
			 session->hai->hai_len,
			 (uintmax_t)session->hai->hai_cookie);
		rc = llapi_fid2path(opt.o_mnt, fid, path,
				    sizeof(path), &recno, &linkno);
		if (rc < 0)
			CT_ERROR(rc, "cannot get path of FID %s", fid);
		else
			CT_TRACE("processing file '%s'", path);
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
	session_t *session = data;
	int rc;

	for (;;) {
		/* Crticial region, lock. */
		pthread_mutex_lock(&queue_mutex);
		while (queue_size(&queue) == 0)
			pthread_cond_wait(&queue_cond, &queue_mutex);

		rc = queue_dequeue(&queue, (void **)&session->hai);
		CT_TRACE("dequeue action '%s' cookie=%#jx, FID="DFID"",
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
	/* If we don't clean up upon interrupt, umount thinks there's a ref
	 * and doesn't remove us from mtab (EINPROGRESS). The lustre client
	 * does successfully unmount and the mount is actually gone, but the
	 * mtab entry remains. So this just makes mtab happier. */
	llapi_hsm_copytool_unregister(&ctdata);

	/* Also remove fifo upon signal as during normal/error exit */
	if (opt.o_event_fifo != NULL)
		llapi_hsm_unregister_event_fifo(opt.o_event_fifo);

	/* TODO: Cleanup TSM session */
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

	if (opt.o_event_fifo != NULL) {
		rc = llapi_hsm_register_event_fifo(opt.o_event_fifo);
		if (rc < 0) {
			CT_ERROR(rc, "failed to register event fifo");
			return rc;
		}
		llapi_error_callback_set(llapi_hsm_log_error);
	}

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

		CT_TRACE("waiting for message from kernel");

		rc = llapi_hsm_copytool_recv(ctdata, &hal, &msgsize);
		if (rc == -ESHUTDOWN) {
			CT_TRACE("shutting down");
			break;
		} else if (rc < 0) {
			CT_WARN("cannot receive action list: %s",
				strerror(-rc));
			err_major++;
			if (opt.o_abort_on_error)
				break;
			else
				continue;
		}

		CT_TRACE("copytool fs=%s archive#=%d item_count=%d",
			 hal->hal_fsname, hal->hal_archive_id, hal->hal_count);

		if (strcmp(hal->hal_fsname, fs_name) != 0) {
			rc = -EINVAL;
			CT_ERROR(rc, "'%s' invalid fs name, expecting: %s",
				 hal->hal_fsname, fs_name);
			err_major++;
			if (opt.o_abort_on_error)
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
			CT_TRACE("enqueue action '%s' cookie=%#jx, FID="DFID"",
				 hsm_copytool_action2name(work_hai->hai_action),
				 (uintmax_t)work_hai->hai_cookie,
				 PFID(&work_hai->hai_fid));
			if (rc) {
				CT_ERROR(ECANCELED, "enqueue action '%s'"
					 "cookie=%#jx, FID="DFID" failed",
				 hsm_copytool_action2name(work_hai->hai_action),
				 (uintmax_t)work_hai->hai_cookie,
				 PFID(&work_hai->hai_fid));
			}

			/* Free the lock of the queue. */
			pthread_mutex_unlock(&queue_mutex);

			/* Signal a thread that it should check for new work. */
			pthread_cond_signal(&queue_cond);

			hai = hai_next(hai);
		}

		if (opt.o_abort_on_error && err_major)
			break;
	}

	llapi_hsm_copytool_unregister(&ctdata);
	if (opt.o_event_fifo != NULL)
		llapi_hsm_unregister_event_fifo(opt.o_event_fifo);

	return rc;
}

static int ct_connect_sessions(void)
{
	int rc;
	login_t login;
	uint16_t n;

	rc = tsm_init(DSM_MULTITHREAD);

	bzero(&login, sizeof(login));
	login_fill(&login, opt.o_servername,
		   opt.o_node, opt.o_password,
		   opt.o_owner, LOGIN_PLATFORM,
		   FSNAME, FSTYPE); /* TODO: opt.o_fsname, opt.o_fstype */

	session = calloc(N_THREADS, sizeof(session_t *));
	if (session == NULL) {
		rc = errno;
		CT_ERROR(rc, "malloc failed");
		return rc;
	}

	/* session = calloc(N_THREADS, sizeof(session_t)); */
	for (n = 0; n < N_THREADS; n++) {
		session[n] = calloc(1, sizeof(session_t));
		if (session[n] == NULL) {
			rc = errno;
			CT_ERROR(rc, "malloc failed");
			goto cleanup;
		}
		session[n]->id = n;
		session[n]->ctdata = &ctdata;

        CT_TRACE("tsm_init: session[%d], session[%d]->%d",
                 n, session[n]->id);

		rc = tsm_connect(&login, session[n]);
		if (rc) {
			rc = ECANCELED;
			CT_ERROR(rc, "tsm_init failed");
			goto cleanup;
		}
		/* Querying session is optional. */
		rc = tsm_query_session(session[n]);
		if (rc) {
			rc = ECANCELED;
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

	thread = calloc(N_THREADS, sizeof(pthread_t *));
	if (thread == NULL) {
		rc = errno;
		CT_ERROR(rc, "malloc failed");
		return rc;
	}
	for (n = 0; n < N_THREADS; n++) {
		thread[n] = calloc(1, sizeof(pthread_t));
		if (thread[n] == NULL) {
			rc = errno;
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

	for (n = 0; n < N_THREADS; n++) {
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

	llapi_msg_set_level(opt.o_verbose);

	rc = llapi_search_fsname(opt.o_mnt, fs_name);
	if (rc < 0) {
		CT_ERROR(rc, "can find a Lustre filesystem mounted at '%s'",
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

	/* Create N_THREADS sessions to TSM server. */
	rc = ct_connect_sessions();
	if (rc) {
		rc = ECANCELED;
		CT_ERROR(rc, "ct_connect_sessions failed");
		return rc;
	}

	rc = ct_start_threads();
	if (rc) {
		rc = ECANCELED;
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

	for (uint16_t n = 0; n < N_THREADS; n++) {
		if (session[n]) {
			tsm_disconnect(session[n]);
			free(session[n]);
		}
		if (thread[n])
			free(thread[n]);
	}
	free(session);
	free(thread);
	tsm_cleanup(DSM_MULTITHREAD);

	return rc;
}

int main(int argc, char *argv[])
{
	int rc;

	strncpy(cmd_name, basename(argv[0]), sizeof(cmd_name));
	rc = ct_parseopts(argc, argv);
	if (rc < 0) {
		CT_WARN("try '%s --help' for more information", cmd_name);
		return -rc;
	}

	rc = ct_setup();
	if (rc < 0)
		goto error_cleanup;

	rc = ct_run();
	CT_TRACE("process finished, rc=%d (%s)", rc, strerror(-rc));

error_cleanup:
	ct_cleanup();

	return -rc;
}
