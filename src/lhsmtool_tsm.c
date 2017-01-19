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
#include <linux/limits.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <lustre/lustre_idl.h>
#include <lustre/lustreapi.h>
#include "tsmapi.h"

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
};

struct options opt = {
	.o_verbose = LLAPI_MSG_INFO,
	.o_servername = {0},
	.o_node = {0},
	.o_owner = {0},
	.o_password = {0},
};

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

static int ct_finish(struct hsm_copyaction_private **phcp,
		     const struct hsm_action_item *hai, int hp_flags,
		     int ct_rc, char *fpath)
{
	struct hsm_copyaction_private *hcp;
	int rc;

	CT_TRACE("Action completed, notifying coordinator "
		 "cookie=%#jx, FID="DFID", hp_flags=%d err=%d",
		 (uintmax_t)hai->hai_cookie, PFID(&hai->hai_fid),
		 hp_flags, -ct_rc);

	if (phcp == NULL || *phcp == NULL) {
		rc = llapi_hsm_action_begin(&hcp, ctdata, hai, -1, 0, true);
		if (rc < 0) {
			CT_ERROR(rc, "llapi_hsm_action_begin() on '%s' failed",
				 fpath);
			return rc;
		}
		phcp = &hcp;
	}

	rc = llapi_hsm_action_end(phcp, &hai->hai_extent, hp_flags, abs(ct_rc));
	if (rc == -ECANCELED)
		CT_ERROR(rc, "completed action on '%s' has been canceled: "
			 "cookie=%#jx, FID="DFID, fpath,
			 (uintmax_t)hai->hai_cookie, PFID(&hai->hai_fid));
	else if (rc < 0)
		CT_ERROR(rc, "llapi_hsm_action_end() on '%s' failed", fpath);
	else
		CT_TRACE("llapi_hsm_action_end() on '%s' ok (rc=%d)",
			 fpath, rc);

	return rc;
}

static int ct_archive(const struct hsm_action_item *hai, const long hal_flags)
{
	struct hsm_copyaction_private *hcp = NULL;
	char fpath[PATH_MAX + 1] = {0};
	int rc;
	int rcf = 0;
	int hp_flags = 0;
	int src_fd = -1;

	rc = fid_realpath(opt.o_mnt, &hai->hai_fid, fpath, sizeof(fpath));
	if (rc < 0) {
		CT_ERROR(rc, "fid_realpath()");
		goto cleanup;
	}

	rc = llapi_hsm_action_begin(&hcp, ctdata, hai, -1, 0, false);
	if (rc < 0) {
		CT_ERROR(rc, "llapi_hsm_action_begin() on '%s' failed", fpath);
		goto cleanup;
	}

	CT_TRACE("archiving '%s' to TSM storage", fpath);

	if (opt.o_dry_run) {
		rc = 0;
		goto cleanup;
	}

	src_fd = llapi_hsm_action_get_fd(hcp);
	if (src_fd < 0) {
		rc = src_fd;
		CT_ERROR(rc, "cannot open '%s' for read", fpath);
		goto cleanup;
	}

	rc = tsm_archive_fpath(FSNAME, fpath, NULL, -1,
			       (const void *)&hai->hai_fid);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_archive_fpath_fid on '%s' failed", fpath);
		goto cleanup;
	}

	CT_TRACE("data archiving for '%s' to TSM storage done", fpath);

cleanup:
	if (!(src_fd < 0))
		close(src_fd);

	rc = ct_finish(&hcp, hai, hp_flags, rcf, fpath);

	return rc;
}

static int ct_restore(const struct hsm_action_item *hai, const long hal_flags)
{
	struct hsm_copyaction_private *hcp = NULL;
	int rc;
	int dst_fd = -1;
	int mdt_index = -1;
	int open_flags = 0;
	int hp_flags = 0;
	char fpath[PATH_MAX + 1] = {0};
	struct lu_fid dfid;

	rc = fid_realpath(opt.o_mnt, &hai->hai_fid, fpath, sizeof(fpath));
	if (rc < 0) {
		CT_ERROR(rc, "fid_realpath()");
		return rc;
	}

	rc = llapi_get_mdt_index_by_fid(opt.o_mnt_fd, &hai->hai_fid,
					&mdt_index);
	if (rc < 0) {
		CT_ERROR(rc, "cannot get mdt index "DFID"",
			 PFID(&hai->hai_fid));
		return rc;
	}

	rc = llapi_hsm_action_begin(&hcp, ctdata, hai, mdt_index, open_flags,
				    false);
	if (rc < 0) {
		CT_ERROR(rc, "llapi_hsm_action_begin() on '%s' failed", fpath);
		return rc;
	}

	rc = llapi_hsm_action_get_dfid(hcp, &dfid);
	if (rc < 0) {
	    CT_ERROR(rc, "restoring "DFID
		     ", cannot get FID of created volatile file",
		     PFID(&hai->hai_fid));
	    goto cleanup;
	}

	CT_TRACE("restoring data from TSM storage to '%s'", fpath);
	if (opt.o_dry_run) {
	    rc = 0;
	    goto cleanup;
	}

	dst_fd = llapi_hsm_action_get_fd(hcp);
	if (dst_fd < 0) {
		rc = dst_fd;
		CT_ERROR(rc, "cannot open '%s' for write", fpath);
		goto cleanup;
	}

	rc = tsm_retrieve_fpath(FSNAME, fpath, NULL, dst_fd);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_retrieve_fpath on '%s' failed", fpath);
		goto cleanup;
	}

	CT_TRACE("data restore from TSM storage to '%s' done", fpath);

cleanup:
	rc = ct_finish(&hcp, hai, hp_flags, rc, fpath);

	if (!(dst_fd < 0))
		close(dst_fd);

	return rc;
}

static int ct_remove(const struct hsm_action_item *hai, const long hal_flags)
{
	struct hsm_copyaction_private *hcp = NULL;
	char fpath[PATH_MAX + 1] = {0};
	int rc;

	rc = fid_realpath(opt.o_mnt, &hai->hai_fid, fpath, sizeof(fpath));
	if (rc < 0) {
		CT_ERROR(rc, "fid_realpath()");
		goto cleanup;
	}

	rc = llapi_hsm_action_begin(&hcp, ctdata, hai, -1, 0, false);
	if (rc < 0) {
		CT_ERROR(rc, "llapi_hsm_action_begin() on '%s' failed", fpath);
		goto cleanup;
	}

	CT_TRACE("removing from TSM storage file '%s'", fpath);

	if (opt.o_dry_run) {
		rc = 0;
		goto cleanup;
	}
	rc = tsm_delete_fpath(FSNAME, fpath);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_delete_fpath on '%s' failed", fpath);
		goto cleanup;
	}

cleanup:
	rc = ct_finish(&hcp, hai, 0, rc, fpath);

	return rc;
}

static int ct_process_item(struct hsm_action_item *hai, const long hal_flags)
{
	int rc = 0;

	if (opt.o_verbose >= LLAPI_MSG_INFO || opt.o_dry_run) {
		/* Print the original path */
		char fid[128];
		char path[PATH_MAX];
		long long recno = -1;
		int linkno = 0;

		sprintf(fid, DFID, PFID(&hai->hai_fid));
		CT_TRACE("'%s' action %s reclen %d, cookie=%#jx",
			 fid, hsm_copytool_action2name(hai->hai_action),
			 hai->hai_len, (uintmax_t)hai->hai_cookie);
		rc = llapi_fid2path(opt.o_mnt, fid, path,
				    sizeof(path), &recno, &linkno);
		if (rc < 0)
			CT_ERROR(rc, "cannot get path of FID %s", fid);
		else
			CT_TRACE("processing file '%s'", path);
	}

	switch (hai->hai_action) {
	/* set err_major, minor inside these functions */
	case HSMA_ARCHIVE:
		rc = ct_archive(hai, hal_flags);
		break;
	case HSMA_RESTORE:
		rc = ct_restore(hai, hal_flags);
		break;
	case HSMA_REMOVE:
		rc = ct_remove(hai, hal_flags);
		break;
	case HSMA_CANCEL:
		CT_TRACE("cancel not implemented for file system '%s'",
			 opt.o_mnt);
		/* Don't report progress to coordinator for this cookie:
		 * the copy function will get ECANCELED when reporting
		 * progress. */
		err_minor++;
		return 0;
		break;
	default:
		rc = -EINVAL;
		CT_ERROR(rc, "unknown action %d, on '%s'", hai->hai_action,
			 opt.o_mnt);
		err_minor++;
		ct_finish(NULL, hai, 0, rc, NULL);
	}

	return 0;
}

struct ct_th_data {
	long hal_flags;
	struct hsm_action_item *hai;
};

static void *ct_thread(void *data)
{
	struct ct_th_data *cttd = data;
	int rc;

	rc = ct_process_item(cttd->hai, cttd->hal_flags);

	free(cttd->hai);
	free(cttd);
	pthread_exit((void *)(intptr_t)rc);
}

static int ct_process_item_async(const struct hsm_action_item *hai,
				 long hal_flags)
{
	pthread_attr_t attr;
	pthread_t thread;
	struct ct_th_data *data;
	int rc;

	data = malloc(sizeof(*data));
	if (data == NULL)
		return -ENOMEM;

	data->hai = malloc(hai->hai_len);
	if (data->hai == NULL) {
		free(data);
		return -ENOMEM;
	}

	memcpy(data->hai, hai, hai->hai_len);
	data->hal_flags = hal_flags;

	rc = pthread_attr_init(&attr);
	if (rc != 0) {
		CT_ERROR(rc, "pthread_attr_init failed for '%s' service",
			 opt.o_mnt);
		free(data->hai);
		free(data);
		return -rc;
	}

	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	rc = pthread_create(&thread, &attr, ct_thread, data);
	if (rc != 0)
		CT_ERROR(rc, "cannot create thread for '%s' service",
			 opt.o_mnt);

	pthread_attr_destroy(&attr);
	return 0;
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
			rc = ct_process_item_async(hai, hal->hal_flags);
			if (rc < 0)
				CT_ERROR(rc, "'%s' item %d process",
					 opt.o_mnt, i);
			if (opt.o_abort_on_error && err_major)
				break;
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

static int ct_setup(void)
{
	login_t login;
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

	memset(&login, 0, sizeof(login));
	strcpy(login.node, opt.o_node);
	strcpy(login.password, opt.o_password);
	strcpy(login.owner, opt.o_owner);
	strcpy(login.platform, LOGIN_PLATFORM);
	strcpy(login.fsname, FSNAME);
	strcpy(login.fstype, FSTYPE);
	const unsigned short o_servername_len = 1 + strlen(opt.o_servername) + strlen("-se=");
	if (o_servername_len < MAX_OPTIONS_LENGTH)
		snprintf(login.options, o_servername_len, "-se=%s", opt.o_servername);
	else
		CT_WARN("Option parameter \'-se=%s\' is larger than "
			"MAX_OPTIONS_LENGTH: %d and is ignored\n",
			opt.o_servername, MAX_OPTIONS_LENGTH);

	rc = tsm_init(&login);
	if (rc)
		return -rc;
	rc = tsm_query_session_info();
	if (rc)
		return -rc;

	return rc;
}

static int ct_cleanup(void)
{
	int rc = 0;

	if (opt.o_mnt_fd >= 0) {
		rc = close(opt.o_mnt_fd);
		if (rc < 0) {
			rc = -errno;
			CT_ERROR(rc, "cannot close mount point");
			goto cleanup;
		}
	}

cleanup:
	tsm_quit();

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
