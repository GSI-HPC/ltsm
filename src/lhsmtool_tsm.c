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
 * Copyright (c) 2016-2020, GSI Helmholtz Centre for Heavy Ion Research
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
#include <semaphore.h>
#include <time.h>
#include <stdint.h>
#include <linux/limits.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/param.h>
#include <lustre/lustreapi.h>
#include "tsmapi.h"
#include "queue.h"

/* Lustre commit 3bfb6107ba4e92d8aa02e842502bc44bac7b8b43
   increased the upper limit of maximum HSM backends
   registered with MDT. We still use (sizeof(__u32) * 8) = 32
   for compatibility with older MDT. */
#ifndef LL_HSM_ORIGIN_MAX_ARCHIVE
#define LL_HSM_ORIGIN_MAX_ARCHIVE (sizeof(__u32) * 8)
#endif

/* Program options */
struct options {
	int o_daemonize;
	int o_dry_run;
	int o_nthreads;
	int o_verbose;
	int o_restore_stripe;
	int o_abort_on_err;
	int o_enable_maxmpc;
        int o_archive_cnt;
        int o_archive_id[LL_HSM_ORIGIN_MAX_ARCHIVE + 1];
	char *o_mnt;
	int o_mnt_fd;
	char o_servername[DSM_MAX_SERVERNAME_LENGTH + 1];
	char o_node[DSM_MAX_NODE_LENGTH + 1];
	char o_owner[DSM_MAX_OWNER_LENGTH + 1];
	char o_password[DSM_MAX_VERIFIER_LENGTH + 1];
	char o_fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char o_fstype[DSM_MAX_FSTYPE_LENGTH + 1];
	char o_conf[MAX_OPTIONS_LENGTH + 1];
};

static struct options opt = {
	.o_verbose = API_MSG_NORMAL,
	.o_servername = {0},
	.o_node = {0},
	.o_owner = {0},
	.o_password = {0},
	.o_fsname = {0},
	.o_fstype = {0},
	.o_conf = {0}
};

/* Threads */
static volatile
enum {
	RUNNING,
	EXITING,
	FINISHED
} 			proc_state = RUNNING;
static uint16_t		nthreads = 1;
static pthread_t	*threads;

/* Work queue */
#define QUEUE_MAX_ITEMS	(2 * nthreads)
static sem_t		queue_sem;
static pthread_mutex_t	queue_mutex;
static pthread_cond_t	queue_cond;
static queue_t		queue;

/* Session */
static struct session_t	*sessions;
static char lustre_fsname[MAX_OBD_NAME + 1] = {0};
static struct hsm_copytool_private *ctdata;

/* Error handling */
static int err_major;
static int err_minor;

static void usage(const char *cmd_name, const int rc)
{
	dsmApiVersionEx libapi_ver = get_libapi_ver();
	dsmAppVersion appapi_ver = get_appapi_ver();

	fprintf(stdout, "usage: %s [options] <lustre_mount_point>\n"
		"\t-a, --archive-id <int> [default: 0]\n"
		"\t\t""archive id number\n"
		"\t-t, --threads <int>\n"
		"\t\t""number of processing threads [default: %d]\n"
		"\t-n, --node <string>\n"
		"\t\t""node name registered on tsm server\n"
		"\t-p, --password <string>\n"
		"\t\t""password of tsm node/owner\n"
		"\t-o, --owner <string>\n"
		"\t\t""owner of tsm node\n"
		"\t-s, --servername <string>\n"
		"\t\t""hostname of tsm server\n"
		"\t-c, --conf <file>\n"
		"\t\t""option conf file\n"
		"\t-v, --verbose {error, warn, message, info, debug}"
		" [default: message]\n"
		"\t\t""produce more verbose output\n"
		"\t--abort-on-error\n"
		"\t\t""abort operation on major error\n"
		"\t--daemon\n"
		"\t\t""daemon mode run in background\n"
		"\t--dry-run\n"
		"\t\t""don't run, just show what would be done\n"
		"\t--restore-stripe\n"
		"\t\t""restore stripe information\n"
		"\t--enable-maxmpc\n"
		"\t\t""enable tsm mount point check to infer the maximum number"
		" of feasible threads\n"
		"\t-h, --help\n"
		"\t\t""show this help\n"
		"\nIBM API library version: %d.%d.%d.%d, "
		"IBM API application client version: %d.%d.%d.%d\n"
		"version: %s © 2017 by GSI Helmholtz Centre for Heavy Ion Research\n",
		cmd_name,
		nthreads,
		libapi_ver.version, libapi_ver.release, libapi_ver.level,
		libapi_ver.subLevel,
		appapi_ver.applicationVersion, appapi_ver.applicationRelease,
		appapi_ver.applicationLevel, appapi_ver.applicationSubLevel,
		PACKAGE_VERSION);
	exit(rc);
}

static void sanity_arg_check(const struct options *opts, const char *argv)
{
	if (!opt.o_node[0]) {
		fprintf(stdout, "missing argument -n, --node <string>\n\n");
		usage(argv, 1);
	} else if (!opt.o_password[0]) {
		fprintf(stdout, "missing argument -p, --password <string>\n\n");
		usage(argv, 1);
	} else if (!opt.o_servername[0]) {
		fprintf(stdout, "missing argument -s, --servername "
			"<string>\n\n");
		usage(argv, 1);
	}
}

static int parse_archive_id(const char *arg)
{
	char *end = NULL;
	int val = strtol(arg, &end, 10);
	int rc = 0;

	if (*end != '\0') {
		rc = -EINVAL;
		CT_ERROR(rc, "invalid archive-id: '%s'", arg);
		return rc;
	}
	if ((opt.o_archive_cnt > (int)LL_HSM_ORIGIN_MAX_ARCHIVE) ||
	    (val > (int)LL_HSM_ORIGIN_MAX_ARCHIVE)) {
		rc = -EINVAL;
		CT_ERROR(rc, "archive number must be less"
			 " than %zu", LL_HSM_ORIGIN_MAX_ARCHIVE + 1);
		return rc;
	}
	opt.o_archive_id[opt.o_archive_cnt] = val;
	opt.o_archive_cnt++;

	return rc;
}

static int parse_nthreads(const char *arg)
{
	char *end = NULL;
	int val = strtol(arg, &end, 10);
	int rc = 0;

	if (*end != '\0') {
		rc = -EINVAL;
		CT_ERROR(rc, "invalid number of threads: '%s'", arg);
		return rc;
	}
	if (val <= 0) {
		rc = -EINVAL;
		CT_ERROR(rc, "number of threads must be greater than 0");
		return rc;
	}
	nthreads = val;

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
			if (OPTNCMP("servername", kv_opt.kv[n].key))
				strncpy(opt.o_servername, kv_opt.kv[n].val,
					1 + MIN(DSM_MAX_SERVERNAME_LENGTH,
					       MAX_OPTIONS_LENGTH));
			else if (OPTNCMP("node", kv_opt.kv[n].key))
				strncpy(opt.o_node, kv_opt.kv[n].val,
					1 + MIN(DSM_MAX_NODE_LENGTH,
						MAX_OPTIONS_LENGTH));
			else if (OPTNCMP("owner", kv_opt.kv[n].key))
				strncpy(opt.o_owner, kv_opt.kv[n].val,
					1 + MIN(DSM_MAX_OWNER_LENGTH,
						MAX_OPTIONS_LENGTH));
			else if (OPTNCMP("password", kv_opt.kv[n].key))
				strncpy(opt.o_password, kv_opt.kv[n].val,
					1 + MIN(DSM_MAX_VERIFIER_LENGTH,
						MAX_OPTIONS_LENGTH));
			else if (OPTNCMP("archive-id", kv_opt.kv[n].key)) {
				rc = parse_archive_id(kv_opt.kv[n].val);
				if (rc)
					CT_WARN("wrong value '%s' for option "
						"'%s' in conf file '%s'",
						kv_opt.kv[n].val,
						kv_opt.kv[n].key,
						filename);
			}
			else if (OPTNCMP("threads", kv_opt.kv[n].key)) {
				rc = parse_nthreads(kv_opt.kv[n].val);
				if (rc)
					CT_WARN("wrong value '%s' for option "
						"'%s' in conf file '%s'",
						kv_opt.kv[n].val,
						kv_opt.kv[n].key,
						filename);
			}
			else if (OPTNCMP("verbose", kv_opt.kv[n].key)) {
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

static int ct_parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{.name = "abort-on-error", .has_arg = no_argument,       .flag = &opt.o_abort_on_err,   .val =  1},
		{.name = "archive-id",	   .has_arg = required_argument, .flag = NULL,	                .val = 'a'},
		{.name = "daemon",	   .has_arg = no_argument,       .flag = &opt.o_daemonize,      .val =   1},
		{.name = "threads",        .has_arg = required_argument, .flag = NULL,	                .val = 't'},
		{.name = "node",           .has_arg = required_argument, .flag = NULL,                  .val = 'n'},
		{.name = "password",       .has_arg = required_argument, .flag = NULL,                  .val = 'p'},
		{.name = "owner",          .has_arg = required_argument, .flag = NULL,                  .val = 'o'},
		{.name = "servername",     .has_arg = required_argument, .flag = NULL,                  .val = 's'},
		{.name = "conf",	   .has_arg = required_argument, .flag = NULL,		        .val = 'c'},
		{.name = "verbose",        .has_arg = required_argument, .flag = NULL,                  .val = 'v'},
		{.name = "dry-run",	   .has_arg = no_argument,	 .flag = &opt.o_dry_run,        .val =   1},
		{.name = "restore-stripe", .has_arg = no_argument,	 .flag = &opt.o_restore_stripe, .val =   1},
		{.name = "enable-maxmpc",  .has_arg = no_argument,	 .flag = &opt.o_enable_maxmpc,  .val =   1},
		{.name = "help",           .has_arg = no_argument,       .flag = NULL,		        .val = 'h'},
		{.name = NULL}
	};

	int c, rc;
	optind = 0;

	while ((c = getopt_long(argc, argv, "a:t:n:p:o:s:c:v:h",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'a': {
			rc = parse_archive_id(optarg);
			if (rc)
				return rc;
			break;
		}
		case 't': {
			rc = parse_nthreads(optarg);
			if (rc)
				return rc;
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
		case 'o': {
			strncpy(opt.o_owner, optarg, DSM_MAX_OWNER_LENGTH);
			break;
		}
		case 's': {
			strncpy(opt.o_servername, optarg,
				DSM_MAX_SERVERNAME_LENGTH);
			break;
		}
		case 'c': {
			read_conf(optarg);
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

	/* Filespace name is set to Lustre mount point. */
	strncpy(opt.o_fsname, opt.o_mnt, DSM_MAX_FSNAME_LENGTH);
	const size_t len_fsname = strlen(opt.o_fsname);
	if (len_fsname > 2 && opt.o_fsname[len_fsname - 1] == '/')
		opt.o_fsname[len_fsname - 1] = '\0';
	CT_DEBUG("using TSM filespace name '%s'", opt.o_fsname);

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
	if (rc == -ECANCELED)
		CT_WARN("[rc=%d] llapi_hsm_action_progress operation canceled",
			rc);
	else if (rc)
		CT_ERROR(rc, "llapi_hsm_action_progress");

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

static int ct_hsm_action_begin(struct session_t *session, int mdt_index,
			       int open_flags, bool is_error)
{
	return llapi_hsm_action_begin(&session->hcp, ctdata, session->hai,
				      mdt_index, open_flags, is_error);
}

static int ct_hsm_action_end(struct session_t *session, const int ct_rc,
			     const char *fpath)
{
	int rc;

	CT_MESSAGE("action completed, notifying coordinator "
		   "cookie=%#jx, FID="DFID", err=%d",
		   (uintmax_t)session->hai->hai_cookie,
		   PFID(&session->hai->hai_fid),
		   -ct_rc);

	rc = llapi_hsm_action_end(&session->hcp, &session->hai->hai_extent,
				  0 /* HP_FLAG_RETRY */, ct_rc ? EIO : 0);
	if (rc == -ECANCELED)
		CT_ERROR(rc, "completed action on '%s' has been canceled: "
			 "cookie=%#jx, FID="DFID, fpath,
			 (uintmax_t)session->hai->hai_cookie,
			 PFID(&session->hai->hai_fid));
	else if (rc < 0)
		CT_ERROR(rc, "llapi_hsm_action_end on '%s' failed", fpath);
	else
		CT_DEBUG("[rc=%d] llapi_hsm_action_end on '%s' ok", rc,
			 fpath);

	return rc;
}

static int ct_archive(struct session_t *session)
{
	int rc;
	int fd = -1;
	int mdt_index = -1;
	int open_flags = 0;
	char fpath[PATH_MAX + 1] = {0};
	struct lustre_info_t lustre_info = {.fid = {0},
					    .lov = {0}};

	rc = fid_realpath(opt.o_mnt, &session->hai->hai_fid, fpath,
			  sizeof(fpath));
	if (rc < 0) {
		CT_ERROR(rc, "fid_realpath failed");
		goto cleanup;
	}

	rc = ct_hsm_action_begin(session, mdt_index, open_flags, false);
	CT_DEBUG("[rc=%d] ct_hsm_action_begin on '%s'", rc, fpath);
	if (rc < 0) {
		CT_ERROR(rc, "ct_hsm_action_begin on '%s' failed", fpath);
		goto cleanup;
	}
	CT_MESSAGE("archiving '%s' to TSM storage", fpath);

	if (opt.o_dry_run) {
		CT_MESSAGE("running in dry-run mode, skipping effective"
			   " archiving TSM operation");
		rc = 0;
		goto cleanup;
	}

	fd = llapi_hsm_action_get_fd(session->hcp);
	CT_DEBUG("[fd=%d] llapi_hsm_action_get_fd()", fd);
	if (fd < 0) {
		rc = fd;
		CT_ERROR(rc, "cannot open '%s' for read", fpath);
		goto cleanup;
	}

	lustre_info.fid.seq = session->hai->hai_fid.f_seq;
	lustre_info.fid.oid = session->hai->hai_fid.f_oid;
	lustre_info.fid.ver = session->hai->hai_fid.f_ver;

	if (opt.o_restore_stripe) {
		rc = xattr_get_lov(fd, &lustre_info, fpath);
		CT_DEBUG("[rc=%d,fd=%d] xattr_get_lov '%s'", rc, fd, fpath);
		if (rc)
			CT_WARN("[rc=%d,fd=%d] xattr_get_lov failed on '%s' "
				"stripe information cannot be obtained", rc, fd,
				fpath);
	}

	rc = tsm_archive_fpath(opt.o_fsname, fpath, NULL /* Description */, fd,
			       &lustre_info, session);
	if (rc) {
		CT_ERROR(rc, "tsm_archive_fpath on '%s' failed", fpath);
		goto cleanup;
	}
	CT_MESSAGE("archiving '%s' to TSM storage done", fpath);

cleanup:
	if (!(fd < 0))
		close(fd);

	rc = ct_hsm_action_end(session, rc, fpath);

	return rc;
}

static int ct_restore(struct session_t *session)
{
	int rc;
	int fd = -1;
	int mdt_index = -1;
	int open_flags = 0;
	char fpath[PATH_MAX + 1] = {0};

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

	if (opt.o_restore_stripe)
		open_flags |= O_LOV_DELAY_CREATE;

	rc = ct_hsm_action_begin(session, mdt_index, open_flags, false);
	if (rc < 0) {
		CT_ERROR(rc, "llapi_hsm_action_begin on '%s' failed", fpath);
		return rc;
	}
	CT_MESSAGE("restoring data from TSM storage to '%s'", fpath);

	if (opt.o_dry_run) {
		CT_MESSAGE("running in dry-run mode, skipping effective"
			   " restoring TSM operation");
		rc = 0;
		goto cleanup;
	}

	fd = llapi_hsm_action_get_fd(session->hcp);
	if (fd < 0) {
		rc = fd;
		CT_ERROR(rc, "cannot open '%s' for write", fpath);
		goto cleanup;
	}

	rc = tsm_retrieve_fpath(opt.o_fsname, fpath, NULL /* Description */,
				fd, session);
	if (rc) {
		CT_ERROR(rc, "tsm_retrieve_fpath on '%s' failed", fpath);
		goto cleanup;
	}
	CT_MESSAGE("data restore from TSM storage to '%s' done", fpath);

cleanup:
	if (!(fd < 0))
		close(fd);

	rc = ct_hsm_action_end(session, rc, fpath);

	return rc;
}

static int ct_remove(struct session_t *session)
{
	int rc;
	int mdt_index = -1;
	int open_flags = 0;
	char fpath[PATH_MAX + 1] = {0};

	rc = fid_realpath(opt.o_mnt, &session->hai->hai_fid, fpath,
			  sizeof(fpath));
	if (rc < 0) {
		CT_ERROR(rc, "fid_realpath()");
		goto cleanup;
	}

	rc = ct_hsm_action_begin(session, mdt_index, open_flags, false);
	if (rc < 0) {
		CT_ERROR(rc, "ct_hsm_action_begin on '%s' failed", fpath);
		goto cleanup;
	}
	CT_MESSAGE("removing from TSM storage file '%s'", fpath);

	if (opt.o_dry_run) {
		CT_MESSAGE("running in dry-run mode, skipping effective"
			   " removing TSM operation");
		rc = 0;
		goto cleanup;
	}
	rc = tsm_delete_fpath(opt.o_fsname, fpath, session);
	if (rc != DSM_RC_SUCCESSFUL) {
		CT_ERROR(rc, "tsm_delete_fpath on '%s' failed", fpath);
		goto cleanup;
	}

cleanup:
	rc = ct_hsm_action_end(session, rc, fpath);

	return rc;
}

static int ct_process_item(struct session_t *session)
{
	int rc = 0;

	if (opt.o_verbose >= API_MSG_NORMAL) {

		char strfid[FID_LEN + 1];
		char path[PATH_MAX];
		long long recno = -1;
		int linkno = 0;

		snprintf(strfid, sizeof(strfid), DFID, PFID(&session->hai->hai_fid));
		CT_MESSAGE("'%s' action %s reclen %d, cookie=%#jx",
			   strfid,
			   hsm_copytool_action2name(session->hai->hai_action),
			   session->hai->hai_len,
			   (uintmax_t)session->hai->hai_cookie);
		rc = llapi_fid2path(opt.o_mnt, strfid, path,
				    sizeof(path), &recno, &linkno);
		if (rc < 0)
			CT_ERROR(rc, "cannot get path of FID %s", strfid);
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
	case HSMA_CANCEL:
		/* Cancel operation is handle by llapi_hsm_action_progress,
		   however catch it here as a known action. */
		break;
	default:
		rc = -EINVAL;
		CT_ERROR(rc, "unknown action %d, on '%s'",
			 session->hai->hai_action,
			 opt.o_mnt);
		err_minor++;
		ct_hsm_action_end(session, rc, NULL);
	}
	free(session->hai);

	return rc;
}

static void *ct_thread(void *data)
{
	struct session_t *session = (struct session_t *) data;
	int rc;

	for (;;) {
		/* Critical region, lock. */
		pthread_mutex_lock(&queue_mutex);
		while (queue_size(&queue) == 0) {
			if (proc_state != RUNNING) {
				rc = 0;
				pthread_mutex_unlock(&queue_mutex);
				goto thread_exit;
			}

			pthread_cond_wait(&queue_cond, &queue_mutex);
		}

		rc = queue_dequeue(&queue, (void **)&session->hai);

		/* Unlock. */
		pthread_mutex_unlock(&queue_mutex);

		/* Signal work queue empty slot */
		sem_post(&queue_sem);

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

		rc = ct_process_item(session);
		if (rc)
			CT_ERROR(rc, "ct_process_item failed");
	}

thread_exit:
	return NULL;
}


/* Daemon waits for messages from the kernel; run it in the background. */
static int ct_run(void)
{
	int n, rc;

	if (opt.o_daemonize) {
		rc = daemon(1, 1);
		if (rc < 0) {
			rc = -errno;
			CT_ERROR(rc, "cannot daemonize");
			return rc;
		}
	}

	rc = llapi_hsm_copytool_register(&ctdata, opt.o_mnt,
					 opt.o_archive_cnt,
					 opt.o_archive_id, 0);
	if (rc < 0) {
		CT_ERROR(rc, "cannot start copytool interface");
		return rc;
	}

	while (1) {
		struct hsm_action_list *hal;
		struct hsm_action_item *hai;
		int msgsize;
		const struct timespec sem_wait_time = { .tv_sec = 1,
							.tv_nsec = 0 };

		int i = 0;

		CT_DEBUG("waiting for message from kernel");

		bool already_shown = false;
		/* Check if work queue is already full */
		while (-1 == sem_timedwait(&queue_sem, &sem_wait_time)) {
			if (proc_state != RUNNING)
				break;

			/* Just show the message once,
			   while waiting for a free spot. */
			if (!already_shown) {
				CT_MESSAGE("waiting for free spots in "
					   "work queue");
				already_shown = true;
			}
		}

		/* Wait for new items from Lustre HSM */
		rc = llapi_hsm_copytool_recv(ctdata, &hal, &msgsize);
		if (rc == -ESHUTDOWN) {
			CT_MESSAGE("ct_run() stopping, Lustre is shutting down");
			break;
		} else if (rc == -EINTR && proc_state != RUNNING) {
			CT_DEBUG("ct_run() stopping, interrupted %d", errno);
			break;
		} else if (rc < 0) {
			CT_WARN("cannot receive action list: %s", strerror(-rc));
			err_major++;
			if (opt.o_abort_on_err)
				break;
			else
				continue;
		}

		CT_MESSAGE("copytool fs=%s archive#=%d item_count=%d",
			   hal->hal_fsname, hal->hal_archive_id, hal->hal_count);

		/*
		 * Update work queue available slot count depending on length
		 * of hsm action list. Note that this can allow more items into
		 * the work queue, but no new hsm actions will be received until
		 * queue length drops below QUEUE_MAX_ITEMS.
		 */
		if (hal->hal_count == 0) {
			CT_DEBUG("Received an empty HSM action list");
			sem_post(&queue_sem);
			continue;
		} else {
			for(n = (int)hal->hal_count - 1; n > 0; n--)
				sem_trywait(&queue_sem);
		}

		if (strcmp(hal->hal_fsname, lustre_fsname) != 0) {
			rc = -EINVAL;
			CT_ERROR(rc, "'%s' invalid fs name, expecting: %s",
				 hal->hal_fsname, lustre_fsname);
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

	/* Handle shutdown */
	/* cancel pending work items */
	pthread_mutex_lock(&queue_mutex);
	CT_MESSAGE("Exiting: cleaning pending queue");

	while (queue_size(&queue) > 0) {
		struct hsm_action_item *hai;
		struct hsm_copyaction_private *hcp;
		char fid[128];

		queue_dequeue(&queue, (void **)&hai);

		sprintf(fid, DFID, PFID(&hai->hai_fid));
		CT_DEBUG("canceling fid '%s' action %s reclen %d, cookie=%#jx",
			fid, hsm_copytool_action2name(hai->hai_action),
			hai->hai_len, (uintmax_t)hai->hai_cookie);

		rc = llapi_hsm_action_begin(&hcp, ctdata, hai, -1, 0, true);
		if (rc < 0)
			CT_ERROR(rc, "cancel with llapi_hsm_action_begin() failed");

		rc = llapi_hsm_action_end(&hcp, &hai->hai_extent, 0, abs(rc));
		if (rc < 0)
			CT_ERROR(rc, "cancel with llapi_hsm_action_end() failed");

		free(hai);
	}
	pthread_mutex_unlock(&queue_mutex);

	/* Signal all threads to continue */
	proc_state = EXITING;
	pthread_cond_broadcast(&queue_cond);
	/* Wait for threads to terminate */
	for (n = 0; n < nthreads; n++) {
		rc = pthread_join(threads[n], NULL);
		CT_MESSAGE("Exiting: stopped thread worker %d with %d", n, rc);
	}

	/* We're done with hsm now */
	rc = llapi_hsm_copytool_unregister(&ctdata);
	ctdata = NULL;
	CT_MESSAGE("Exiting: copytool unregistered with rc %d", rc);


	return rc;
}

static int ct_connect_sessions(void)
{
	int rc;
	struct login_t login;
	int n;
	int threads_asked = nthreads;

	sessions = calloc(nthreads, sizeof(struct session_t));
	if (sessions == NULL) {
		rc = -errno;
		CT_ERROR(rc, "malloc failed");
		return rc;
	}

	rc = tsm_init(DSM_MULTITHREAD);
	if (rc) {
		rc = -ECANCELED;
		CT_ERROR(rc, "tsm_init failed");
		return rc;
	}

	memset(&login, 0, sizeof(login));
	login_init(&login, opt.o_servername,
		   opt.o_node, opt.o_password,
		   opt.o_owner, LINUX_PLATFORM,
		   opt.o_fsname, DEFAULT_FSTYPE);

	for (n = 0; n < nthreads; n++) {
		sessions[n].progress = progress_callback;
		CT_MESSAGE("tsm_init: session: %d", n + 1);

		rc = tsm_connect(&login, &sessions[n]);
		if (rc) {
			CT_ERROR(rc, "tsm_init failed");
			nthreads = n; // Don't attempt to create more threads.
			rc = 0;
			break;
		}

		if (!opt.o_enable_maxmpc)
			continue;

		/* Find maximum number of allowed mountpoints (alias the number
		   of maxium threads) by sending DSM_OBJ_DIRECTORY and verifying
		   whether transaction was successful. If rc is ECONNREFUSED,
		   then number of threads > maximum number of allowed
		   mountpoints, and the current number of threads have to be
		   decreased. */
		rc = tsm_check_free_mountp(opt.o_fsname, &sessions[n]);
		if (rc) {
			tsm_disconnect(&sessions[n]);
			nthreads = n; // don't attempt to create more threads
			if (rc == ECONNREFUSED) {
				if (opt.o_abort_on_err) {
					/* exit and clean sessions */
					CT_ERROR(rc, "Check TSM `MAXNUMMP` setting for the node"
					" (Maximum Mount Points Allowed). Aborting...");
					goto fail;
				} else {
					CT_WARN("Check TSM `MAXNUMMP` setting for the node"
					" (Maximum Mount Points Allowed)");
				}
			}
			rc = 0;
			break;
		}
	}

	CT_DEBUG("Abort on error %d", opt.o_abort_on_err);

	if (nthreads == 0) {
		CT_WARN("tsm_query_session failed");
		return -EACCES;
	}

	if (threads_asked != nthreads)
		CT_WARN("Created %d out of %d threads!", nthreads, threads_asked);
fail:
	return rc;
}

static int ct_start_threads(void)
{
	int rc;
	uint16_t n;
	pthread_attr_t attr;
	char thread_name[32];

	threads = calloc(nthreads, sizeof(pthread_t));
	if (threads == NULL) {
		rc = -errno;
		CT_ERROR(rc, "malloc failed");
		return rc;
	}

	rc = pthread_attr_init(&attr);
	if (rc != 0) {
		CT_ERROR(rc, "pthread_attr_init failed");
		goto cleanup;
	}

	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	for (n = 0; n < nthreads; n++) {
		rc = pthread_create(&threads[n], &attr, ct_thread, &sessions[n]);
		if (rc != 0)
			CT_ERROR(rc, "cannot create worker thread '%d' for"
				 "'%s'", n, opt.o_mnt);

		sprintf(thread_name, "lhsmtool_tsm/%d", n);
		pthread_setname_np(threads[n], thread_name);
	}
	pthread_attr_destroy(&attr);

	return rc;

cleanup:
	free(threads);
	threads = NULL;

	return rc;
}

static int ct_setup(void)
{
	int rc;

	rc = llapi_search_fsname(opt.o_mnt, lustre_fsname);
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

	if (opt.o_restore_stripe) {
		set_restore_stripe(true);
		CT_MESSAGE("stripe information will be restored");
	}

	sem_init(&queue_sem, 0, QUEUE_MAX_ITEMS);
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

static void ct_cleanup(void)
{
	if (opt.o_mnt_fd >= 0) {
		int rc;

		rc = close(opt.o_mnt_fd);
		if (rc < 0) {
			rc = -errno;
			CT_ERROR(rc, "cannot close mount point");
		}
	}
	sem_destroy(&queue_sem);
	pthread_mutex_destroy(&queue_mutex);
	pthread_cond_destroy(&queue_cond);

	for (int n = 0; n < nthreads && sessions && threads; n++)
		tsm_disconnect(&sessions[n]);

	if (sessions) {
		free(sessions);
		sessions = NULL;
	}
	if (threads) {
		free(threads);
		threads = NULL;
	}

	tsm_cleanup(DSM_MULTITHREAD);
}

static void atexit_unregister(void)
{
	/* If we don't clean up upon interrupt, umount thinks there's a ref
	 * and doesn't remove us from mtab (EINPROGRESS). The lustre client
	 * does successfully unmount and the mount is actually gone, but the
	 * mtab entry remains. So this just makes mtab happier. */
	if (ctdata)
		llapi_hsm_copytool_unregister(&ctdata);
}


/*
 * Signal handling:
 * We intercept SIGINT and SIGTERM in order to implement graceful shutdown of the
 * copytool. In case the signal is delivered to one of the worker threads, we
 * signal the main thread in order to interrupt blocking calls inside of ct_run()
 */
static pthread_t main_thread;

static void handler_int_term(int signal)
{
	if((signal == SIGINT || signal == SIGTERM) && proc_state == RUNNING) {
		proc_state = EXITING;
		psignal(signal, "Exiting: changing process status to EXITING on signal");
	}

	/* Forward the signal to the main thread if needed */
	if (pthread_self() != main_thread) {
		psignal(signal, "Exiting: forwarding signal to the main thread");
		pthread_kill(main_thread, signal);
	}

	CT_DEBUG("Interrupt handler: process state: %d", proc_state);
}


int main(int argc, char *argv[])
{
	struct sigaction cleanup_sigaction;
	int rc;

	/* Line buffered stdout */
	(void) setvbuf(stdout, NULL, _IOLBF, 0);

	/* Register the at-exit method for lustre hsm cleanup */
	atexit(atexit_unregister);

	/* Set the signal handler */
	main_thread = pthread_self();
	cleanup_sigaction.sa_handler = handler_int_term;
	cleanup_sigaction.sa_flags = 0;
	sigemptyset(&cleanup_sigaction.sa_mask);
	sigaction(SIGINT, &cleanup_sigaction, NULL);
	sigaction(SIGTERM, &cleanup_sigaction, NULL);

	rc = ct_parseopts(argc, argv);
	if (rc < 0) {
		CT_WARN("try '%s --help' for more information", argv[0]);
		return -rc;
	}

	rc = ct_setup();
	if (rc)
		goto error_cleanup;

	rc = ct_run();
	CT_MESSAGE("process finished, rc=%d (%s)", rc, strerror(-rc));

error_cleanup:
	ct_cleanup();

	/* All done*/
	proc_state = FINISHED;

	return -rc;
}
