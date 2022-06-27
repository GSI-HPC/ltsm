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
 * Copyright (c) 2022, GSI Helmholtz Centre for Heavy Ion Research
 */

#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <getopt.h>
#include "fsqapi.h"

#define BUF_LENGTH 0xFFFFF

#define FSQ_STORAGE_DEST_HUMAN_STR(s)			       \
	s == FSQ_STORAGE_LOCAL      ? "local"      :	       \
	s == FSQ_STORAGE_LUSTRE     ? "lustre"     :	       \
	s == FSQ_STORAGE_LUSTRE_TSM ? "lustre_tsm" : 	       \
        s == FSQ_STORAGE_TSM        ? "tsm"        :           \
        s == FSQ_STORAGE_NULL       ? "null"       : "UNKNOWN"

struct options {
	int	o_verbose;
	char	o_servername[HOST_NAME_MAX + 1];
	char	o_node[DSM_MAX_NODE_LENGTH + 1];
	char	o_password[DSM_MAX_VERIFIER_LENGTH + 1];
	char	o_fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char	o_fpath[PATH_MAX + 1];
	char	o_filename[PATH_MAX + 1];
	int	o_storage_dest;
	int     o_pipe;
};

static struct options opt = {
	.o_verbose	  = API_MSG_NORMAL,
	.o_servername	  = {0},
	.o_node		  = {0},
	.o_password	  = {0},
	.o_fsname	  = {0},
	.o_fpath	  = {0},
	.o_filename	  = {0},
	.o_storage_dest   = FSQ_STORAGE_NULL
};

static void usage(const char *cmd_name, const int rc)
{
	fprintf(stdout,
		"usage: %s [options] <file>\n"
		"\t--pipe\n"
		"\t-f, --fsname <string>\n"
		"\t-a, --fpath <string>\n"
		"\t-l, --filename <string>\n"
		"\t-o, --storagedest {null, local, lustre, tsm, lustre_tsm} [default: %s] \n"
		"\t-n, --node <string>\n"
		"\t-p, --password <string>\n"
		"\t-s, --servername <string>\n"
		"\t-v, --verbose {error, warn, message, info, debug} [default: %s]\n"
		"\t-h, --help\n"
		"version: %s, fsq protocol version: %s "
		"© 2022 by GSI Helmholtz Centre for Heavy Ion Research\n",
		cmd_name,
		FSQ_STORAGE_DEST_HUMAN_STR(opt.o_storage_dest),
		LOG_LEVEL_HUMAN_STR(opt.o_verbose),
		PACKAGE_VERSION,
		FSQ_PROTOCOL_VER_STR(FSQ_PROTOCOL_VER));

	exit(rc);
}

static int is_path_prefix(const char *fsname, const char *fpath)
{
	size_t len_fsname = strlen(opt.o_fsname);
	size_t len_fpath = strlen(opt.o_fpath);

	if (len_fpath < len_fsname)
		return -EINVAL;
	if (memcmp(opt.o_fsname, opt.o_fpath, len_fsname))
		return -EINVAL;
	if (len_fsname > 1 && opt.o_fpath[len_fsname] != '/')
		return -EINVAL;

	return 0;
}

static void sanity_arg_check(const char *argv)
{
	/* Required arguments. */
	if (!opt.o_fsname[0]) {
		fprintf(stderr, "missing argument -f, --fsname <string>\n\n");
		usage(argv, -EINVAL);
	}
	if (!opt.o_fpath[0]) {
		fprintf(stderr, "missing argument -a, --fpath <string>\n\n");
		usage(argv, -EINVAL);
	}
	if (!opt.o_node[0]) {
		fprintf(stderr, "missing argument -n, --node <string>\n\n");
		usage(argv, -EINVAL);
	}
	if (!opt.o_password[0]) {
		fprintf(stderr, "missing argument -p, --password <string>\n\n");
		usage(argv, -EINVAL);
	}
	if (!opt.o_servername[0]) {
		fprintf(stderr, "missing argument -s, --servername "
			"<string>\n\n");
		usage(argv, -EINVAL);
	}
	if (opt.o_filename[0] && strchr(opt.o_filename, '/')) {
		fprintf(stderr, "argument -l, --filename '%s' contains "
			"illegal character(s) '/'\n\n",
			opt.o_filename);
		usage(argv, -EINVAL);
	}
	if (opt.o_fsname[0] && opt.o_fpath[0]) {
		if (is_path_prefix(opt.o_fsname, opt.o_fpath)) {
			fprintf(stderr, "argument -f, --fsname '%s' is not a "
				"strict path prefix of argument -a, --fpath '%s'\n\n",
				opt.o_fsname, opt.o_fpath);
			usage(argv, -EINVAL);
		}
	}
}

static int parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{.name = "fsname",	.has_arg = required_argument, .flag = NULL,        .val = 'f'},
		{.name = "fpath",	.has_arg = required_argument, .flag = NULL,        .val = 'a'},
		{.name = "filename",	.has_arg = required_argument, .flag = NULL,        .val = 'l'},
		{.name = "storagedest",	.has_arg = required_argument, .flag = NULL,        .val = 'o'},
		{.name = "node",	.has_arg = required_argument, .flag = NULL,        .val = 'n'},
		{.name = "password",	.has_arg = required_argument, .flag = NULL,        .val = 'p'},
		{.name = "servername",	.has_arg = required_argument, .flag = NULL,        .val = 's'},
		{.name = "verbose",	.has_arg = required_argument, .flag = NULL,        .val = 'v'},
		{.name = "pipe",        .has_arg = no_argument,       .flag = &opt.o_pipe, .val = 1},
		{.name = "help",	.has_arg = no_argument,       .flag = NULL,        .val = 'h'},
		{0, 0, 0, 0}
	};
	int c;
	while ((c = getopt_long(argc, argv, "f:a:l:o:n:p:s:v:h",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'f': {
			strncpy(opt.o_fsname, optarg, DSM_MAX_FSNAME_LENGTH);
			break;
		}
		case 'a': {
			strncpy(opt.o_fpath, optarg, PATH_MAX);
			break;
		}
		case 'l': {
			strncpy(opt.o_filename, optarg, PATH_MAX);
			break;
		}
		case 'o': {
			if (OPTNCMP("null", optarg))
				opt.o_storage_dest = FSQ_STORAGE_NULL;
			else if (OPTNCMP("local", optarg))
				opt.o_storage_dest = FSQ_STORAGE_LOCAL;
			else if (OPTNCMP("lustre", optarg))
				opt.o_storage_dest = FSQ_STORAGE_LUSTRE;
			else if (OPTNCMP("tsm", optarg))
				opt.o_storage_dest = FSQ_STORAGE_TSM;
			else if (OPTNCMP("lustre_tsm", optarg))
				opt.o_storage_dest = FSQ_STORAGE_LUSTRE_TSM;
			else {
				fprintf(stderr, "wrong argument for -o, "
					"--storagedest='%s'\n", optarg);
				usage(argv[0], -EINVAL);
			}
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
				usage(argv[0], EINVAL);
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
			return EINVAL;
		}
	}

	sanity_arg_check(argv[0]);

	return 0;
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

	FILE *file = NULL;
	const char *filename = NULL;

	/* Argument <file> is provided. */
	if (argc - optind == 1) {
		/* Argument --pipe is provided and results in exclusionary parameters. */
		if (opt.o_pipe) {
			fprintf(stdout, "error two exclusionary parameters --pipe and <file>\n\n");
			usage(argv[0], -EINVAL);
		} else
			filename = argv[argc - 1];
	} else {
		if (opt.o_filename[0] && opt.o_pipe && argc - optind == 0)
			file = stdin;
		else if (!opt.o_filename[0] && opt.o_pipe && argc - optind == 0) {
			fprintf(stderr, "missing argument -l, --filename <filename>\n\n");
			usage(argv[0], -EINVAL);
		} else {
			fprintf(stderr, "missing or incorrect number of arguments\n\n");
			usage(argv[0], -EINVAL);
		}
	}

	struct fsq_login_t fsq_login;
	rc = fsq_init(&fsq_login, opt.o_node, opt.o_password, opt.o_servername);
	if (rc) {
		CT_ERROR(rc, "fsq_init failed");
		return rc;
	}

	struct fsq_session_t fsq_session;
	memset(&fsq_session, 0, sizeof(fsq_session));

	rc = fsq_fconnect(&fsq_login, &fsq_session);
	if (rc) {
		CT_ERROR(rc, "fsq_connect failed");
		return rc;
	}

	/* Argument <file> is provided and not stdin (--pipe). */
	if (!file) {
		file = fopen(filename, "rb");
		if (!file) {
			rc = -errno;
			CT_ERROR(rc, "fopen '%s' failed", filename);
			goto cleanup;
		}
	}

	if (opt.o_filename[0])
		filename = opt.o_filename;

	const size_t l = strlen(opt.o_fpath);
	if (opt.o_fpath[l - 1] != '/')
		opt.o_fpath[l] = '/';

	strncat(opt.o_fpath, basename(filename), PATH_MAX);

	rc = fsq_fdopen(opt.o_fsname, opt.o_fpath, NULL, opt.o_storage_dest,
			&fsq_session);
	if (rc) {
		CT_ERROR(rc, "fsq_fdopen '%s'", opt.o_fpath);
		goto cleanup;
	}

	char buf[BUF_LENGTH] = {0};
	ssize_t size;
	do {
		size = fread(buf, 1, BUF_LENGTH, file);
		if (ferror(file)) {
			rc = -EIO;
			CT_ERROR(rc, "fread failed");
			break;
		}
		if (size == 0)
			break;
		rc = fsq_fwrite(buf, 1, size, &fsq_session);
		if (rc < 0) {
			CT_ERROR(rc, "fsq_fwrite failed");
			break;
		}
	} while (!feof(file));

cleanup:
	if (file)
		fclose(file);
	rc = fsq_fclose(&fsq_session);
	fsq_fdisconnect(&fsq_session);

	if (rc)
		CT_ERROR(rc, "failed sending file '%s' with fpath '%s' to FSQ server '%s'\n",
			 filename, opt.o_fpath, opt.o_servername);

	else
		CT_MESSAGE("successfully sent file '%s' with fpath '%s' to FSQ server '%s'\n",
			   filename, opt.o_fpath, opt.o_servername);

	return rc;
}
