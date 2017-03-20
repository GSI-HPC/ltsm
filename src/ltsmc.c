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
 * 			     Jörg Behrendt <j.behrendt@gsi.de>
 */

#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <getopt.h>
#include <sys/stat.h>
#include "tsmapi.h"

struct options {
	int o_archive;
	int o_retrieve;
	int o_query;
	int o_delete;
	int o_verbose;
	int o_latest;
	int o_recursive;
	char o_servername[DSM_MAX_SERVERNAME_LENGTH + 1];
	char o_node[DSM_MAX_NODE_LENGTH + 1];
	char o_owner[DSM_MAX_OWNER_LENGTH + 1];
	char o_password[DSM_MAX_VERIFIER_LENGTH + 1];
	char o_fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char o_fstype[DSM_MAX_FSTYPE_LENGTH + 1];
	char o_desc[DSM_MAX_DESCR_LENGTH + 1];
};

struct options opt = {
	.o_verbose = API_MSG_NORMAL,
	.o_servername = {0},
	.o_node = {0},
	.o_owner = {0},
	.o_password = {0},
	.o_fsname = {0},
	.o_fstype = {0},
	.o_desc = {0},
};

static void usage(const char *cmd_name, const int rc)
{
	dsmApiVersionEx libapi_ver = get_libapi_ver();
	dsmAppVersion appapi_ver = get_appapi_ver();

	fprintf(stdout, "usage: %s [options] <files|directories|wildcards>\n"
		"\t--archive\n"
		"\t--retrieve\n"
		"\t--query\n"
		"\t--delete\n"
		"\t-l, --latest [retrieve object with latest timestamp when multiple exists]\n"
		"\t-r, --recursive [archive directory and all sub-directories]\n"
		"\t-f, --fsname <string> [default: '/']\n"
		"\t-d, --description <string>\n"
		"\t-n, --node <string>\n"
		"\t-o, --owner <string>\n"
		"\t-p, --password <string>\n"
		"\t-s, --servername <string>\n"
		"\t-v, --verbose {error, warn, message, info, debug} [default: message]\n"
		"\t-h, --help\n"
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
	unsigned char count = 0;
	count = opt.o_archive  == 1 ? count + 1 : count;
	count = opt.o_retrieve == 1 ? count + 1 : count;
	count = opt.o_delete   == 1 ? count + 1 : count;
	count = opt.o_query    == 1 ? count + 1 : count;

	if (count == 0) {
		fprintf(stdout, "missing argument --archive, --retrieve,"
			" --query or --delete\n\n");
		usage(argv, 1);
	} else if (count != 1) {
		printf("multiple incompatible arguments"
		       " --archive, --retrieve, --query or --delete\n\n");
		usage(argv, 1);
	}

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

static int parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{"archive",           no_argument, &opt.o_archive,     1},
		{"retrieve",          no_argument, &opt.o_retrieve,    1},
		{"query",             no_argument, &opt.o_query,       1},
		{"delete",            no_argument, &opt.o_delete,      1},
		{"latest",            no_argument, 0,                'l'},
		{"recursive",         no_argument, 0,                'r'},
		{"fsname",      required_argument, 0,                'f'},
		{"description", required_argument, 0,                'd'},
		{"node",        required_argument, 0,                'n'},
		{"owner",       required_argument, 0,                'o'},
		{"password",    required_argument, 0,                'p'},
		{"servername",  required_argument, 0,                's'},
		{"verbose",	required_argument, 0,                'v'},
		{"help",              no_argument, 0,	             'h'},
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long(argc, argv, "lrf:d:n:o:p:s:v:h",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'l': {
			opt.o_latest = 1;
			break;
		}
		case 'r': {
			opt.o_recursive = 1;
			set_recursive(bTrue);
			break;
		}
		case 'f': {
			strncpy(opt.o_fsname, optarg,
				strlen(optarg) < DSM_MAX_FSNAME_LENGTH ?
				strlen(optarg) : DSM_MAX_FSNAME_LENGTH);
			break;
		}
		case 'd': {
			strncpy(opt.o_desc, optarg,
				strlen(optarg) < DSM_MAX_DESCR_LENGTH ?
				strlen(optarg) : DSM_MAX_DESCR_LENGTH);
			break;
		}
		case 'n': {
			strncpy(opt.o_node, optarg,
				strlen(optarg) < DSM_MAX_NODE_LENGTH ?
				strlen(optarg) : DSM_MAX_NODE_LENGTH);
			break;
		}
		case 'o': {
			strncpy(opt.o_owner, optarg,
				strlen(optarg) < DSM_MAX_OWNER_LENGTH ?
				strlen(optarg) : DSM_MAX_OWNER_LENGTH);
			break;
		}
		case 'p': {
			strncpy(opt.o_password, optarg,
				strlen(optarg) < DSM_MAX_VERIFIER_LENGTH ?
				strlen(optarg) : DSM_MAX_VERIFIER_LENGTH);
			break;
		}
		case 's': {
			strncpy(opt.o_servername, optarg,
				strlen(optarg) < DSM_MAX_SERVERNAME_LENGTH ?
				strlen(optarg) : DSM_MAX_SERVERNAME_LENGTH);
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

	sanity_arg_check(&opt, argv[0]);

	return 0;
}

int main(int argc, char *argv[])
{
	int rc;
	api_msg_set_level(opt.o_verbose);
	rc = parseopts(argc, argv);
	if (rc) {
		CT_WARN("try '%s --help' for more information", argv[0]);
		return 1;
	}

	size_t num_files_dirs = 0;
	char **files_dirs_arg = NULL;

	/* file or directory parameter */
	if (optind < argc) {
		size_t i = 0;
		num_files_dirs = argc - optind;
		files_dirs_arg = malloc(sizeof(char *) * num_files_dirs);
		while (optind < argc) {
			size_t len = strlen(argv[optind]);
			files_dirs_arg[i] = calloc(len + 1, sizeof(char));
			(files_dirs_arg[i])[len] = '\0';
			strncpy(files_dirs_arg[i++], argv[optind++], len);
		}
	}

	struct login_t login;
	login_fill(&login, opt.o_servername,
		   opt.o_node, opt.o_password,
		   opt.o_owner, LINUX_PLATFORM,
		   opt.o_fsname, DEFAULT_FSTYPE);

	struct session_t session;
	bzero(&session, sizeof(session));
	session.qtable.multiple = opt.o_latest == 1 ? bFalse : bTrue;

	rc = tsm_init(DSM_SINGLETHREAD);
	if (rc)
		goto cleanup;

	rc = tsm_connect(&login, &session);
	if (rc)
		goto cleanup;

	rc = tsm_query_session(&session);
	if (rc)
		goto cleanup;

	/* Handle operations on files and directories resp. */
	for (size_t i = 0; i < num_files_dirs &&
		     files_dirs_arg[i]; i++) {
		if (opt.o_query)
			rc = tsm_query_fpath(opt.o_fsname, files_dirs_arg[i],
					     opt.o_desc, &session);
		else if (opt.o_retrieve)
			rc = tsm_retrieve_fpath(opt.o_fsname, files_dirs_arg[i],
						opt.o_desc, -1, &session);
		else if (opt.o_delete)
			rc = tsm_delete_fpath(opt.o_fsname, files_dirs_arg[i],
					      &session);
		else if (opt.o_archive)
			rc = tsm_archive_fpath(opt.o_fsname,
					       files_dirs_arg[i],
					       opt.o_desc, -1, NULL, &session);
		if (rc)
			goto cleanup;
	}

cleanup:
	if (files_dirs_arg) {
		for (size_t i = 0; i < num_files_dirs; i++) {
			if (files_dirs_arg[i])
				free(files_dirs_arg[i]);
		}
		free(files_dirs_arg);
	}

	tsm_disconnect(&session);
	tsm_cleanup(DSM_SINGLETHREAD);

	return rc;
}
