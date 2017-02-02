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

#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <getopt.h>
#include <sys/stat.h>
#include "tsmapi.h"

#ifndef PACKAGE_VERSION
#define PACKAGE_VERSION "NA"
#endif

/* Arguments */
static dsBool_t a_arg = bFalse;
static dsBool_t r_arg = bFalse;
static dsBool_t q_arg = bFalse;
static dsBool_t d_arg = bFalse;
static char f_arg[DSM_MAX_FSNAME_LENGTH + 1] = {0};
static char c_arg[DSM_MAX_DESCR_LENGTH + 1] = {0};
static char n_arg[DSM_MAX_NODE_LENGTH + 1] = {0};
static char o_arg[DSM_MAX_OWNER_LENGTH + 1] = {0};
static char p_arg[DSM_MAX_VERIFIER_LENGTH + 1] = {0};
static char s_arg[DSM_MAX_SERVERNAME_LENGTH + 1] = {0};
static char **files_dirs_arg = NULL;

void usage(const char *cmd_name)
{
	dsmApiVersionEx libapi_ver = get_libapi_ver();
	dsmAppVersion appapi_ver = get_appapi_ver();

	printf("Syntax: %s\n"
	       "\t-a, --archive\n"
	       "\t-r, --retrieve\n"
	       "\t-q, --query\n"
	       "\t-d, --delete\n"
	       "\t-l, --latest\n"
	       "\t-i, --recursive [archive]\n"
	       "\t-f, --fsname <STRING> [default '/']\n"
	       "\t-c, --description <STRING>\n"
	       "\t-n, --node <STRING>\n"
	       "\t-o, --owner <STRING>\n"
	       "\t-p, --password <STRING>\n"
	       "\t-s, --servername <STRING>\n"
	       "\t-v, --verbose [optional level <v,vv,vvv>]\n"
	       "\nVersion: %s © by Thomas Stibor <t.stibor@gsi.de>\n"
	       "IBM API library version: %d.%d.%d.%d, "
	       "IBM API application client version: %d.%d.%d.%d\n",
	       cmd_name, PACKAGE_VERSION,
	       libapi_ver.version, libapi_ver.release, libapi_ver.level,
	       libapi_ver.subLevel,
	       appapi_ver.applicationVersion, appapi_ver.applicationRelease,
	       appapi_ver.applicationLevel, appapi_ver.applicationSubLevel);

	exit(DSM_RC_UNSUCCESSFUL);
}

void sanity_arg_check(const char *cmd_name)
{
	unsigned int count = 0;
	count = a_arg == bTrue ? count + 1 : count;
	count = r_arg == bTrue ? count + 1 : count;
	count = q_arg == bTrue ? count + 1 : count;
	count = d_arg == bTrue ? count + 1 : count;

	if (count != 1) {
		printf("missing or wrong argument combinations of "
		       "archive, retrieve, query or delete\n");
		usage(cmd_name);
	}

	if (strlen(n_arg) == 0) {
		printf("missing argument: -n, --node\n");
		usage(cmd_name);
	} else if (strlen(p_arg) == 0) {
		printf("missing argument: -p, --password\n");
		usage(cmd_name);
	} else if (strlen(s_arg) == 0) {
		printf("missing argument: -s, --servername\n");
		usage(cmd_name);
	} /* Owner (arg_o) is not necessarily required. */
}

int main(int argc, char *argv[])
{
	int c;

	api_msg_set_level(API_MSG_NORMAL);
	set_recursive(bFalse);
	strncpy(f_arg, FSNAME, strlen(FSNAME));
	while (1) {
		static struct option long_options[] = {
			{"archive",           no_argument, 0, 'a'},
			{"retrieve",          no_argument, 0, 'r'},
			{"query",             no_argument, 0, 'q'},
			{"delete",            no_argument, 0, 'd'},
			{"latest",            no_argument, 0, 'l'},
			{"recursive",         no_argument, 0, 'i'},
			{"fsname",      optional_argument, 0, 'f'},
			{"description", required_argument, 0, 'c'},
			{"node",        required_argument, 0, 'n'},
			{"owner",       required_argument, 0, 'o'},
			{"password",    required_argument, 0, 'p'},
			{"servername",  required_argument, 0, 's'},
			{"verbose",	required_argument, 0, 'v'},
			{0, 0, 0, 0}
		};
		/* getopt_long stores the option index here. */
		int option_index = 0;

		c = getopt_long (argc, argv, "arqdlif:c:n:o:p:s:v::",
				 long_options, &option_index);

		/* Detect the end of the options. */
		if (c == -1)
			break;

		switch (c) {
		case 'a':	/* archive */
			a_arg = bTrue;
			break;
		case 'r':	/* retrieve */
			r_arg = bTrue;
			break;
		case 'q':	/* query */
			q_arg = bTrue;
			break;
		case 'd':	/* delete */
			d_arg = bTrue;
			break;
		case 'l':	/* latest */
			select_latest(bTrue);
			break;
		case 'i':	/* recursive */
			set_recursive(bTrue);
			break;
		case 'f':	/* fsname */
			strncpy(f_arg, optarg,
				strlen(optarg) < DSM_MAX_FSNAME_LENGTH ?
				strlen(optarg) : DSM_MAX_FSNAME_LENGTH);
			break;
		case 'c':	/* description */
			strncpy(c_arg, optarg,
				strlen(optarg) < DSM_MAX_DESCR_LENGTH ?
				strlen(optarg) : DSM_MAX_DESCR_LENGTH);
			break;
		case 'n':	/* node */
			strncpy(n_arg, optarg,
				strlen(optarg) < DSM_MAX_NODE_LENGTH ?
				strlen(optarg) : DSM_MAX_NODE_LENGTH);
			break;
		case 'o':	/* owner */
			strncpy(o_arg, optarg,
				strlen(optarg) < DSM_MAX_OWNER_LENGTH ?
				strlen(optarg) : DSM_MAX_OWNER_LENGTH);
			break;
		case 'p':	/* password */
			strncpy(p_arg, optarg,
				strlen(optarg) < DSM_MAX_VERIFIER_LENGTH ?
				strlen(optarg) : DSM_MAX_VERIFIER_LENGTH);
			break;
		case 's':	/* servername */
			strncpy(s_arg, optarg,
				strlen(optarg) < DSM_MAX_SERVERNAME_LENGTH ?
				strlen(optarg) : DSM_MAX_SERVERNAME_LENGTH);
			break;
		case 'v': {	/* verbosity */
			if (!optarg)
				api_msg_set_level(API_MSG_INFO);
			else {
				if (strlen(optarg) == 1 && strncmp(optarg, "v", 1) == 0)
					api_msg_set_level(API_MSG_DEBUG);
				else if (strlen(optarg) == 2 && strncmp(optarg, "vv", 2) == 0)
					api_msg_set_level(API_MSG_MAX);
				else {
					printf("unknown verbose parameter: %s\n", optarg);
					usage(argv[0]);
				}
			}
			break;
		}
		default:
			usage(argv[0]);
		}
	}

	argc == 1 ? usage(argv[0]) : sanity_arg_check(argv[0]);

	size_t num_files_dirs = 0;

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

	login_t login;
	memset(&login, 0, sizeof(login));
	strcpy(login.node, n_arg);
	strcpy(login.password, p_arg);
	strcpy(login.owner, o_arg);
	strcpy(login.platform, LOGIN_PLATFORM);
	strcpy(login.fsname, f_arg);
	strcpy(login.fstype, FSTYPE);
	const unsigned short s_arg_len = 1 + strlen(s_arg) + strlen("-se=");
	if (s_arg_len < MAX_OPTIONS_LENGTH)
		snprintf(login.options, s_arg_len, "-se=%s", s_arg);
	else
		CT_WARN("Option parameter \'-se=%s\' is larger than "
			"MAX_OPTIONS_LENGTH: %d and is ignored\n",
			s_arg, MAX_OPTIONS_LENGTH);

	dsInt16_t rc;

	rc = tsm_init(&login);
	if (rc)
		goto clean_up;

	rc = tsm_query_session_info();
	if (rc)
		goto clean_up;

	/* Handle operations on files and directories resp. */
	for (size_t i = 0; i < num_files_dirs &&
		     files_dirs_arg[i]; i++) {
		if (q_arg)	/* Query. */
			rc = tsm_query_fpath(f_arg, files_dirs_arg[i],
					     c_arg, bTrue);
		else if (r_arg)	/* Retrieve. */
			rc = tsm_retrieve_fpath(f_arg, files_dirs_arg[i],
						c_arg, -1);
		else if (d_arg)	/* Delete. */
			rc = tsm_delete_fpath(f_arg, files_dirs_arg[i]);
		else if (a_arg) {	/* Archive. */
			rc = tsm_archive_fpath(f_arg,
					       files_dirs_arg[i],
					       c_arg, -1, NULL);
		}
		if (rc)
			goto clean_up;
	}

clean_up:
	if (files_dirs_arg) {
		for (size_t i = 0; i < num_files_dirs; i++) {
			if (files_dirs_arg[i])
				free(files_dirs_arg[i]);
		}
		free(files_dirs_arg);
	}

	tsm_quit();

	return rc;
}
