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

#if HAVE_CONFIG_H
#include <config.h>
#endif
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <getopt.h>
#include <sys/stat.h>
#include "tsmapi.h"
#include "tsmfileapi.h"

struct tsm_filehandle_t filehandle;

struct options {
	int o_verbose;
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

	CT_MESSAGE("\nqueries file if no pipe input is given and prints"
		" it to stdout\n"
		"if pipe input is given it will directly written to "
		"specified file\n"
		"usage: %s [options] <files|directories|wildcards>\n"
		"\t-f, --fsname <string> [default: '/']\n"
		"\t-n, --node <string>\n"
		"\t-o, --owner <string>\n"
		"\t-p, --password <string>\n"
		"\t-s, --servername <string>\n"
		"\t-v, --verbose {error, warn, message, info, debug} "
		"[default: message]\n"
		"\t-h, --help\n"
		"\nIBM API library version: %d.%d.%d.%d, "
		"IBM API application client version: %d.%d.%d.%d\n"
		"version: %s © 2017 by Thomas Stibor <t.stibor@gsi.de>,"
		" Jörg Behrendt <j.behrendt@gsi.de>",
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
		CT_ERROR(1, "missing argument -n, --node <string>");
		usage(argv, 1);
	} else if (!strlen(opt.o_password)) {
		CT_ERROR(1, "missing argument -p, --password <string>");
		usage(argv, 1);
	} else if (!strlen(opt.o_servername)) {
		CT_ERROR(1, "missing argument -s, --servername "
			"<string>");
		usage(argv, 1);
	} else if (!strlen(opt.o_fsname)) {
		strncpy(opt.o_fsname, DEFAULT_FSNAME, strlen(DEFAULT_FSNAME));
	}
}

static int parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{"fsname",      required_argument, 0,                'f'},
		{"node",        required_argument, 0,                'n'},
		{"owner",       required_argument, 0,                'o'},
		{"password",    required_argument, 0,                'p'},
		{"servername",  required_argument, 0,                's'},
		{"description", required_argument, 0,                'd'},
		{"verbose",	required_argument, 0,                'v'},
		{"help",              no_argument, 0,	             'h'},
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long(argc, argv, "f:d:n:o:p:s:v:h",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'f': {
			strncpy(opt.o_fsname, optarg,
				strlen(optarg) < DSM_MAX_FSNAME_LENGTH ?
				strlen(optarg) : DSM_MAX_FSNAME_LENGTH);
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
		case 'd': {
			strncpy(opt.o_desc, optarg,
				strlen(optarg) < DSM_MAX_DESCR_LENGTH ?
				strlen(optarg) : DSM_MAX_DESCR_LENGTH);
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
				CT_ERROR(1,"wrong argument for -v, "
					"--verbose '%s'", optarg);
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

int write_stdin_to_tsmfile(){
	int rc = DSM_RC_UNSUCCESSFUL;
	int blocksize = TSM_BUF_LENGTH;
	void *buffer = malloc(blocksize);
	if (buffer == NULL) {
		rc = DSM_RC_UNSUCCESSFUL;
		CT_ERROR(rc, "Buffer allocation failed");
		return rc;
	}
	while (1) {
		size_t size = fread(buffer, 1, blocksize, stdin);
		CT_DEBUG("Got %lu from stdin", size);
		if (size == 0) break;
		rc = tsm_file_write(&filehandle, buffer, 1, size);
		if (rc) {
			CT_ERROR(rc, "tsm_file_write failed");
			break;
		}
	}
	free(buffer);
	return rc;
}

int read_tsmfile_to_stdout(){
	int rc;
	int blocksize = TSM_BUF_LENGTH;
	char *buffer = malloc(blocksize);
	if (buffer == NULL) {
		rc = DSM_RC_UNSUCCESSFUL;
		CT_ERROR(rc, "Buffer allocation failed");
		return rc;
	}
	rc = DSM_RC_MORE_DATA;
	while (rc == DSM_RC_MORE_DATA) {
		size_t bytes_read = 0;
		rc = tsm_file_read(&filehandle, buffer, 1,
				   blocksize, &bytes_read);
		fwrite(buffer, bytes_read, 1, stdout);
	}
	free(buffer);
	if (rc == DSM_RC_FINISHED) return 0;
	CT_ERROR(rc, "tsm_file_read failed");
	return rc;
}

int main(int argc, char *argv[])
{
	int rc = 0;
	api_msg_set_level(opt.o_verbose);

	rc = parseopts(argc, argv);
	if (rc){
		CT_ERROR(1,"try '%s --help' for more information", argv[0]);
		return 1;
	}
	if ((optind+1) != argc ) {
		CT_ERROR(1,"missing file argument");
		usage(argv[0], 1);
		return 1;
	}
	char* filename = argv[optind++];

	tsm_init(DSM_SINGLETHREAD);
	struct login_t login;
	login_fill(&login, opt.o_servername,
		   opt.o_node, opt.o_password,
		   opt.o_owner, LINUX_PLATFORM,
		   opt.o_fsname, DEFAULT_FSTYPE);

	if (isatty(fileno(stdin))) {
		rc = tsm_file_open(&filehandle, &login, filename,
				   opt.o_desc, TSM_FILE_MODE_READ);
		if (rc) {
			CT_ERROR(rc, "Cannot create filehandle for file %s",
				 filename);
			goto cleanup;
		}
		rc = read_tsmfile_to_stdout();
		if (rc) {
			CT_ERROR(rc, "Reading from tsm file %s return error",
				 filename);
		} else {
			CT_MESSAGE("Reading from tsm file %s successfull",
				   filename);
		}
	} else {
		rc = tsm_file_open(&filehandle, &login, filename, opt.o_desc,
			           TSM_FILE_MODE_WRITE);
		if (rc) {
			CT_ERROR(rc, "Cannot create filehandle for file %s",
				 filename);
			goto cleanup;
		}
		rc = write_stdin_to_tsmfile();
		if (rc) {
			CT_ERROR(rc, "Writing stdin to tsm file %s return "
				 "error", filename);
		} else {
			CT_MESSAGE("Writing to tsm file %s successfull",
				    filename);
		}
	}
	tsm_file_close(&filehandle);
cleanup:
	dsmCleanUp(DSM_SINGLETHREAD);

	return rc;
}
