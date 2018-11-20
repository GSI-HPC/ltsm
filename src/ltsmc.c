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
 * Copyright (c) 2016, 2017, GSI Helmholtz Centre for Heavy Ion Research
 */

#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <getopt.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <zlib.h>
#include "tsmapi.h"
#include "measurement.h"

MSRT_DECLARE(tsm_archive_fpath);
MSRT_DECLARE(tsm_retrieve_fpath);

struct options {
	int o_archive;
	int o_retrieve;
	int o_query;
	int o_delete;
	int o_pipe;
	int o_verbose;
	int o_latest;
	int o_recursive;
	int o_checksum;
	int o_sort;
	char o_servername[DSM_MAX_SERVERNAME_LENGTH + 1];
	char o_node[DSM_MAX_NODE_LENGTH + 1];
	char o_owner[DSM_MAX_OWNER_LENGTH + 1];
	char o_password[DSM_MAX_VERIFIER_LENGTH + 1];
	char o_fsname[DSM_MAX_FSNAME_LENGTH + 1];
	char o_fstype[DSM_MAX_FSTYPE_LENGTH + 1];
	char o_desc[DSM_MAX_DESCR_LENGTH + 1];
	char o_conf[MAX_OPTIONS_LENGTH + 1];
	dsmDate o_date_lower_bound;
	dsmDate o_date_upper_bound;
};

struct options opt = {
	.o_verbose	    = API_MSG_NORMAL,
	.o_servername	    = {0},
	.o_node		    = {0},
	.o_owner	    = {0},
	.o_password	    = {0},
	.o_fsname	    = {0},
	.o_fstype	    = {0},
	.o_desc		    = {0},
	.o_conf		    = {0},
	.o_date_lower_bound = {DATE_MINUS_INFINITE, 1, 1, 0, 0, 0},
	.o_date_upper_bound = {DATE_PLUS_INFINITE, 12, 31, 23, 59, 59},
	.o_checksum	    = 0,
	.o_sort		    = SORT_NONE
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
		"\t--pipe\n"
		"\t--checksum\n"
		"\t-l, --latest [retrieve object with latest timestamp when multiple exists]\n"
		"\t-x, --prefix [retrieve prefix directory]\n"
		"\t-r, --recursive [archive directory and all sub-directories]\n"
		"\t-t, --sort={ascending, descending, restore} [sort query in date or restore order]\n"
		"\t-f, --fsname <string> [default: '/']\n"
		"\t-d, --description <string>\n"
		"\t-n, --node <string>\n"
		"\t-o, --owner <string>\n"
		"\t-p, --password <string>\n"
		"\t-s, --servername <string>\n"
		"\t-v, --verbose {error, warn, message, info, debug} [default: message]\n"
		"\t-c, --conf <file>\n"
		"\t-y, --datelow <string>\n"
		"\t-z, --datehigh <string>\n"
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

static int is_valid(char *arg, const int lower_bound, const int upper_bound)
{
	char *end = NULL;
	int val = strtol(arg, &end, 10);

	if (*end != '\0') {
		CT_ERROR(-EINVAL, "invalid argument: '%s'", arg);
		return -EINVAL;
	}
	if (!(val >= lower_bound && val <= upper_bound)) {
		CT_ERROR(-EINVAL, "invalid argument: '%s'", arg);
		return -EINVAL;
	}

	return val;
}

static int parse_date_time(char *arg, dsmDate *dsm_date)
{
	const char *delim = ":";
	char *token;
	uint16_t cnt = 0;
	int val;

	/* YYYY:MM:DD:hh:mm:ss, e.g. 2018:05:28:23:15:59 */
	token = strtok(arg, delim);
	while (token) {
		if (cnt == 0) { /* Year, 16-bit integer (e.g., 1990). */
			val = is_valid(token, 0, 0xFFFF);
			if (val < 0)
				return val;
			dsm_date->year = val;
		}
		else if (cnt == 1) { /* Month, 8-bit integer (1 - 12). */
			val = is_valid(token, 1, 12);
			if (val < 0)
				return val;
			dsm_date->month = val;
		}
		else if (cnt == 2) { /* Day, 8-bit integer (1 - 31). */
			val = is_valid(token, 1, 31);
			if (val < 0)
				return val;
			dsm_date->day = val;
		}
		else if (cnt == 3) { /* Hour, 8-bit integer (0 - 23). */
			val = is_valid(token, 0, 23);
			if (val < 0)
				return val;
			dsm_date->hour = val;
		}
		else if (cnt == 4) { /* Minute, 8-bit integer (0 - 59). */
			val = is_valid(token, 0, 59);
			if (val < 0)
				return val;
			dsm_date->minute = val;
		}
		else if (cnt == 5) { /* Second, 8-bit integer (0 - 59). */
			val = is_valid(token, 0, 59);
			if (val < 0)
				return val;
			dsm_date->second = val;
		}
		else {
			CT_ERROR(-EINVAL, "invalid argument: '%s'", token);
			return -EINVAL;
		}
		cnt++;
		token = strtok(NULL, delim);
	}
	return (cnt <= 6 ? 0 : -EINVAL);
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
					sizeof(opt.o_servername));
			else if (OPTNCMP("node", kv_opt.kv[n].key))
				strncpy(opt.o_node, kv_opt.kv[n].val,
					sizeof(opt.o_node));
			else if (OPTNCMP("owner", kv_opt.kv[n].key))
				strncpy(opt.o_owner, kv_opt.kv[n].val,
					sizeof(opt.o_owner));
			else if (OPTNCMP("password", kv_opt.kv[n].key))
				strncpy(opt.o_password, kv_opt.kv[n].val,
					sizeof(opt.o_password));
			else if (OPTNCMP("fsname", kv_opt.kv[n].key))
				strncpy(opt.o_fsname, kv_opt.kv[n].val,
					sizeof(opt.o_fsname));
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

static void sanity_arg_check(const char *argv)
{
	uint8_t count = 0;
	count = opt.o_archive  == 1 ? count + 1 : count;
	count = opt.o_retrieve == 1 ? count + 1 : count;
	count = opt.o_delete   == 1 ? count + 1 : count;
	count = opt.o_query    == 1 ? count + 1 : count;
	count = opt.o_checksum == 1 ? count + 1 : count;
	count = opt.o_pipe     == 1 ? count + 1 : count;

	if (count == 0) {
		CT_ERROR(0, "missing argument --archive, --retrieve,"
			 " --query, --delete, --pipe or --checksum");
		usage(argv, 1);
	} else if (count != 1) {
		CT_ERROR(0, "multiple incompatible arguments"
			 " --archive, --retrieve, --query, --delete, --pipe or "
			 "--checksum");
		usage(argv, 1);
	}

	/* There are no additional required arguments when
	   --checksum is chosen. */
	if (opt.o_checksum)
		return;

	/* Required arguments. */
	if (!opt.o_node[0]) {
		CT_ERROR(0, "missing argument -n, --node <string>");
		usage(argv, 1);
	} else if (!opt.o_password[0]) {
		CT_ERROR(0, "missing argument -p, --password <string>");
		usage(argv, 1);
	} else if (!opt.o_servername[0]) {
		CT_ERROR(0, "missing argument -s, --servername <string>");
		usage(argv, 1);
	} else if (!opt.o_fsname[0])
		strncpy(opt.o_fsname, DEFAULT_FSNAME, DSM_MAX_FSNAME_LENGTH);
}

static int parseopts(int argc, char *argv[])
{
	struct option long_opts[] = {
		{"archive",	      no_argument, &opt.o_archive,     1},
		{"retrieve",	      no_argument, &opt.o_retrieve,    1},
		{"query",	      no_argument, &opt.o_query,       1},
		{"delete",	      no_argument, &opt.o_delete,      1},
		{"pipe",              no_argument, &opt.o_pipe,        1},
		{"checksum",          no_argument, &opt.o_checksum,    1},
		{"latest",	      no_argument, 0,		     'l'},
		{"recursive",	      no_argument, 0,		     'r'},
		{"sort",	required_argument, 0,		     't'},
		{"fsname",	required_argument, 0,		     'f'},
		{"description", required_argument, 0,		     'd'},
		{"node",	required_argument, 0,		     'n'},
		{"owner",	required_argument, 0,		     'o'},
		{"password",	required_argument, 0,		     'p'},
		{"servername",	required_argument, 0,		     's'},
		{"verbose",	required_argument, 0,		     'v'},
		{"prefix",	required_argument, 0,		     'x'},
		{"conf",	required_argument, 0,		     'c'},
		{"datelow",	required_argument, 0,		     'y'},
		{"datehigh",	required_argument, 0,		     'z'},
		{"help",	      no_argument, 0,		     'h'},
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long(argc, argv, "lrt:f:d:n:o:p:s:v:x:c:y:z:h",
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
		case 't': {
			if (OPTNCMP("none", optarg))
				opt.o_sort = SORT_NONE;
			else if (OPTNCMP("ascending", optarg))
				opt.o_sort = SORT_DATE_ASCENDING;
			else if (OPTNCMP("descending", optarg))
				opt.o_sort = SORT_DATE_DESCENDING;
			else if (OPTNCMP("restore", optarg))
				opt.o_sort = SORT_RESTORE_ORDER;
			else {
				CT_ERROR(0, "wrong argument for -t, "
					"--sort '%s", optarg);
				usage(argv[0], 1);
			}
			break;
		}
		case 'f': {
			strncpy(opt.o_fsname, optarg, DSM_MAX_FSNAME_LENGTH);
			break;
		}
		case 'd': {
			strncpy(opt.o_desc, optarg, DSM_MAX_DESCR_LENGTH);
			break;
		}
		case 'n': {
			strncpy(opt.o_node, optarg, DSM_MAX_NODE_LENGTH);
			break;
		}
		case 'o': {
			strncpy(opt.o_owner, optarg, DSM_MAX_OWNER_LENGTH);
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
			int rc = parse_verbose(optarg, &opt.o_verbose);
			if (rc) {
				CT_ERROR(0, "wrong argument for -v, "
					 "--verbose '%s'", optarg);
				usage(argv[0], 1);
			}
			break;
		}
		case 'x': {
			set_prefix(optarg);
			break;
		}
		case 'c': {
			read_conf(optarg);
			break;
		}
		case 'y': {
			int rc = parse_date_time(optarg, &opt.o_date_lower_bound);
			if (rc) {
				CT_ERROR(0, "wrong argument for -y, "
					 "--datelow '%s'", optarg);
				usage(argv[0], 1);
			}
			break;
		}
		case 'z': {
			int rc = parse_date_time(optarg, &opt.o_date_upper_bound);
			if (rc) {
				CT_ERROR(0, "wrong argument for -z, "
					 "--datehigh '%s'", optarg);
				usage(argv[0], 1);
			}
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

static int progress_callback(struct progress_size_t *pg_size,
			      struct session_t *session)
{
	MSRT_DATA(tsm_archive_fpath, pg_size->cur);
	MSRT_DATA(tsm_retrieve_fpath, pg_size->cur);

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

	if (opt.o_checksum) {

		if (num_files_dirs == 0) {
			CT_ERROR(0, "missing argument <files>");
			usage(argv[0], 1);
		}

		uint32_t crc32sum = 0;
		for (size_t i = 0; i < num_files_dirs &&
			     files_dirs_arg[i]; i++) {
			rc = crc32file(files_dirs_arg[i], &crc32sum);
			if (rc)
				CT_WARN("calculation of crc32 for '%s' failed",
					files_dirs_arg[i]);
			else
				fprintf(stdout, "crc32: "
					"0x%08x (%010u), file: '%s'\n", crc32sum,
					crc32sum, files_dirs_arg[i]);
		}
		goto cleanup;
	}

	struct login_t login;
	login_fill(&login, opt.o_servername,
		   opt.o_node, opt.o_password,
		   opt.o_owner, LINUX_PLATFORM,
		   opt.o_fsname, DEFAULT_FSTYPE);

	struct session_t session;
	bzero(&session, sizeof(session));

	session.qtable.multiple = opt.o_latest == 1 ? bFalse : bTrue;
	session.qtable.sort_by = opt.o_sort;

	rc = tsm_init(DSM_SINGLETHREAD);
	if (rc)
		goto cleanup_tsm;

	if (opt.o_pipe) {
		if (num_files_dirs == 0) {
			CT_ERROR(0, "missing argument <files>");
			usage(argv[0], 1);
		} else if (num_files_dirs > 1) {
			CT_ERROR(0, "too many arguments <files>");
			usage(argv[0], 1);
		}

		rc = tsm_fconnect(&login, &session);
		if (rc)
			goto cleanup;

		rc = tsm_fopen(opt.o_fsname, files_dirs_arg[0], opt.o_desc,
			       &session);
		if (rc) {
			tsm_cleanup(DSM_SINGLETHREAD);
			goto cleanup;
		}

		char buf[TSM_BUF_LENGTH] = {0};
		size_t size;
		do {
			size = fread(buf, 1, TSM_BUF_LENGTH, stdin);
			if (ferror(stdin)) {
				session.tsm_file->err = EIO;
				CT_ERROR(EIO, "fread failed");
				break;
			}
			if (size == 0)
				break;
			rc = tsm_fwrite(buf, 1, size, &session);
			if (rc < 0) {
				CT_ERROR(errno, "tsm_fwrite failed");
				break;
			}
		} while (!feof(stdin));

		rc = tsm_fclose(&session);
		if (rc)
			CT_ERROR(errno, "tsm_fclose failed");

		tsm_cleanup(DSM_SINGLETHREAD);
		goto cleanup;
	}

	session.progress = progress_callback;

	rc = tsm_connect(&login, &session);
	if (rc)
		goto cleanup_tsm;

	rc = tsm_query_session(&session);
	if (rc)
		goto cleanup_tsm;

	/* Handle operations on files and directories resp. */
	for (size_t i = 0; i < num_files_dirs &&
		     files_dirs_arg[i]; i++) {
		if (opt.o_query)
			rc = tsm_query_fpath(opt.o_fsname, files_dirs_arg[i],
					     opt.o_desc, &opt.o_date_lower_bound,
					     &opt.o_date_upper_bound, &session);
		else if (opt.o_retrieve) {
			MSRT_START(tsm_retrieve_fpath);
			rc = tsm_retrieve_fpath(opt.o_fsname, files_dirs_arg[i],
						opt.o_desc, -1, &session);
			MSRT_STOP(tsm_retrieve_fpath);
			MSRT_DISPLAY_RESULT(tsm_retrieve_fpath);
		}
		else if (opt.o_delete)
			rc = tsm_delete_fpath(opt.o_fsname, files_dirs_arg[i],
					      &session);
		else if (opt.o_archive) {
			MSRT_START(tsm_archive_fpath);
			rc = tsm_archive_fpath(opt.o_fsname,
					       files_dirs_arg[i],
					       opt.o_desc, -1, NULL, &session);
			MSRT_STOP(tsm_archive_fpath);
			MSRT_DISPLAY_RESULT(tsm_archive_fpath);
		}
		if (rc)
			goto cleanup_tsm;
	}

cleanup_tsm:
	tsm_disconnect(&session);
	tsm_cleanup(DSM_SINGLETHREAD);

cleanup:
	if (files_dirs_arg) {
		for (size_t i = 0; i < num_files_dirs; i++) {
			if (files_dirs_arg[i])
				free(files_dirs_arg[i]);
		}
		free(files_dirs_arg);
	}

	return rc;
}
