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
#include "log.h"

/* Arguments */
static dsBool_t a_arg = bFalse;
static dsBool_t r_arg = bFalse;
static dsBool_t q_arg = bFalse;
static dsBool_t d_arg = bFalse;
static char f_arg[DSM_MAX_FSNAME_LENGTH + 1] = {0};
static char h_arg[DSM_MAX_HL_LENGTH + 1] = {0};
static char l_arg[DSM_MAX_LL_LENGTH + 1] = {0};
static char c_arg[DSM_MAX_DESCR_LENGTH + 1] = {0};
static char n_arg[DSM_MAX_NODE_LENGTH + 1] = {0};
static char u_arg[MAX_USERNAME_LENGTH + 1] = {0};
static char p_arg[MAX_PASSWORD_LENGTH + 1] = {0};
static char s_arg[MAX_OPTIONS_LENGTH + 1] = {0};
static char **files_dirs_arg = NULL;

void usage(const char *prg_name)
{
    printf("Syntax: %s\n"
	   "\t-a, --archive\n"
	   "\t-r, --retrieve\n"
	   "\t-q, --query\n"
	   "\t-d, --delete\n"
	   "\t-f, --fsname <STRING>\n"
	   "\t-h, --hl <STRING>\n"
	   "\t-l, --ll <STRING>\n"
	   "\t-c, --description <STRING>\n"
	   "\t-n, --node <STRING>\n"
	   "\t-u, --username <STRING>\n"
	   "\t-p, --password <STRING>\n"
	   "\t-s, --servername <STRING>\n"
	   "\nVersion: %s,© Thomas Stibor <t.stibor@gsi.de>\n",
	   prg_name, VERSION);

    exit(DSM_RC_UNSUCCESSFUL);
}

dsBool_t isset_hl_ll()
{
    return (strlen(h_arg) && strlen(l_arg));
}

void sanity_arg_check(const char *prg_name)
{
    unsigned int count = 0;
    count = a_arg == bTrue ? count + 1 : count;
    count = r_arg == bTrue ? count + 1 : count;
    count = q_arg == bTrue ? count + 1 : count;
    count = d_arg == bTrue ? count + 1 : count;

    if (count != 1) {
	printf("missing or wrong argument combinations of "
	       "archive, retrieve, query or delete\n");
	usage(prg_name);
    }

    if (strlen(f_arg) == 0) {
	printf("missing argument: -f, --fsname\n");
	usage(prg_name);
    } else if (strlen(n_arg) == 0) {
	printf("missing argument: -n, --node\n");
	usage(prg_name);
    } else if (strlen(p_arg) == 0) {
	printf("missing argument: -p, --password\n");
	usage(prg_name);
    } else if (strlen(s_arg) == 0) {
	printf("missing argument: -s, --servername\n");
	usage(prg_name);
    } /* Username (arg_u) is not necessarily required. */
}

#if 0
int create_test_objdesc(const char *filename, char *result)
{
    int rc;
    struct stat st_buf;

    if (!result)
	return DSM_RC_UNSUCCESSFUL;
    
    rc = stat(filename, &st_buf);
    if (rc) {
	ERR_MSG("stat");
	return DSM_RC_UNSUCCESSFUL;
    }
    
    snprintf(result, DSM_MAX_OBJINFO_LENGTH,
	     "inode:%zu,uid:%d,gid:%d", st_buf.st_ino, st_buf.st_uid, st_buf.st_gid);

    return DSM_RC_SUCCESSFUL;
}
#endif

dsBool_t is_valid_dir(const char *filepath)
{
    int rc;;
    struct stat st_buf;

    rc = stat(filepath, &st_buf);
    if (rc) {
	ERR_MSG("stat");
	return bFalse;
    }
    
    return (S_ISDIR(st_buf.st_mode)) ? bTrue : bFalse;
}

int main(int argc, char *argv[])
{
    int c;

    while (1) {
      static struct option long_options[] = {
          {"archive",           no_argument, 0, 'a'},
          {"retrieve",          no_argument, 0, 'r'},
          {"query",             no_argument, 0, 'q'},
	  {"delete",            no_argument, 0, 'd'},
          {"fsname",      required_argument, 0, 'f'},
          {"hl",          required_argument, 0, 'h'},
	  {"ll",          required_argument, 0, 'l'},
	  {"description", required_argument, 0, 'c'},	  
	  {"node",        required_argument, 0, 'n'},
	  {"username",    required_argument, 0, 'u'},
	  {"password",    required_argument, 0, 'p'},
	  {"servername",  required_argument, 0, 's'},
          {0, 0, 0, 0}
      };
      /* getopt_long stores the option index here. */
      int option_index = 0;

      c = getopt_long (argc, argv, "arqdf:h:l:c:n:u:p:s:",
                       long_options, &option_index);

      /* Detect the end of the options. */
      if (c == -1)
        break;

      switch (c) {
      case 'a':			/* archive */
	  VERBOSE_MSG("option -a, --archive\n");
	  a_arg = bTrue;
          break;
      case 'r':			/* retrieve */
	  VERBOSE_MSG("option -r, --retrieve\n");
	  r_arg = bTrue;
          break;
      case 'q':			/* query */
	  VERBOSE_MSG("option -q, --query\n");
	  q_arg = bTrue;
          break;
      case 'd':			/* delete */
	  VERBOSE_MSG("option -d, --delete\n");
	  d_arg = bTrue;
          break;
      case 'f':			/* fsname */
	  VERBOSE_MSG("option -f, --fsname=%s\n", optarg);
	  strncpy(f_arg, optarg, strlen(optarg) < DSM_MAX_FSNAME_LENGTH ? strlen(optarg) : DSM_MAX_FSNAME_LENGTH);
          break;
      case 'h':			/* hl */
	  VERBOSE_MSG("option -h, --hl=%s\n", optarg);
	  strncpy(h_arg, optarg, strlen(optarg) < DSM_MAX_HL_LENGTH ? strlen(optarg) : DSM_MAX_HL_LENGTH);
          break;
      case 'l':			/* ll */
	  VERBOSE_MSG("option -l, --ll=%s\n", optarg);
	  strncpy(l_arg, optarg, strlen(optarg) < DSM_MAX_LL_LENGTH ? strlen(optarg) : DSM_MAX_LL_LENGTH);
          break;
      case 'c':			/* description */
	  VERBOSE_MSG("option -c, --description=%s\n", optarg);
	  strncpy(c_arg, optarg, strlen(optarg) < DSM_MAX_DESCR_LENGTH ? strlen(optarg) : DSM_MAX_DESCR_LENGTH);
          break;
      case 'n':			/* node */
	  VERBOSE_MSG("option -n, --node=%s\n", optarg);
	  strncpy(n_arg, optarg, strlen(optarg) < DSM_MAX_NODE_LENGTH ? strlen(optarg) : DSM_MAX_NODE_LENGTH);
          break;
      case 'u':			/* username */
	  VERBOSE_MSG("option -u, --username=%s\n", optarg);
	  strncpy(u_arg, optarg, strlen(optarg) < MAX_USERNAME_LENGTH ? strlen(optarg) : MAX_USERNAME_LENGTH);
          break;
      case 'p':			/* password */
	  VERBOSE_MSG("option -p, --password=%s\n", optarg);
	  strncpy(p_arg, optarg, strlen(optarg) < MAX_PASSWORD_LENGTH ? strlen(optarg) : MAX_PASSWORD_LENGTH);
          break;
      case 's':			/* servername */
	  VERBOSE_MSG("option -s, --servername=%s\n", optarg);
	  strncpy(s_arg, optarg, strlen(optarg) < MAX_OPTIONS_LENGTH ? strlen(optarg) : MAX_OPTIONS_LENGTH);
          break;
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
    strcpy(login.username, u_arg);
    strcpy(login.platform, "GNU/Linux");
    const unsigned short s_arg_len = 1 + strlen(s_arg) + strlen("-se=");
    if (s_arg_len < MAX_OPTIONS_LENGTH)
	snprintf(login.options, s_arg_len, "-se=%s", s_arg);
    else
	WARN_MSG("Option parameter \'-se=%s\' is larger than "
		 "MAX_OPTIONS_LENGTH: %d and is ignored\n",
		 s_arg, MAX_OPTIONS_LENGTH);

    dsInt16_t rc;
    rc = tsm_init(&login);
    if (rc)
        goto clean_up;

    rc = tsm_query_session_info();
    if (rc)
	goto clean_up;

    /* Handle operations with arguments --hl and --ll. */
    if (isset_hl_ll()) {
	if (q_arg)		/* Query. */
	    rc = tsm_query_hl_ll(f_arg, h_arg, l_arg, c_arg, bTrue);
	else if (r_arg)		/* Retrieve. */
	    rc = tsm_retrieve_hl_ll(f_arg, h_arg, l_arg, c_arg);
	else if (d_arg)		/* Delete. */
	    rc = tsm_delete_hl_ll(f_arg, h_arg, l_arg);
	else {
	    printf("Arguments --hl and --ll provided however --query, --delete or --retrieve is missing\n");
	    usage(argv[0]);
	}
    } else { /* Handle operations on files and directories resp. */
	for (size_t i = 0; i < num_files_dirs && files_dirs_arg[i]; i++) {
	    if (q_arg)		/* Query. */
		rc = tsm_query_file(f_arg, files_dirs_arg[i], c_arg, bTrue);
	    else if (r_arg)	/* Retrieve. */
		rc = tsm_retrieve_file(f_arg, files_dirs_arg[i], c_arg);
	    else if (d_arg)	/* Delete. */
		rc = tsm_delete_file(f_arg, files_dirs_arg[i]);
	    else if (a_arg) {	/* Archive. */
		rc = is_valid_dir(files_dirs_arg[i]) ? tsm_archive_dir(f_arg, files_dirs_arg[i], c_arg) :
		    tsm_archive_file(f_arg, files_dirs_arg[i], c_arg);
	    }
	    if (rc)
		goto clean_up;
	}
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
}

