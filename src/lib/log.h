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
 * Copyright (c) 2016, GSI Helmholtz Centre for Heavy Ion Research
 */

#ifndef LOG_H
#define LOG_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/time.h>
#include <linux/limits.h>

#define LOG_LEVEL_HUMAN_STR(s)		              \
	s == API_MSG_OFF    ? "off"    :	      \
	s == API_MSG_FATAL  ? "fatal"  :	      \
	s == API_MSG_ERROR  ? "error"  :	      \
	s == API_MSG_WARN   ? "warn"   :	      \
	s == API_MSG_NORMAL ? "normal" :	      \
	s == API_MSG_INFO   ? "info"   :	      \
	s == API_MSG_DEBUG  ? "debug"  :	      \
        s == API_MSG_MAX    ? "max"    : "UNKNOWN"    \

#define EFAILED 0
#define UNUSED(x) (void)(x)

/* Bottom three bits reserved for api_message_level. */
#define API_MSG_MASK		0x00000007
#define API_MSG_NO_ERRNO	0x00000010

enum api_message_level {
	API_MSG_OFF    = 0,
	API_MSG_FATAL  = 1,
	API_MSG_ERROR  = 2,
	API_MSG_WARN   = 3,
	API_MSG_NORMAL = 4,
	API_MSG_INFO   = 5,
	API_MSG_DEBUG  = 6,
	API_MSG_MAX
};

int api_msg_get_level(void);
void api_msg_set_level(int level);
void api_error(enum api_message_level level, int err, const char *fmt, ...);
double time_now(void);

#define NRM  "\x1B[0m"
#define RED  "\x1B[31m"
#define GRN  "\x1B[32m"
#define YEL  "\x1B[33m"
#define BLU  "\x1B[34m"
#define MAG  "\x1B[35m"
#define CYN  "\x1B[36m"
#define WHT  "\x1B[37m"
#define RESET "\033[0m"

#define CT_ERROR(_rc, _format, ...)				       \
	api_error(API_MSG_ERROR, _rc,				       \
		  RED "[E] " RESET "%f [%ld] %s:%d "_format,	       \
		  time_now(), syscall(SYS_gettid), __FILE__, __LINE__, \
		  ## __VA_ARGS__)

#define CT_WARN(_format, ...)					       \
	api_error(API_MSG_WARN | API_MSG_NO_ERRNO, 0,		       \
		  RED "[W] " RESET "%f [%ld] %s:%d "_format,	       \
		  time_now(), syscall(SYS_gettid), __FILE__, __LINE__, \
		  ## __VA_ARGS__)

#define CT_MESSAGE(_format, ...)					\
	api_error(API_MSG_NORMAL | API_MSG_NO_ERRNO, 0,			\
		  MAG "[M] " RESET "%f [%ld] %s:%d "_format,		\
		  time_now(), syscall(SYS_gettid), __FILE__, __LINE__,	\
		  ## __VA_ARGS__)

#define CT_INFO(_format, ...)					       \
	api_error(API_MSG_INFO | API_MSG_NO_ERRNO, 0,		       \
		  YEL "[I] " RESET "%f [%ld] %s:%d "_format,	       \
		  time_now(), syscall(SYS_gettid), __FILE__, __LINE__, \
		  ## __VA_ARGS__)

#define CT_DEBUG(_format, ...)					       \
	api_error(API_MSG_DEBUG | API_MSG_NO_ERRNO, 0,		       \
		  BLU "[D] " RESET "%f [%ld] %s:%d "_format,	       \
		  time_now(), syscall(SYS_gettid), __FILE__, __LINE__, \
		  ## __VA_ARGS__)

#endif /* LOG_H */
