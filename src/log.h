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

#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <string.h>
#include <errno.h>

#define NRM  "\x1B[0m"
#define RED  "\x1B[31m"
#define GRN  "\x1B[32m"
#define YEL  "\x1B[33m"
#define BLU  "\x1B[34m"
#define MAG  "\x1B[35m"
#define CYN  "\x1B[36m"
#define WHT  "\x1B[37m"
#define RESET "\033[0m"

#define ERR_MSG(msg, ...) fprintf(stderr, RED "[ERROR] " RESET "(%s:%d) %s(...), call %s(...) \"%s\"\n", \
				  __FILE__, __LINE__, __func__, msg, strerror(errno));

#define WARN_MSG(msg, ...) fprintf(stderr, RED "[WARNING]\n" RESET msg, ##__VA_ARGS__);

#ifdef DEBUG
#define DEBUG_MSG(msg, ...) fprintf(stdout, YEL "[DEBUG]" RESET " (%s:%d) %s(...) " msg, __FILE__, __LINE__, __func__, ##__VA_ARGS__);
#else
#define DEBUG_MSG(msg, ...)
#endif	/* DEBUG */

#ifdef VERBOSE
#define VERBOSE_MSG(msg, ...) fprintf(stdout, BLU "[VERBOSE]\n" RESET msg, ##__VA_ARGS__);
#else
#define VERBOSE_MSG(msg, ...)
#endif	/* VERBOSE */

#endif /* LOG_H */
