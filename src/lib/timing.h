/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Copyright (c) 2016, 2017 Thomas Stibor <t.stibor@gsi.de>
 */

/* Important note: The API can only retrieve objects that were
   archived using TSM API calls, that is, data archived with dsmc cannot
   be retrieved with these TSM API calls or console client ltsmc.
   Moreover the API doesn't support subdir opperations, that is, hl/ll
   queries must be constructed (in a clever way) with wildcard (*) and
   question mark (?) to match sub directories and files. For more detail c.f.
   PDF Dokument: Using the Application Programming Interface
   (http://www.ibm.com/support/knowledgecenter/SSGSG7_7.1.3/api/b_api_using.pdf)
*/

#ifndef TIMING_H
#define TIMING_H

#if HAVE_CONFIG_H
 #include <config.h>
#endif

#if WITH_TIMINGS

#include <stdint.h>
#include "time.h"
#include "log.h"

struct timer_t {
	uint64_t data_processed;
	int enabled;
	const char* name;
	struct timespec start, end;
};

uint64_t timing_runtime(struct timer_t *t);

void timing_print_custom(struct timer_t *t,
	double factor_size, double factor_time,
	const char *string_size, int size_string_pad,
	const char *string_time, int time_string_pad,
	int size_min_pad, int time_min_pad, int vpt_min_pad, int decplaces);

void timing_print_custom_simple(struct timer_t *t,
	double factor_size,
	double factor_time,
	const char *string_size,
	const char *string_time,
	int decplaces);



 #define TIMER_DECLARE(s,e) struct timer_t timer_##s##_t = {.name = #s, .enabled = e};
 #define TIMER_START(s) timer_##s##_t.data_processed = 0;\
		       clock_gettime(CLOCK_MONOTONIC, &timer_##s##_t.end);\
		       clock_gettime(CLOCK_MONOTONIC, &timer_##s##_t.start);\
		       timer_##s##_t.enabled? CT_MESSAGE("started timer '%s'",\
		       timer_##s##_t.name) : "";
 #define TIMER_ADD_DATA(s,d) timer_##s##_t.data_processed += d;
 #define TIMER_STOP(s) clock_gettime(CLOCK_MONOTONIC, &timer_##s##_t.end);\
		       timer_##s##_t.enabled? CT_MESSAGE("stopped timer '%s' "\
		       "after %lu ns",\
		       timer_##s##_t.name,TIMER_RUNTIME(s)) : "" ;
 #define TIMER_GET(s) &timer_##s##_t  /*pointer to timer*/
 #define TIMER_RUNTIME(s) timing_runtime(&timer_##s##_t)   /*returns uint64_t*/
 #define PRINT_TIMING_bps(s) timing_print_custom_simple(&timer_##s##_t,\
			     1.0/8,1000.0*1000,"bit","s",3);
 #define PRINT_TIMING_Bps(s) timing_print_custom_simple(&timer_##s##_t,\
			     1.0,1000.0*1000,"byte","s",3);
 #define PRINT_TIMING_KBps(s) timing_print_custom_simple(&timer_##s##_t,\
			     1024.0,1000.0*1000,"KB","s",3);
 #define PRINT_TIMING_KBps(s) timing_print_custom_simple(&timer_##s##_t,\
			     1024.0,1000.0*1000,"KB","s",3);
 #define PRINT_TIMING_MBps(s) timing_print_custom_simple(&timer_##s##_t,\
			     1024.0*1024,1000.0*1000,"MB","s",3);
 #define PRINT_TIMING_GBps(s) timing_print_custom_simple(&timer_##s##_t,\
			     1024.0*1024*1024,1000.0*1000,"GB","s",3);
 #define PRINT_TIMING_TBps(s) timing_print_custom_simple(&timer_##s##_t,\
			     1024.0*1024*1024*1024,1000.0*1000,"TB","s",3);
 #define PRINT_TIMING_CUSTOM_SIMPLE(s,...) timing_print_custom_simple\
					  (&timer_##s##_t, __VA_ARGS__);
 #define PRINT_TIMING_CUSTOM(s,...) timing_print_custom(&timer_##s##_t,\
			           __VA_ARGS__);
#else /* WITH_TIMINGS == 1*/
 #define TIMER_DECLARE(s,e)
 #define TIMER_START(s)
 #define TIMER_STOP(s)
 #define TIMER_ADD_DATA(s,d)
 #define TIMER_GET(s) NULL
 #define TIMER_RUNTIME(s) 0
 #define PRINT_TIMING_bps(s)
 #define PRINT_TIMING_Bps(s)
 #define PRINT_TIMING_KBps(s)
 #define PRINT_TIMING_KBps(s)
 #define PRINT_TIMING_MBps(s)
 #define PRINT_TIMING_GBps(s)
 #define PRINT_TIMING_TBps(s)
 #define PRINT_TIMING_CUSTOM_SIMPLE(s,...)
 #define PRINT_TIMING_CUSTOM(s,...)
#endif /* WITH_TIMINGS == 0*/

#endif /* TIMING_H */
