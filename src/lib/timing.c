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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#if HAVE_CONFIG_H
 #include <config.h>
#endif

#if WITH_TIMINGS
 #include "time.h"
 #include "timing.h"
 #include "log.h"


uint64_t timing_runtime(struct timer_t *t){
	if(!t->enabled) return 0;
 	uint64_t t_start = t->start.tv_sec * 1000000 + t->start.tv_nsec / 1000;
 	uint64_t t_end = t->end.tv_sec * 1000000 + t->end.tv_nsec / 1000;
 	if(t_end <= t_start){
 		CT_DEBUG("Timer '%s' not finished yet. Get runtime until now.",
 			 t->name);
 		struct timespec curr;
		clock_gettime (CLOCK_MONOTONIC, &curr);
		t_end = curr.tv_sec * 1000000 + curr.tv_nsec / 1000;
 	}
 	return (t_end-t_start);
}

void timing_print_custom(struct timer_t *t,
	double factor_size, double factor_time,
	const char *string_size, int size_string_pad,
	const char *string_time, int time_string_pad,
	int size_min_pad, int time_min_pad, int vpt_min_pad, int decplaces)
{
	if(!t->enabled) return;
 	uint64_t nsec_diff = timing_runtime(t);

 	double custom_data_processed = t->data_processed / factor_size;
 	double custom_diff = nsec_diff / factor_time;
 	double custom_vpt = custom_data_processed / custom_diff;

	CT_MESSAGE("Timer '%s' processed %*.*f %*s in %*.*f %*s "
		   "(%*s per %*s: %*.*f)",
		   t->name, size_min_pad, decplaces, custom_data_processed,
		   size_string_pad, string_size, time_min_pad, decplaces,
		   custom_diff, time_string_pad, string_time, size_string_pad,
		   string_size, time_string_pad, string_time, vpt_min_pad,
		   decplaces, custom_vpt);

}

void timing_print_custom_simple(struct timer_t *t,
	double factor_size,
	double factor_time,
	const char *string_size,
	const char *string_time,
	int decplaces)
{
	if(!t->enabled) return;
	timing_print_custom(t, factor_size, factor_time, string_size, 0,
		            string_time, 0, 0, 0, 0, decplaces);
}

#endif