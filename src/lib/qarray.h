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
 * Copyright (c) 2016, Thomas Stibor <t.stibor@gsi.de>
 */

#ifndef QARRAY_H
#define QARRAY_H

#include "tsmapi.h"

#define INITIAL_CAPACITY 32

dsInt16_t init_qarray(struct qarray_t **qarray);
dsInt16_t insert_query(qryRespArchiveData *query_data, struct qarray_t **qarray,
		       const dsmBool_t overwrite_oldest);
dsInt16_t get_query(qryRespArchiveData *query_data,
		    const struct qarray_t *qarray, const unsigned long n);
unsigned long qarray_size(const struct qarray_t *qarray);
void sort_qarray(struct qarray_t **qarray);
int cmp_restore_order(const void *a, const void *b);
void destroy_qarray(struct qarray_t **qarray);

#endif /* QARRAY_H */
