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

#include <stdlib.h>
#include "qarray.h"

static query_arr_t *qarray = NULL;

dsInt16_t init_qarray()
{
    if (qarray)
	return DSM_RC_UNSUCCESSFUL;

    qarray = malloc(sizeof(query_arr_t));
    if (!qarray) {
	ERR_MSG("malloc");
	return DSM_RC_NO_MEMORY;
    }
    
    qarray->data = malloc(sizeof(qryRespArchiveData) * INITIAL_CAPACITY);
    if (!qarray->data) {
	free(qarray);
	ERR_MSG("malloc");
	return DSM_RC_NO_MEMORY;
    }
    qarray->capacity = INITIAL_CAPACITY;
    qarray->N = 0;

    return DSM_RC_SUCCESSFUL;
}

dsInt16_t add_query(const qryRespArchiveData *query_data)
{
    if (!qarray)
	return DSM_RC_UNSUCCESSFUL;
    if (!qarray->data)
	return DSM_RC_UNSUCCESSFUL;

    if (qarray->N >= qarray->capacity) {
	qarray->capacity *= 2;
	qarray->data = realloc(qarray->data, sizeof(qryRespArchiveData) * qarray->capacity);
    }

    memcpy(&(qarray->data[qarray->N++]), query_data, sizeof(qryRespArchiveData));

    return DSM_RC_SUCCESSFUL;
}

dsInt16_t get_query(qryRespArchiveData *query_data, const unsigned long n)
{
    if (!qarray || !qarray->data)
	return DSM_RC_UNSUCCESSFUL;

    if (n > qarray->N)
	return DSM_RC_UNSUCCESSFUL;

    *query_data = (qarray->data[n]);

    return DSM_RC_SUCCESSFUL;
}

unsigned long qarray_size()
{
    if (!qarray || !qarray->data)
	return 0;

    return qarray->N;
}

void destroy_qarray()
{
    if (!qarray)
	return;

    if (qarray->data) {
	free(qarray->data);
	qarray->data = NULL;
    }

    free(qarray);
    qarray = NULL;
}

int cmp_restore_order(const void *a, const void *b)
{
    const qryRespArchiveData *query_data_a = (qryRespArchiveData *)a;
    const qryRespArchiveData *query_data_b = (qryRespArchiveData *)b;

    if (query_data_a->restoreOrderExt.top > query_data_b->restoreOrderExt.top)
        return(DS_GREATERTHAN);
    else if (query_data_a->restoreOrderExt.top < query_data_b->restoreOrderExt.top)
        return(DS_LESSTHAN);
    else if (query_data_a->restoreOrderExt.hi_hi > query_data_b->restoreOrderExt.hi_hi)
        return(DS_GREATERTHAN);
    else if (query_data_a->restoreOrderExt.hi_hi < query_data_b->restoreOrderExt.hi_hi)
        return(DS_LESSTHAN);
    else if (query_data_a->restoreOrderExt.hi_lo > query_data_b->restoreOrderExt.hi_lo)
        return(DS_GREATERTHAN);
    else if (query_data_a->restoreOrderExt.hi_lo < query_data_b->restoreOrderExt.hi_lo)
        return(DS_LESSTHAN);
    else if (query_data_a->restoreOrderExt.lo_hi > query_data_b->restoreOrderExt.lo_hi)
        return(DS_GREATERTHAN);
    else if (query_data_a->restoreOrderExt.lo_hi < query_data_b->restoreOrderExt.lo_hi)
        return(DS_LESSTHAN);
    else if (query_data_a->restoreOrderExt.lo_lo > query_data_b->restoreOrderExt.lo_lo)
        return(DS_GREATERTHAN);
    else if (query_data_a->restoreOrderExt.lo_lo < query_data_b->restoreOrderExt.lo_lo)
        return(DS_LESSTHAN);
    else
        return(DS_EQUAL);
}

void sort_qarray()
{
    /* Sort objects to restore on key restoreOrderExt to ensure that tapes are
       mounted only once and are read from front to back. Sort them in ascending
       order (low to high). */
    qsort(qarray->data, qarray->N, sizeof(qryRespArchiveData), cmp_restore_order);
}
