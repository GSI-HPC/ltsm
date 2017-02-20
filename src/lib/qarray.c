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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <search.h>
#include "log.h"
#include "qarray.h"

#define RC_QARRAY_UPDATED 20000

static unsigned long long date_in_sec(const dsmDate *date)
{
	return (unsigned long long)date->second +
		(unsigned long long)date->minute * 60 +
		(unsigned long long)date->hour * 3600 +
		(unsigned long long)date->day * 86400 +
		(unsigned long long)date->month * 2678400 +
		(unsigned long long)date->year * 977616000;
}

dsInt16_t init_qarray(qarray_t **qarray)
{
	int rc;

	if (*qarray)
		return DSM_RC_UNSUCCESSFUL;

	*qarray = calloc(1, sizeof(qarray_t));
	if (!qarray) {
		CT_ERROR(errno, "malloc");
		return DSM_RC_UNSUCCESSFUL;
	}

	(*qarray)->data = calloc(INITIAL_CAPACITY, sizeof(qryRespArchiveData));
	if (!(*qarray)->data) {
		CT_ERROR(errno, "malloc");
		rc = DSM_RC_UNSUCCESSFUL;
		goto cleanup;
	}
	(*qarray)->capacity = INITIAL_CAPACITY;
	(*qarray)->N = 0;

	/* The structure pointed to by htab must be zeroed before the first
	   call to hcreate_r(). */
	(*qarray)->htab = calloc(1, sizeof(struct hsearch_data));
	if ((*qarray)->htab == NULL) {
		CT_ERROR(errno, "calloc");
		rc = DSM_RC_UNSUCCESSFUL;
		goto cleanup;
	}
	rc = hcreate_r(INITIAL_CAPACITY * INITIAL_CAPACITY, (*qarray)->htab);
	if (rc == 0) {
		CT_ERROR(errno, "hcreate");
		rc = DSM_RC_UNSUCCESSFUL;
		goto cleanup;
	}

	return DSM_RC_SUCCESSFUL;

cleanup:
	if ((*qarray)->htab) {
		hdestroy_r((*qarray)->htab);
		free((*qarray)->htab);
		(*qarray)->htab = NULL;
	}

	if ((*qarray)->data) {
		free((*qarray)->data);
		(*qarray)->data = NULL;
	}
	if (*qarray) {
		free(*qarray);
		*qarray = NULL;
	}

	return rc;
}

static dsInt16_t replace_oldest_obj(const qryRespArchiveData *query_data,
				    qarray_t **qarray, const dsmBool_t overwrite_oldest)
{
	int rc;
	ENTRY entry_ht;
	ENTRY *result_ht = NULL;
	const size_t key_len = strlen(query_data->objName.fs) +
		strlen(query_data->objName.hl) +
		strlen(query_data->objName.ll) + 1;
	char key[key_len];
	bzero(key, key_len);

	snprintf(key, key_len, "%s%s%s", query_data->objName.fs,
		 query_data->objName.hl,
		 query_data->objName.ll);

	entry_ht.key = key;
	entry_ht.data = (void *)(*qarray)->N;
	rc = hsearch_r(entry_ht, FIND, &result_ht, (*qarray)->htab);
	/* Key: fs/hl/ll is already in hashtable, thus get position in qarray
	   and fetch the corresponding qryRespArchiveData object. */
	if (rc) {
		const unsigned long n = (unsigned long)result_ht->data;
		/* If insertion date of qryRespArchiveData object in qarray is
		   older than the one we want to insert, then overwrite the
		   qryRespArchiveData object at position n in qarray with
		   the newer one. */
		if (date_in_sec(&(*qarray)->data[n].insDate) <
		    date_in_sec(&query_data->insDate)) {
			CT_TRACE("replacing older date qryRespArchiveData: "
				 "%i/%02i/%02i %02i:%02i:%02i with newer "
				 "date: %i/%02i/%02i %02i:%02i:%02i",
				 (*qarray)->data[n].insDate.year,
				 (*qarray)->data[n].insDate.month,
				 (*qarray)->data[n].insDate.day,
				 (*qarray)->data[n].insDate.hour,
				 (*qarray)->data[n].insDate.minute,
				 (*qarray)->data[n].insDate.second,
				 query_data->insDate.year,
				 query_data->insDate.month,
				 query_data->insDate.day,
				 query_data->insDate.hour,
				 query_data->insDate.minute,
				 query_data->insDate.second);

			if (overwrite_oldest) {
				memcpy(&((*qarray)->data[n]), query_data,
				       sizeof(qryRespArchiveData));
				return RC_QARRAY_UPDATED;
			}
		}
		return DSM_RC_SUCCESSFUL;
	} else {
		/* Insert key: fs/hl/ll and data: (*qarray)->N into hashtable. */
		rc = hsearch_r(entry_ht, ENTER, &result_ht, (*qarray)->htab);
		if (rc == 0) {
			CT_ERROR(errno, "hsearch_r ENTER '%s' failed", entry_ht.key);
			return DSM_RC_UNSUCCESSFUL;
		}
	}
	return DSM_RC_SUCCESSFUL;
}

dsInt16_t insert_query(qryRespArchiveData *query_data, qarray_t **qarray,
		       const dsmBool_t overwrite_oldest)
{
	dsInt16_t rc;

	if (!(*qarray) || !(*qarray)->data)
		return DSM_RC_UNSUCCESSFUL;

	rc = replace_oldest_obj(query_data, qarray, overwrite_oldest);
	if (rc == RC_QARRAY_UPDATED)
		return DSM_RC_SUCCESSFUL;
	else if (rc == DSM_RC_UNSUCCESSFUL)
		return rc;

	/* Increase length (capacity) by factor of 2 when qarray is full. */
	if ((*qarray)->N >= (*qarray)->capacity) {
		(*qarray)->capacity *= 2;
		(*qarray)->data = realloc((*qarray)->data,
					  sizeof(qryRespArchiveData) *
					  (*qarray)->capacity);
		if ((*qarray)->data == NULL) {
			CT_ERROR(errno, "realloc");
			return DSM_RC_UNSUCCESSFUL;
		}
	}
	memcpy(&((*qarray)->data[(*qarray)->N++]), query_data,
	       sizeof(qryRespArchiveData));

	return DSM_RC_SUCCESSFUL;
}

dsInt16_t get_query(qryRespArchiveData *query_data, const qarray_t *qarray,
		    const unsigned long n)
{
	if (!qarray || !qarray->data)
		return DSM_RC_UNSUCCESSFUL;

	if (n > qarray->N)
		return DSM_RC_UNSUCCESSFUL;

	*query_data = (qarray->data[n]);

	return DSM_RC_SUCCESSFUL;
}

unsigned long qarray_size(const qarray_t *qarray)
{
	if (!qarray || !qarray->data)
		return 0;

	return qarray->N;
}

void destroy_qarray(qarray_t **qarray)
{
	if ((*qarray) == NULL)
		return;

	if ((*qarray)->htab) {
		hdestroy_r((*qarray)->htab);
		free((*qarray)->htab);
		(*qarray)->htab = NULL;
	}

	if ((*qarray)->data) {
		free((*qarray)->data);
		(*qarray)->data = NULL;
	}
	if (*qarray) {
		free(*qarray);
		*qarray = NULL;
	}
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


/**
 * @brief Sort query objects on the restore order field.
 *
 * Sort the objects in ascending order (low to high). This sorting is very
 * important to the performance of the restore operation. Sorting the objects
 * on the restoreOrderExt fields ensures that the data is read from the server
 * in the most efficient order.
 *
 * @param[in] qarray Query array containing query objects.
 */
void sort_qarray(qarray_t **qarray)
{
	qsort((*qarray)->data, (*qarray)->N, sizeof(qryRespArchiveData), cmp_restore_order);
}
