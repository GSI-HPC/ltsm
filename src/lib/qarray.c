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

#include <stdlib.h>
#include <search.h>
#include "log.h"
#include "qarray.h"

#define QARRAY_RC_UPDATED 20000
#define DSM_DATE_TO_SEC(date) (date.second + date.minute * 60 +		\
			       date.hour * 3600 + date.day * 86400 +	\
			       date.month * 2678400 + date.year * 977616000)

static query_arr_t *qarray = NULL;
static ENTRY entry_ht;
static ENTRY *result_ht = NULL;

dsInt16_t init_qarray()
{
	int rc;

	if (qarray)
		return DSM_RC_UNSUCCESSFUL;

	rc = hcreate(INITIAL_CAPACITY * INITIAL_CAPACITY);
	if (rc == 0) {
		CT_ERROR(errno, "hcreate");
		return DSM_RC_UNSUCCESSFUL;
	}

	qarray = malloc(sizeof(query_arr_t));
	if (!qarray) {
		CT_ERROR(errno, "malloc");
		return DSM_RC_UNSUCCESSFUL;
	}

	qarray->data = malloc(sizeof(qryRespArchiveData) * INITIAL_CAPACITY);
	if (!qarray->data) {
		free(qarray);
		CT_ERROR(errno, "malloc");
		return DSM_RC_UNSUCCESSFUL;
	}
	qarray->capacity = INITIAL_CAPACITY;
	qarray->N = 0;

	return DSM_RC_SUCCESSFUL;
}

static dsInt16_t replace_oldest_obj(const qryRespArchiveData *query_data)
{
	const size_t key_len = strlen(query_data->objName.fs) +
		strlen(query_data->objName.hl) +
		strlen(query_data->objName.ll) + 1;
	char key[key_len];
	bzero(key, key_len);

	snprintf(key, key_len, "%s%s%s", query_data->objName.fs,
		 query_data->objName.hl,
		 query_data->objName.ll);

	entry_ht.key = key;
	result_ht = hsearch(entry_ht, FIND);
	/* Key: fs/hl/ll is already in hashtable, thus get position in qarray
	   and fetch the corresponding qryRespArchiveData object. */
	if (result_ht) {
		const unsigned long long n = (unsigned long)result_ht->data;
		const qryRespArchiveData qdata_in_ar = qarray->data[n];
		/* If insertion date of qryRespArchiveData object in qarray is
		   older than the one we want to add, then overwrite the
		   qryRespArchiveData object at position n in qarray with
		   the newer one. */
		if (DSM_DATE_TO_SEC(qdata_in_ar.insDate) <
		    DSM_DATE_TO_SEC(query_data->insDate)) {
			CT_TRACE("replacing older date qryRespArchiveData: "
				 "%i/%02i/%02i %02i:%02i:%02i with newer "
				 "date: %i/%02i/%02i %02i:%02i:%02i",
				 qdata_in_ar.insDate.year,
				 qdata_in_ar.insDate.month,
				 qdata_in_ar.insDate.day,
				 qdata_in_ar.insDate.hour,
				 qdata_in_ar.insDate.minute,
				 qdata_in_ar.insDate.second,
				 query_data->insDate.year,
				 query_data->insDate.month,
				 query_data->insDate.day,
				 query_data->insDate.hour,
				 query_data->insDate.minute,
				 query_data->insDate.second);

			memcpy(&(qarray->data[n]), query_data,
			       sizeof(qryRespArchiveData));

			return QARRAY_RC_UPDATED;
		}
	} else {
		result_ht = hsearch(entry_ht, ENTER);
		if (result_ht == NULL) {
			CT_ERROR(errno, "hsearch ENTER '%s' failed", entry_ht.key);
			return DSM_RC_UNSUCCESSFUL;
		}
	}
	return DSM_RC_SUCCESSFUL;
}

dsInt16_t add_query(const qryRespArchiveData *query_data, const dsmBool_t use_latest)
{
	dsInt16_t rc;

	if (!qarray || !qarray->data)
		return DSM_RC_UNSUCCESSFUL;

	if (use_latest) {
		rc = replace_oldest_obj(query_data);
		if (rc == QARRAY_RC_UPDATED)
			return DSM_RC_SUCCESSFUL;
		else if (rc == DSM_RC_UNSUCCESSFUL)
			return rc;
	}

	/* Increase length (capacity) by factor of 2 when qarray is full. */
	if (qarray->N >= qarray->capacity) {
		qarray->capacity *= 2;
		qarray->data = realloc(qarray->data,
				       sizeof(qryRespArchiveData) *
				       qarray->capacity);
		if (qarray->data == NULL) {
			CT_ERROR(errno, "realloc");
			return DSM_RC_UNSUCCESSFUL;
		}
	}
	memcpy(&(qarray->data[qarray->N++]), query_data,
	       sizeof(qryRespArchiveData));

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
	hdestroy();
	result_ht = NULL;

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
