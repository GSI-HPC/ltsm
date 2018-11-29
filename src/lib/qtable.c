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
 * Copyright (c) 2017 GSI Helmholtz Centre for Heavy Ion Research
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdlib.h>
#include <stdint.h>
#include "qtable.h"

struct object_t {
	char key[DSM_MAX_FSNAME_LENGTH +
		 DSM_MAX_HL_LENGTH +
		 DSM_MAX_LL_LENGTH + 1];
	qryRespArchiveData qra_data;
};

static uint64_t date_in_sec(const dsmDate *date)
{
	return (uint64_t)date->second +
		(uint64_t)date->minute * 60 +
		(uint64_t)date->hour * 3600 +
		(uint64_t)date->day * 86400 +
		(uint64_t)date->month * 2678400 +
		(uint64_t)date->year * 977616000;
}

static void setup_object(struct object_t *object,
			 const qryRespArchiveData *qra_data)
{
	const size_t klen = strlen(qra_data->objName.fs) +
		strlen(qra_data->objName.hl) +
		strlen(qra_data->objName.ll) + 1;
	char key[klen];
	memset(key, 0, klen);
	memset(&object->qra_data, 0, sizeof(qryRespArchiveData));

	snprintf(object->key, klen, "%s%s%s",
		 qra_data->objName.fs,
		 qra_data->objName.hl,
		 qra_data->objName.ll);

	memcpy(&object->qra_data, qra_data, sizeof(qryRespArchiveData));
}

static int match(const void *object1, const void *object2)
{
	const struct object_t *obj1 = object1;
	const struct object_t *obj2 = object2;

	return strcmp(obj1->key, obj2->key);
}

static int remove_older_obj(struct qtable_t *qtable,
			    const struct object_t *newobj)
{
	int rc;
	struct object_t *oldobj = NULL;
	rc = chashtable_lookup(qtable->chashtable, newobj,
			       (void **)&oldobj);
	/* If newobj has same key (fs/hl/ll) and same date or
	   later date than the oldobj (which is already in the hashtable),
	   then we remove oldobj. */
	if (rc == RC_DATA_FOUND && (date_in_sec(&newobj->qra_data.insDate) >=
				    date_in_sec(&oldobj->qra_data.insDate))) {
		rc = chashtable_remove(qtable->chashtable,
				       newobj, (void **)&oldobj);
		if (rc == RC_SUCCESS) {
			free(oldobj);
			return DSM_RC_SUCCESSFUL;
		} else
			return DSM_RC_UNSUCCESSFUL;
	}
	return DSM_RC_SUCCESSFUL;
}

dsInt16_t init_qtable(struct qtable_t *qtable)
{
	dsInt16_t rc;

	if (qtable->chashtable)
		return DSM_RC_UNSUCCESSFUL;

	qtable->chashtable = calloc(1, sizeof(chashtable_t));
	if (qtable->chashtable == NULL)
		return DSM_RC_UNSUCCESSFUL;

	if (qtable->nbuckets == 0)
		qtable->nbuckets = DEFAULT_NUM_BUCKETS;
	rc = chashtable_init(qtable->chashtable, qtable->nbuckets, hash_djb_str,
			     match, free);
	if (rc == RC_SUCCESS)
		return DSM_RC_SUCCESSFUL;

	if (qtable->chashtable) {
		free(qtable->chashtable);
		qtable->chashtable = NULL;
	}

	return DSM_RC_SUCCESSFUL;
}

dsInt16_t insert_qtable(struct qtable_t *qtable,
			const qryRespArchiveData *qra_data)
{
	dsInt16_t rc;
	struct object_t *insobj;
	insobj = calloc(1, sizeof(struct object_t));
	if (insobj == NULL) {
		CT_ERROR(errno, "calloc failed");
		return DSM_RC_UNSUCCESSFUL;
	}
	setup_object(insobj, qra_data);

	if (!qtable->multiple) {
		rc = remove_older_obj(qtable, insobj);
		if (rc == DSM_RC_UNSUCCESSFUL) {
			free(insobj);
			return rc;
		}
	}

	const uint32_t bucket = qtable->chashtable->h(insobj) %
		qtable->chashtable->buckets;
	rc = list_ins_next(&qtable->chashtable->table[bucket], NULL, insobj);
	if (rc == RC_SUCCESS) {
		qtable->chashtable->size++;
		rc = DSM_RC_SUCCESSFUL;
	} else {
		free(insobj);
		rc = DSM_RC_UNSUCCESSFUL;
	}

	return rc;
}

void destroy_qtable(struct qtable_t *qtable)
{
	if (qtable->chashtable) {
		chashtable_destroy(qtable->chashtable);
		free(qtable->chashtable);
		qtable->chashtable = NULL;
	}

	if (qtable->qarray.data) {
		free(qtable->qarray.data);
		qtable->qarray.size = 0;
		qtable->qarray.data = NULL;
	}
}

int cmp_restore_order(const void *a, const void *b)
{
	const qryRespArchiveData *query_data_a = (qryRespArchiveData *)a;
	const qryRespArchiveData *query_data_b = (qryRespArchiveData *)b;

	if (query_data_a->restoreOrderExt.top >
	    query_data_b->restoreOrderExt.top)
		return(DS_GREATERTHAN);
	else if (query_data_a->restoreOrderExt.top <
		 query_data_b->restoreOrderExt.top)
		return(DS_LESSTHAN);
	else if (query_data_a->restoreOrderExt.hi_hi >
		 query_data_b->restoreOrderExt.hi_hi)
		return(DS_GREATERTHAN);
	else if (query_data_a->restoreOrderExt.hi_hi <
		 query_data_b->restoreOrderExt.hi_hi)
		return(DS_LESSTHAN);
	else if (query_data_a->restoreOrderExt.hi_lo >
		 query_data_b->restoreOrderExt.hi_lo)
		return(DS_GREATERTHAN);
	else if (query_data_a->restoreOrderExt.hi_lo <
		 query_data_b->restoreOrderExt.hi_lo)
		return(DS_LESSTHAN);
	else if (query_data_a->restoreOrderExt.lo_hi >
		 query_data_b->restoreOrderExt.lo_hi)
		return(DS_GREATERTHAN);
	else if (query_data_a->restoreOrderExt.lo_hi <
		 query_data_b->restoreOrderExt.lo_hi)
		return(DS_LESSTHAN);
	else if (query_data_a->restoreOrderExt.lo_lo >
		 query_data_b->restoreOrderExt.lo_lo)
		return(DS_GREATERTHAN);
	else if (query_data_a->restoreOrderExt.lo_lo <
		 query_data_b->restoreOrderExt.lo_lo)
		return(DS_LESSTHAN);
	else
		return(DS_EQUAL);
}

int cmp_date_ascending(const void *a, const void *b)
{
	const qryRespArchiveData *query_data_a = (qryRespArchiveData *)a;
	const qryRespArchiveData *query_data_b = (qryRespArchiveData *)b;

	if (date_in_sec(&query_data_a->insDate) > date_in_sec(&query_data_b->insDate))
		return DS_GREATERTHAN;
	else if (date_in_sec(&query_data_a->insDate) < date_in_sec(&query_data_b->insDate))
		return DS_LESSTHAN;
	else
		return DS_EQUAL;
}

int cmp_date_descending(const void *a, const void *b)
{
	const qryRespArchiveData *query_data_a = (qryRespArchiveData *)a;
	const qryRespArchiveData *query_data_b = (qryRespArchiveData *)b;

	if (date_in_sec(&query_data_b->insDate) > date_in_sec(&query_data_a->insDate))
		return DS_GREATERTHAN;
	else if (date_in_sec(&query_data_b->insDate) < date_in_sec(&query_data_a->insDate))
		return DS_LESSTHAN;
	else
		return DS_EQUAL;
}

dsInt16_t create_array(struct qtable_t *qtable, enum sort_by_t sort_by)
{
	if (qtable->qarray.size > 0 || qtable->qarray.data)
		return DSM_RC_UNSUCCESSFUL;

	qtable->qarray.size = 0;
	qtable->qarray.data = calloc(chashtable_size(qtable->chashtable),
					  sizeof(qryRespArchiveData));
	if (qtable->qarray.data == NULL) {
		return DSM_RC_UNSUCCESSFUL;
	}
	uint32_t c = 0;
	for (uint32_t b = 0; b < qtable->chashtable->buckets; b++) {
		for (list_node_t *node = list_head(&qtable->
						   chashtable->table[b]);
		     node != NULL;
		     node = list_next(node)) {
			struct object_t *object = list_data(node);
			memcpy(&qtable->qarray.data[c++], &object->qra_data,
			       sizeof(qryRespArchiveData));
		}
	}
	qtable->qarray.size = c;

	if (c) {
		switch (sort_by) {
		case SORT_RESTORE_ORDER: {
			qsort(qtable->qarray.data, c, sizeof(qryRespArchiveData),
			      cmp_restore_order);
			break;
		}
		case SORT_DATE_ASCENDING: {
			qsort(qtable->qarray.data, c, sizeof(qryRespArchiveData),
			      cmp_date_ascending);
			break;
		}
		case SORT_DATE_DESCENDING: {
			qsort(qtable->qarray.data, c, sizeof(qryRespArchiveData),
			      cmp_date_descending);
			break;
		}
		case SORT_NONE:
		default:
			break;
		}
	}

	return DSM_RC_SUCCESSFUL;
}

dsInt16_t get_qra(const struct qtable_t *qtable,
		  qryRespArchiveData *qra_data, const uint32_t n)
{
	if (qtable->qarray.size == 0 || qtable->qarray.data == NULL)
		return DSM_RC_UNSUCCESSFUL;

	*qra_data = qtable->qarray.data[n];
	return DSM_RC_SUCCESSFUL;
}
