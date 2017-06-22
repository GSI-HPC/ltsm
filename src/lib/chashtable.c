/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Copyright (c) 2017, GSI Helmholtz Centre for Heavy Ion Research
 */

/*
 * This code is based on the book: Mastering Algorithms with C,
 *				   Kyle London, First Edition 1999.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "list.h"
#include "chashtable.h"

/* Some known string hash functions */
uint32_t hash_sdbm_str(const void *key)
{
	const char *str = key;
	uint32_t hash = 0;

	while (*str++)
		hash = *str + (hash << 6) + (hash << 16) - hash;

	return hash;
}

uint32_t hash_dek_str(const void *key)
{
	const char *str = key;
	uint32_t hash = strlen(str);

	while (*str++)
		hash = ((hash << 5) ^ (hash >> 27)) ^ *str;

	return hash;
}

uint32_t hash_djb_str(const void *key)
{
	const char *str = key;
        uint32_t hash = 5381;

        while (*str++)
		hash = ((hash << 5) + hash) + *str;

        return hash;
}

int chashtable_init(chashtable_t *chashtable, uint32_t buckets,
		    uint32_t (*h) (const void *key),
		    int (*match) (const void *key1, const void *key2),
		    void (*destroy) (void *data))
{
	if (chashtable->table)
		return RC_ERROR;

	chashtable->table = malloc(buckets * sizeof(list_t));
	if (chashtable->table == NULL)
		return RC_ERROR;

	chashtable->buckets = buckets;

	for (uint32_t b = 0; b < chashtable->buckets; b++)
		list_init(&chashtable->table[b], destroy);

	chashtable->h = h;
	chashtable->match = match;
	chashtable->destroy = destroy;
	chashtable->size = 0;

	return RC_SUCCESS;
}

void chashtable_destroy(chashtable_t *chashtable)
{
	for (uint32_t b = 0; b < chashtable->buckets; b++)
		list_destroy(&chashtable->table[b]);

	free(chashtable->table);
	memset(chashtable, 0, sizeof(chashtable_t));
}

int chashtable_insert(chashtable_t *chashtable, const void *data)
{
	int rc;
	void *temp = NULL;

	/* If data is already in hashtable, do nothing. */
	rc = chashtable_lookup(chashtable, data, &temp);
	if (rc == RC_DATA_FOUND)
		return RC_DATA_ALREADY_INSERTED;

	/* Hash the key. */
	const uint32_t bucket = chashtable->h(data) %
		chashtable->buckets;

	/* Insert data into the corresponding bucket. */
	rc = list_ins_next(&chashtable->table[bucket], NULL, data);
	if (rc == RC_SUCCESS)
		chashtable->size++;

	return rc;
}

int chashtable_remove(chashtable_t *chashtable, const void *lookup_data,
		      void **data)
{
	int rc;
	list_node_t *prev = NULL;
	const uint32_t bucket = chashtable->h(lookup_data) %
		chashtable->buckets;

	/* Iterate over linked-list in bucket. */
	for (list_node_t *node = list_head(&chashtable->table[bucket]);
	     node != NULL;
	     node = list_next(node)) {
		rc = (chashtable->match(lookup_data, list_data(node)));
		if (rc == RC_SUCCESS) {
			/* Remove the data in linked-list in bucket. */
			rc = list_rem_next(&chashtable->table[bucket],
					   prev, data);
			if (rc == RC_SUCCESS) {
				chashtable->size--;
				return rc;
			} else
				return RC_ERROR;
		}
		prev = node;
	}
	return RC_DATA_NOT_FOUND;
}

int chashtable_lookup(const chashtable_t *chashtable, const void *lookup_data,
		      void **data)
{
	int rc;
	const uint32_t bucket = chashtable->h(lookup_data) %
		chashtable->buckets;

	for (list_node_t *node = list_head(&chashtable->table[bucket]);
			node != NULL;
			node = list_next(node)) {

		rc = chashtable->match(lookup_data, list_data(node));
		if (rc == RC_SUCCESS) {
			*data = list_data(node);
			return RC_DATA_FOUND;
		}
	}
	return RC_DATA_NOT_FOUND;
}

void for_each_key(const chashtable_t *chashtable, void (*callback)(void *data))
{
	for (uint32_t b = 0; b < chashtable->buckets; b++)
		for (list_node_t *node = list_head(&chashtable->table[b]);
		     node != NULL;
		     node = list_next(node))
			callback(list_data(node));
}
