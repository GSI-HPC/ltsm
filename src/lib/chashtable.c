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
 * Copyright (c) 2017, Thomas Stibor <t.stibor@gsi.de>
 */

/*
 * This code is based on the book: Mastering Algorithms with C,
 *				   Kyle London, First Edition 1999.
 */

#include <stdlib.h>
#include <string.h>
#include "list.h"
#include "chashtable.h"

/* Some known string hash functions */
unsigned int hash_sdbm_str(const void *key)
{
	const char *str = key;
	unsigned int hash = 0;

	while (*str++)
		hash = *str + (hash << 6) + (hash << 16) - hash;

	return (hash & 0x7FFFFFFF);
}

unsigned int hash_dek_str(const void *key)
{
	const char *str = key;
	unsigned int hash = strlen(str);

	while (*str++)
		hash = ((hash << 5) ^ (hash >> 27)) ^ *str;

	return (hash & 0x7FFFFFFF);
}

unsigned int hash_djb_str(const void *key)
{
	const char *str = key;
        unsigned int hash = 5381;

        while (*str++)
		hash = ((hash << 5) + hash) + *str;

        return (hash & 0x7FFFFFFF);
}


int chashtable_init(chashtable_t *chashtable, unsigned int buckets,
		    unsigned int (*h) (const void *key),
		    int (*match) (const void *key1, const void *key2),
		    void (*destroy) (void *data))
{
	unsigned int i;

	chashtable->table = malloc(buckets * sizeof(list_t));
	if (chashtable->table == NULL)
		return RC_ERROR;

	chashtable->buckets = buckets;

	for (i = 0; i < chashtable->buckets; i++)
		list_init(&chashtable->table[i], destroy);

	chashtable->h = h;
	chashtable->match = match;
	chashtable->destroy = destroy;
	chashtable->size = 0;

	return RC_SUCCESS;
}

void chashtable_destroy(chashtable_t *chashtable)
{
	unsigned int i;

	for (i = 0; i < chashtable->buckets; i++)
		list_destroy(&chashtable->table[i]);

	free(chashtable->table);
	memset(chashtable, 0, sizeof(chashtable_t));
}

int chashtable_insert(chashtable_t *chashtable, const void *data)
{
	int rc;
	void *temp = NULL;
	unsigned int bucket;

	/* If data is already in hashtable, do nothing. */
	rc = chashtable_lookup(chashtable, data, &temp);
	if (rc == RC_DATA_FOUND)
		return RC_DATA_ALREADY_INSERTED;

	/* Hash the key. */
	bucket = chashtable->h(data) % chashtable->buckets;

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
	unsigned int bucket;
	list_node_t *prev = NULL;

	bucket = chashtable->h(lookup_data) % chashtable->buckets;

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
	unsigned int bucket;

	bucket = chashtable->h(lookup_data) % chashtable->buckets;

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
	for (unsigned int b = 0; b < chashtable->buckets; b++)
		for (list_node_t *node = list_head(&chashtable->table[b]);
		     node != NULL;
		     node = list_next(node))
			callback(list_data(node));
}
