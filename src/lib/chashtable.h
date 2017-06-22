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

#ifndef CHASHTABLE_H
#define CHASHTABLE_H

#include <stdlib.h>
#include <stdint.h>
#include "list.h"

typedef struct {
	uint32_t buckets;
	uint32_t (*h)(const void *key);
	int (*match)(const void *key1, const void *key2);
	void (*destroy)(void *data);
	size_t size;
	list_t *table;
} chashtable_t;

#define chashtable_size(chashtable) ((chashtable)->size)

int chashtable_init(chashtable_t *chashtable, uint32_t buckets,
		    uint32_t (*h) (const void *key),
		    int (*match) (const void *key1, const void *key2),
		    void (*destroy) (void *data));

void chashtable_destroy(chashtable_t *chashtable);
int chashtable_insert(chashtable_t *chashtable, const void *data);
int chashtable_remove(chashtable_t *chashtable, const void *lookup_data,
		      void **data);
int chashtable_lookup(const chashtable_t *chashtable, const void *lookup_data,
		      void **data);
void for_each_key(const chashtable_t *chashtable, void (*callback)(void *data));

/* Some known string hash functions */
uint32_t hash_sdbm_str(const void *key);
uint32_t hash_dek_str(const void *key);
uint32_t hash_djb_str(const void *key);

#endif
