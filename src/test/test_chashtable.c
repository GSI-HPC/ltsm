/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Copyright (c) 2017, GSI Helmholtz Centre for Heavy Ion Research
 */

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include "chashtable.h"
#include "CuTest.h"
#include "test_utils.h"
#include "log.h"

#define KEYLEN 256

typedef struct {
	char key[KEYLEN];
	uint32_t data;
} object_t ;

void load_factor(CuTest *tc, const chashtable_t *chashtable, const char *hashf)
{
	uint64_t total_size = 0;
	const float load_factor = chashtable_size(chashtable) /
		(float)chashtable->buckets;
	float sd = 0;
	float min = FLT_MAX;
	float max = -FLT_MAX;
	float temp;

	for (uint32_t b = 0; b < chashtable->buckets; b++) {
		total_size += list_size(&chashtable->table[b]);
#if 0
		CT_INFO("list size: %lu in bucket: %d",
			list_size(&chashtable->table[b]), b);
#endif
		sd += (list_size(&chashtable->table[b]) - load_factor) *
			(list_size(&chashtable->table[b]) - load_factor);

		temp = fabsf(list_size(&chashtable->table[b]) - load_factor);
		if (temp < min)
			min = temp;
		if (temp > max)
			max = temp;
	}
	CuAssertIntEquals(tc, chashtable_size(chashtable), total_size);
	printf("hash function: %s, optimal load factor: %.3f,"
	       " empirical standard deviation: %.3f, min: %.3f, max: %.3f\n",
	       hashf,
	       load_factor, sqrt(sd / (float)chashtable->buckets),
	       min, max);
}

object_t *init_object(const uint16_t i)
{
	object_t *object = malloc(sizeof(object_t));
	if (object == NULL)
		return NULL;
	memset(object->key, 0, KEYLEN);
	sprintf(object->key, "fs/hl/ll/:%d", i);
	object->data = i*i;

	return object;
}

object_t *init_rnd_object(const uint16_t i)
{
	object_t *object = malloc(sizeof(object_t));
	if (object == NULL)
		return NULL;
	memset(object->key, 0, KEYLEN);
	rnd_str(object->key, KEYLEN - 1);
	object->data = i*i;

	return object;
}


int match(const void *object1, const void *object2)
{
	const object_t *obj1 = object1;
	const object_t *obj2 = object2;

	return strcmp(obj1->key, obj2->key);
}

void test_chashtable_1(CuTest *tc)
{
	int rc;
	chashtable_t chashtable;

	for (uint16_t b = 1; b < 256; b++) {

		memset(&chashtable, 0, sizeof(chashtable));
		rc = chashtable_init(&chashtable, b, hash_sdbm_str, match, free);
		CuAssertIntEquals(tc, RC_SUCCESS, rc);
		CuAssertIntEquals(tc, 0, chashtable_size(&chashtable));

		for (uint16_t i = 1; i <= 1024; i++) {
			object_t *object = init_object(i);
			CuAssertPtrNotNull(tc, object);
			rc = chashtable_insert(&chashtable, object);
			CuAssertIntEquals(tc, RC_SUCCESS, rc);
		}
		for (uint16_t i = 1; i <= 2048; i++) {
			object_t *object = init_object(i);
			CuAssertPtrNotNull(tc, object);
			rc = chashtable_insert(&chashtable, object);
			if (i <= 1024) {
				free(object);
				CuAssertIntEquals(tc, RC_DATA_ALREADY_INSERTED, rc);
			} else
				CuAssertIntEquals(tc, RC_SUCCESS, rc);
		}
		for (uint16_t i = 1; i <= 4096; i++) {
			object_t *object_lookup = init_object(i);
			object_t *object = NULL;
			CuAssertPtrNotNull(tc, object_lookup);
			rc = chashtable_lookup(&chashtable, object_lookup,
					       (void **)&object);
			if (i <= 2048) {
				CuAssertIntEquals(tc, RC_DATA_FOUND, rc);
				CuAssertPtrNotNull(tc, object);
			}
			else {
				CuAssertIntEquals(tc, RC_DATA_NOT_FOUND, rc);
				CuAssertPtrEquals(tc, NULL, object);
			}
			free(object_lookup);
		}
		for (uint16_t i = 512; i <= 1536; i++) {
			object_t *object_lookup = init_object(i);
			CuAssertPtrNotNull(tc, object_lookup);
			object_t *object = NULL;
			rc = chashtable_remove(&chashtable, object_lookup,
					       (void **)&object);
			CuAssertIntEquals(tc, RC_SUCCESS, rc);
			CuAssertPtrNotNull(tc, object);
			free(object_lookup);
			free(object);
		}
		for (uint16_t i = 1; i <= 4096; i++) {
			object_t *object_lookup = init_object(i);
			object_t *object = NULL;
			CuAssertPtrNotNull(tc, object_lookup);
			rc = chashtable_lookup(&chashtable, object_lookup,
					       (void **)&object);
			if ((i >= 512 && i <= 1536) || i > 2048) {
				CuAssertIntEquals(tc, RC_DATA_NOT_FOUND, rc);
				CuAssertPtrEquals(tc, NULL, object);
			}
			else {
				CuAssertIntEquals(tc, RC_DATA_FOUND, rc);
				CuAssertPtrNotNull(tc, object);
			}
			free(object_lookup);
		}
		chashtable_destroy(&chashtable);
		CuAssertIntEquals(tc, 0, chashtable_size(&chashtable));
	}
}

void test_chashtable_2(CuTest *tc)
{
	int rc;
	chashtable_t chashtable;

	for (uint8_t j = 0; j < 2; j++) {
		for (uint16_t b = 16; b <= 2048; b *= 2) {
			/* SDBM hashing */
			memset(&chashtable, 0, sizeof(chashtable));
			rc = chashtable_init(&chashtable, b, hash_sdbm_str,
					     match, free);
			CuAssertIntEquals(tc, RC_SUCCESS, rc);
			CuAssertIntEquals(tc, 0, chashtable_size(&chashtable));
			for (uint16_t i = 1; i <= 1024; i++) {
				object_t *object;
				object = j == 0 ? init_object(i) : init_rnd_object(i);
				CuAssertPtrNotNull(tc, object);
				rc = chashtable_insert(&chashtable, object);
				CuAssertIntEquals(tc, RC_SUCCESS, rc);
			}
			load_factor(tc, &chashtable, j == 0 ? "sdbm (fixed)" :
				    "sdbm (random)");
			chashtable_destroy(&chashtable);
			CuAssertIntEquals(tc, 0, chashtable_size(&chashtable));

			/* DEK hashing */
			memset(&chashtable, 0, sizeof(chashtable));
			rc = chashtable_init(&chashtable, b, hash_dek_str,
					     match, free);
			CuAssertIntEquals(tc, RC_SUCCESS, rc);
			CuAssertIntEquals(tc, 0, chashtable_size(&chashtable));
			for (uint16_t i = 1; i <= 1024; i++) {
				object_t *object;
				object = j == 0 ? init_object(i) : init_rnd_object(i);
				CuAssertPtrNotNull(tc, object);
				rc = chashtable_insert(&chashtable, object);
				CuAssertIntEquals(tc, RC_SUCCESS, rc);
			}
			load_factor(tc, &chashtable,  j == 0 ? "dek (fixed)" :
				    "dek (random)");
			chashtable_destroy(&chashtable);
			CuAssertIntEquals(tc, 0, chashtable_size(&chashtable));

			/* DJB hashing */
			memset(&chashtable, 0, sizeof(chashtable));
			rc = chashtable_init(&chashtable, b, hash_djb_str,
					     match, free);
			CuAssertIntEquals(tc, RC_SUCCESS, rc);
			CuAssertIntEquals(tc, 0, chashtable_size(&chashtable));
			for (uint16_t i = 1; i <= 1024; i++) {
				object_t *object;
				object = j == 0 ? init_object(i) : init_rnd_object(i);
				CuAssertPtrNotNull(tc, object);
				rc = chashtable_insert(&chashtable, object);
				CuAssertIntEquals(tc, RC_SUCCESS, rc);
			}
			load_factor(tc, &chashtable, j == 0 ? "djb (fixed)" :
				    "djb (random)");
			chashtable_destroy(&chashtable);
			CuAssertIntEquals(tc, 0, chashtable_size(&chashtable));
			puts("");
		}
	}
}

CuSuite* chashtable_get_suite()
{
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test_chashtable_1);
    SUITE_ADD_TEST(suite, test_chashtable_2);

    return suite;
}
