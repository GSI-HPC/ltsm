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
 * Copyright (c) 2016, 2017 Thomas Stibor <t.stibor@gsi.de>
 */

#include <stdlib.h>
#include <strings.h>
#include <time.h>
#include "tsmapi.h"
#include "qarray.h"
#include "log.h"
#include "CuTest.h"

void test_qarray(CuTest *tc)
{
	dsInt16_t rc;
	struct qarray_t *qarray = NULL;
	rc = init_qarray(&qarray);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

	CuAssertIntEquals_Msg(tc, "number of elements in initial qarray", 0,
			      qarray_size(qarray));

	const unsigned int N = 123;
	const unsigned int n_magic = 17;
	qryRespArchiveData query_data;
	bzero(&query_data, sizeof(query_data));
	for (unsigned int n = 0; n < N; n++) {
		if (n == n_magic) {
			query_data.objId.lo = n;
			query_data.objId.hi = n * n;
		}

		rc = insert_query(&query_data, &qarray, bFalse);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals_Msg(tc, "number of elements in modified qarray", N, qarray_size(qarray));

	bzero(&query_data, sizeof(query_data));
	rc = get_query(&query_data, qarray, n_magic);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals_Msg(tc, "qryRespArchiveData lo matches", n_magic,
			      query_data.objId.lo);
	CuAssertIntEquals_Msg(tc, "qryRespArchiveData hi matches",
			      n_magic * n_magic, query_data.objId.hi);

	destroy_qarray(&qarray);
	CuAssertPtrEquals_Msg(tc, "qarray is NULL", NULL, qarray);
}

void test_qarray_replace_oldest(CuTest *tc)
{
	dsInt16_t rc;
	struct qarray_t *qarray = NULL;
	rc = init_qarray(&qarray);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

	CuAssertIntEquals_Msg(tc, "number of elements in initial qarray", 0,
			      qarray_size(qarray));

	qryRespArchiveData **query_data = NULL;
	query_data = malloc(4 * sizeof(qryRespArchiveData *));
	CuAssertPtrNotNull(tc, query_data);
	for (unsigned char c = 0; c < 4; c++) {
		query_data[c] = calloc(1, sizeof(qryRespArchiveData));
		CuAssertPtrNotNull(tc, query_data[c]);
		sprintf(query_data[c]->objName.fs, "%s", "fstest:");
		sprintf(query_data[c]->objName.hl, "%s", "/hltest/");
		sprintf(query_data[c]->objName.ll, "%s", "/lltest/");
	}

	query_data[0]->insDate.year = 1;
	rc = insert_query(query_data[0], &qarray, bFalse);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals_Msg(tc, "number of elements in modified qarray", 1, qarray_size(qarray));

	query_data[1]->insDate.year = 2;
	/* Will be overwritten due to a more current date. */
	rc = insert_query(query_data[1], &qarray, bTrue);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals_Msg(tc, "number of elements in modified qarray", 1, qarray_size(qarray));

	query_data[2]->insDate.year = 3;
	rc = insert_query(query_data[2], &qarray, bFalse);
	/* Will not be overwritten although it has a newer date. */
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals_Msg(tc, "number of elements in modified qarray", 2, qarray_size(qarray));

	query_data[3]->insDate.year = 4;
	rc = insert_query(query_data[3], &qarray, bTrue);
	/* Will be overwritten due to a more current date. */
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals_Msg(tc, "number of elements in modified qarray", 2, qarray_size(qarray));

	qryRespArchiveData query_data_get;
	rc = get_query(&query_data_get, qarray, qarray_size(qarray) - 1);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

	CuAssertStrEquals(tc, "fstest:", query_data_get.objName.fs);
	CuAssertStrEquals(tc, "/hltest/", query_data_get.objName.hl);
	CuAssertStrEquals(tc, "/lltest/", query_data_get.objName.ll);

	for (unsigned char c = 0; c < 4; c++)
		free(query_data[c]);
	free(query_data);

	destroy_qarray(&qarray);
	CuAssertPtrEquals_Msg(tc, "qarray is NULL", NULL, qarray);
}

void test_qarray_replace_oldest_scale(CuTest *tc)
{
	dsInt16_t rc;
	struct qarray_t *qarray = NULL;
	rc = init_qarray(&qarray);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

	CuAssertIntEquals_Msg(tc, "number of elements in initial qarray", 0,
			      qarray_size(qarray));

	const unsigned int N = 2048;
	qryRespArchiveData query_data;
	bzero(&query_data, sizeof(query_data));
	sprintf(query_data.objName.fs, "%s", "fstest:");
	sprintf(query_data.objName.hl, "%s", "/hltest/");
	for (unsigned int n = 0; n < N; n++) {
		sprintf(query_data.objName.ll, "%s%d", "/lltest", n);
		rc = insert_query(&query_data, &qarray, bTrue);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals_Msg(tc, "number of elements in modified qarray", N, qarray_size(qarray));

	bzero(&query_data, sizeof(query_data));
	sprintf(query_data.objName.fs, "%s", "fstest:");
	sprintf(query_data.objName.hl, "%s", "/hltest/");
	sprintf(query_data.objName.ll, "%s", "/lltest/");
	for (unsigned int n = 0; n < N; n++) {
		query_data.insDate.year = n;
		rc = insert_query(&query_data, &qarray, bTrue);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals_Msg(tc, "number of elements in modified qarray", N + 1, qarray_size(qarray));

	destroy_qarray(&qarray);
	CuAssertPtrEquals_Msg(tc, "qarray is NULL", NULL, qarray);
}

void test_qarray_sort_top(CuTest *tc)
{
	dsInt16_t rc;
	struct qarray_t *qarray = NULL;
	rc = init_qarray(&qarray);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

	const unsigned int N = 4187;
	qryRespArchiveData query_data;
	srand(time(NULL));

	for (unsigned int n = 0; n < N; n++) {
		bzero(&query_data, sizeof(query_data));
		query_data.restoreOrderExt.top = rand() % N;
		rc = insert_query(&query_data, &qarray, bFalse);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}

	sort_qarray(&qarray);

	dsBool_t order_correct = bTrue;
	for (unsigned int n = 0; n < N-1 && order_correct; n++) {
		qryRespArchiveData query_data_a;
		rc = get_query(&query_data_a, qarray, n);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		qryRespArchiveData query_data_b;
		rc = get_query(&query_data_b, qarray, n + 1);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		/* Verify objects are sorting in ascending order (low to high). */
		order_correct = query_data_a.restoreOrderExt.top <=
			query_data_b.restoreOrderExt.top ? bTrue : bFalse;
	}

	CuAssertIntEquals_Msg(tc, "sorting queries in ascending order",
			      order_correct, bTrue);

	destroy_qarray(&qarray);
	CuAssertPtrEquals_Msg(tc, "qarray is NULL", NULL, qarray);
}

void test_qarray_sort_all(CuTest *tc)
{
	dsInt16_t rc;
	struct qarray_t *qarray = NULL;
	rc = init_qarray(&qarray);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

	const unsigned int N = 18237;
	qryRespArchiveData query_data;
	srand(time(NULL));

	for (unsigned int n = 0; n < N; n++) {
		bzero(&query_data, sizeof(query_data));
		query_data.restoreOrderExt.top = rand() % N;
		query_data.restoreOrderExt.hi_hi = rand() % N;
		query_data.restoreOrderExt.hi_lo = rand() % N;
		query_data.restoreOrderExt.lo_hi = rand() % N;
		query_data.restoreOrderExt.lo_lo = rand() % N;
		rc = insert_query(&query_data, &qarray, bFalse);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}

	sort_qarray(&qarray);

	dsBool_t order_correct = bTrue;
	for (unsigned int n = 0; n < N-1 && order_correct; n++) {
		qryRespArchiveData query_data_a;
		rc = get_query(&query_data_a, qarray, n);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		qryRespArchiveData query_data_b;
		rc = get_query(&query_data_b, qarray, n + 1);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		/* Verify objects are sorting in ascending order (low to high). */
		order_correct = (cmp_restore_order(&query_data_a, &query_data_b) == DS_LESSTHAN ||
				 (cmp_restore_order(&query_data_a, &query_data_b) == DS_EQUAL)) ?
			bTrue : bFalse;
	}

	CuAssertIntEquals_Msg(tc, "sorting queries in ascending order",
			      order_correct, bTrue);

	destroy_qarray(&qarray);
	CuAssertPtrEquals_Msg(tc, "qarray is NULL", NULL, qarray);
}

void test_qarray_init_destroy(CuTest *tc)
{
	dsInt16_t rc;
	struct qarray_t *qarray = NULL;

	for (unsigned char i = 0; i < 128; i++) {
		rc = init_qarray(&qarray);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
		destroy_qarray(&qarray);
		CuAssertPtrEquals_Msg(tc, "qarray is NULL", NULL, qarray);
	}

	rc = init_qarray(&qarray);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	rc = init_qarray(&qarray);
	CuAssertTrue(tc, rc == DSM_RC_UNSUCCESSFUL);
	destroy_qarray(&qarray);
	CuAssertPtrEquals_Msg(tc, "qarray is NULL", NULL, qarray);

	/* Adding a query to non initialized qarray should give DSM_RC_UNSUCCESSFUL. */
	qryRespArchiveData query_data;
	bzero(&query_data, sizeof(query_data));
	rc = insert_query(&query_data, &qarray, bFalse);
	CuAssertTrue(tc, rc == DSM_RC_UNSUCCESSFUL);
}

CuSuite* qarray_get_suite()
{
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test_qarray);
    SUITE_ADD_TEST(suite, test_qarray_replace_oldest);
    SUITE_ADD_TEST(suite, test_qarray_replace_oldest_scale);
    SUITE_ADD_TEST(suite, test_qarray_sort_top);
    SUITE_ADD_TEST(suite, test_qarray_sort_all);
    SUITE_ADD_TEST(suite, test_qarray_init_destroy);

    return suite;
}
