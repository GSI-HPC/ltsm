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
 * Copyright (c) 2016, 2017, GSI Helmholtz Centre for Heavy Ion Research
 */

#include <stdlib.h>
#include <strings.h>
#include <time.h>
#include "ltsmapi.h"
#include "qtable.c"
#include "log.h"
#include "CuTest.h"

void test_qtable(CuTest *tc)
{
	dsInt16_t rc;
	struct qtable_t qtable;
	memset(&qtable, 0, sizeof(struct qtable_t));
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 0, chashtable_size(qtable.chashtable));

	const uint32_t N = 1024;
	qryRespArchiveData qra_data;
	memset(&qra_data, 0, sizeof(qra_data));
	for (uint32_t n = 0; n < N; n++) {
		qra_data.objId.lo = n;
		qra_data.objId.hi = n * n;
		rc = insert_qtable(&qtable, &qra_data);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	/* Objects have identical keys and dates, thus their are overwritten. */
	CuAssertIntEquals(tc, 1, chashtable_size(qtable.chashtable));

	rc = create_array(&qtable, SORT_NONE);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 1, qtable.qarray.size);
	CuAssertPtrNotNull(tc, qtable.qarray.data);

	destroy_qtable(&qtable);
	CuAssertIntEquals(tc, 0, qtable.qarray.size);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);
	CuAssertPtrEquals(tc, NULL, qtable.qarray.data);
}

void test_qtable_sort_top(CuTest *tc)
{
	dsInt16_t rc;
	struct qtable_t qtable;
	memset(&qtable, 0, sizeof(struct qtable_t));
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

	const uint32_t N = 4187;
	qryRespArchiveData qra_data;
	srand(time(NULL));

	for (uint32_t n = 0; n < N; n++) {
		memset(&qra_data, 0, sizeof(qra_data));
		qra_data.insDate.year = N - n;
		qra_data.restoreOrderExt.top = rand() % N;
		rc = insert_qtable(&qtable, &qra_data);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals(tc, N, chashtable_size(qtable.chashtable));
	rc = create_array(&qtable, SORT_RESTORE_ORDER);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, qtable.qarray.size,
			  chashtable_size(qtable.chashtable));

	dsBool_t order_correct = bTrue;
	for (uint32_t n = 0; n < N-1 && order_correct; n++) {
		qryRespArchiveData qra_data_a;
		rc = get_qra(&qtable, &qra_data_a, n);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		qryRespArchiveData qra_data_b;
		rc = get_qra(&qtable, &qra_data_b, n + 1);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		/* Verify objects are sorting in ascending order
		   (low to high). */
		order_correct = qra_data_a.restoreOrderExt.top <=
			qra_data_b.restoreOrderExt.top ? bTrue : bFalse;
	}

	CuAssertIntEquals_Msg(tc, "queries are sorted by restore order in "
			      "ascending order", order_correct, bTrue);

	destroy_qtable(&qtable);
	CuAssertIntEquals(tc, 0, qtable.qarray.size);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);
	CuAssertPtrEquals(tc, NULL, qtable.qarray.data);
}

void test_qtable_sort_all(CuTest *tc)
{
	dsInt16_t rc;
	struct qtable_t qtable;
	memset(&qtable, 0, sizeof(struct qtable_t));
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 0, chashtable_size(qtable.chashtable));

	const uint32_t N = 18237;
	qryRespArchiveData qra_data;
	srand(time(NULL));

	for (uint32_t n = 0; n < N; n++) {
		memset(&qra_data, 0, sizeof(qra_data));
		qra_data.insDate.year = N - n;
		qra_data.restoreOrderExt.top = rand() % N;
		qra_data.restoreOrderExt.hi_hi = rand() % N;
		qra_data.restoreOrderExt.hi_lo = rand() % N;
		qra_data.restoreOrderExt.lo_hi = rand() % N;
		qra_data.restoreOrderExt.lo_lo = rand() % N;
		rc = insert_qtable(&qtable, &qra_data);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}

	rc = create_array(&qtable, SORT_RESTORE_ORDER);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, qtable.qarray.size,
			  chashtable_size(qtable.chashtable));

	dsBool_t order_correct = bTrue;
	for (uint32_t n = 0; n < N-1 && order_correct; n++) {
		qryRespArchiveData qra_data_a;
		rc = get_qra(&qtable, &qra_data_a, n);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		qryRespArchiveData qra_data_b;
		rc = get_qra(&qtable, &qra_data_b, n + 1);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		/* Verify objects are sorting in ascending order
		   (low to high). */
		order_correct = (cmp_restore_order(&qra_data_a, &qra_data_b) ==
				 DS_LESSTHAN ||
				 (cmp_restore_order(&qra_data_a, &qra_data_b) ==
				  DS_EQUAL)) ?
			bTrue : bFalse;
	}

	CuAssertIntEquals_Msg(tc, "sorting queries in ascending order",
			      order_correct, bTrue);

	destroy_qtable(&qtable);
	CuAssertIntEquals(tc, 0, qtable.qarray.size);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);
	CuAssertPtrEquals(tc, NULL, qtable.qarray.data);
}

void test_qtable_replace_older1(CuTest *tc)
{
	dsInt16_t rc;
	struct qtable_t qtable;
	memset(&qtable, 0, sizeof(struct qtable_t));
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 0, chashtable_size(qtable.chashtable));

	const uint32_t N = 1024;
	qryRespArchiveData *qra_arrdata;
	qra_arrdata = calloc(N, sizeof(qryRespArchiveData));
	CuAssertPtrNotNull(tc, qra_arrdata);
	for (uint32_t n = 0; n < N; n++) {
		sprintf(qra_arrdata[n].objName.fs, "%s", "fstest:");
		sprintf(qra_arrdata[n].objName.hl, "%s", "/hltest/");
		sprintf(qra_arrdata[n].objName.ll, "%s", "/lltest/");
	}

	qtable.multiple = bFalse;
	qra_arrdata[0].insDate.year = 1;
	rc = insert_qtable(&qtable, &qra_arrdata[0]);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 1, chashtable_size(qtable.chashtable));

	qra_arrdata[1].insDate.year = 2;
	/* Will be overwritten due to a more current date. */
	qtable.multiple = bTrue;
	rc = insert_qtable(&qtable, &qra_arrdata[1]);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 2, chashtable_size(qtable.chashtable));

	qra_arrdata[2].insDate.year = 3;
	/* Will not be overwritten although it has a newer date. */
	qtable.multiple = bFalse;
	rc = insert_qtable(&qtable, &qra_arrdata[2]);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 2, chashtable_size(qtable.chashtable));

	qra_arrdata[3].insDate.year = 4;
	/* Will be overwritten due to a more current date. */
	qtable.multiple = bTrue;
	rc = insert_qtable(&qtable, &qra_arrdata[3]);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 3, chashtable_size(qtable.chashtable));

	qtable.multiple = bTrue;
	for (uint32_t n = 0; n < N; n++) {
		qtable.multiple = bTrue;
		qra_arrdata[n].insDate.year = 10 + n;
		rc = insert_qtable(&qtable, &qra_arrdata[n]);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals(tc, N + 3, chashtable_size(qtable.chashtable));

	/* We did not created a sorted array, thus 0 and NULL. */
	CuAssertIntEquals(tc, 0, qtable.qarray.size);
	CuAssertPtrEquals(tc, NULL, qtable.qarray.data);

	free(qra_arrdata);
	destroy_qtable(&qtable);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);
}

void test_qtable_replace_older2(CuTest *tc)
{
	dsInt16_t rc;
	struct qtable_t qtable;
	memset(&qtable, 0, sizeof(struct qtable_t));
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 0, chashtable_size(qtable.chashtable));
	qtable.multiple = bFalse;

	const uint32_t N = 2048;
	qryRespArchiveData qra_data;
	memset(&qra_data, 0, sizeof(qra_data));
	sprintf(qra_data.objName.fs, "%s", "fstest:");
	sprintf(qra_data.objName.hl, "%s", "/hltest/");
	for (uint32_t n = 0; n < N; n++) {
		sprintf(qra_data.objName.ll, "%s%u", "/lltest", n);
		rc = insert_qtable(&qtable, &qra_data);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals(tc, N, chashtable_size(qtable.chashtable));

	memset(&qra_data, 0, sizeof(qra_data));
	sprintf(qra_data.objName.fs, "%s", "fstest:");
	sprintf(qra_data.objName.hl, "%s", "/hltest/");
	sprintf(qra_data.objName.ll, "%s", "/lltest/");
	for (uint32_t n = 0; n < N; n++) {
		qra_data.insDate.year = n;
		rc = insert_qtable(&qtable, &qra_data);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals(tc, N + 1, chashtable_size(qtable.chashtable));

	destroy_qtable(&qtable);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);
}

void test_qtable_multiple_init_destroy(CuTest *tc)
{
	dsInt16_t rc;
	struct qtable_t qtable;
	memset(&qtable, 0, sizeof(struct qtable_t));
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, 0, chashtable_size(qtable.chashtable));

	for (uint16_t i = 0; i < 8; i++) {
		rc = init_qtable(&qtable);
		CuAssertTrue(tc, rc == DSM_RC_UNSUCCESSFUL);
	}
	destroy_qtable(&qtable);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);

	for (uint16_t i = 0; i < 8; i++) {
		destroy_qtable(&qtable);
		CuAssertTrue(tc, rc == DSM_RC_UNSUCCESSFUL);
	}
}

void test_qtable_sort_date_ascending(CuTest *tc)
{
	dsInt16_t rc;
	struct qtable_t qtable;
	memset(&qtable, 0, sizeof(struct qtable_t));
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	qtable.multiple = bTrue;

	const uint32_t N = 4187;
	qryRespArchiveData qra_data;
	srand(time(NULL));

	for (uint32_t n = 0; n < N; n++) {
		memset(&qra_data, 0, sizeof(qra_data));
		qra_data.insDate.year = rand() % N;
		qra_data.insDate.month = rand() % 12;
		qra_data.insDate.hour = rand() % 24;
		qra_data.insDate.minute = rand() % 60;
		qra_data.insDate.second = rand() % 60;
		rc = insert_qtable(&qtable, &qra_data);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals(tc, N, chashtable_size(qtable.chashtable));
	rc = create_array(&qtable, SORT_DATE_ASCENDING);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, qtable.qarray.size,
			  chashtable_size(qtable.chashtable));

	dsBool_t order_correct = bTrue;
	for (uint32_t n = 0; n < N-1 && order_correct; n++) {
		qryRespArchiveData qra_data_a;
		rc = get_qra(&qtable, &qra_data_a, n);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		qryRespArchiveData qra_data_b;
		rc = get_qra(&qtable, &qra_data_b, n + 1);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		/* Verify objects are sorting in ascending date order
		   (low to high). */
		order_correct = date_in_sec(&qra_data_a.insDate) <=
			date_in_sec(&qra_data_b.insDate) ? bTrue : bFalse;
	}

	CuAssertIntEquals_Msg(tc, "queries are sorted by date in ascending "
			      "order", order_correct, bTrue);

	destroy_qtable(&qtable);
	CuAssertIntEquals(tc, 0, qtable.qarray.size);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);
	CuAssertPtrEquals(tc, NULL, qtable.qarray.data);

	/* No multiples */
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	qtable.multiple = bFalse;

	srand(time(NULL));

	sprintf(qra_data.objName.fs, "%s", "fstest:");
	sprintf(qra_data.objName.hl, "%s", "/hltest/");

	for (uint32_t n = 0; n < N; n++) {
		memset(&qra_data, 0, sizeof(qra_data));
		qra_data.insDate.year = rand() % N;
		qra_data.insDate.month = rand() % 12;
		qra_data.insDate.hour = rand() % 24;
		qra_data.insDate.minute = rand() % 60;
		qra_data.insDate.second = rand() % 60;
		sprintf(qra_data.objName.ll, "%s%u", "/lltest", n);
		rc = insert_qtable(&qtable, &qra_data);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals(tc, N, chashtable_size(qtable.chashtable));
	rc = create_array(&qtable, SORT_DATE_ASCENDING);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, qtable.qarray.size,
			  chashtable_size(qtable.chashtable));

	order_correct = bTrue;
	for (uint32_t n = 0; n < N-1 && order_correct; n++) {
		qryRespArchiveData qra_data_a;
		rc = get_qra(&qtable, &qra_data_a, n);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		qryRespArchiveData qra_data_b;
		rc = get_qra(&qtable, &qra_data_b, n + 1);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		/* Verify objects are sorting in ascending date order
		   (low to high). */
		order_correct = date_in_sec(&qra_data_a.insDate) <=
			date_in_sec(&qra_data_b.insDate) ? bTrue : bFalse;
	}

	CuAssertIntEquals_Msg(tc, "queries are sorted by date in ascending order",
			      order_correct, bTrue);

	destroy_qtable(&qtable);
	CuAssertIntEquals(tc, 0, qtable.qarray.size);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);
	CuAssertPtrEquals(tc, NULL, qtable.qarray.data);

}

void test_qtable_sort_date_descending(CuTest *tc)
{
	dsInt16_t rc;
	struct qtable_t qtable;
	memset(&qtable, 0, sizeof(struct qtable_t));
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	qtable.multiple = bTrue;

	const uint32_t N = 4187;
	qryRespArchiveData qra_data;
	srand(time(NULL));

	for (uint32_t n = 0; n < N; n++) {
		memset(&qra_data, 0, sizeof(qra_data));
		qra_data.insDate.year = rand() % N;
		qra_data.insDate.month = rand() % 12;
		qra_data.insDate.hour = rand() % 24;
		qra_data.insDate.minute = rand() % 60;
		qra_data.insDate.second = rand() % 60;
		rc = insert_qtable(&qtable, &qra_data);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals(tc, N, chashtable_size(qtable.chashtable));
	rc = create_array(&qtable, SORT_DATE_DESCENDING);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, qtable.qarray.size,
			  chashtable_size(qtable.chashtable));

	dsBool_t order_correct = bTrue;
	for (uint32_t n = 0; n < N-1 && order_correct; n++) {
		qryRespArchiveData qra_data_a;
		rc = get_qra(&qtable, &qra_data_a, n);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		qryRespArchiveData qra_data_b;
		rc = get_qra(&qtable, &qra_data_b, n + 1);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		/* Verify objects are sorting in descending date order
		   (low to high). */
		order_correct = date_in_sec(&qra_data_a.insDate) >=
			date_in_sec(&qra_data_b.insDate) ? bTrue : bFalse;
	}

	CuAssertIntEquals_Msg(tc, "queries are sorted by date in descending "
			      "order", order_correct, bTrue);

	destroy_qtable(&qtable);
	CuAssertIntEquals(tc, 0, qtable.qarray.size);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);
	CuAssertPtrEquals(tc, NULL, qtable.qarray.data);

	/* No multiples */
	rc = init_qtable(&qtable);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	qtable.multiple = bFalse;

	srand(time(NULL));

	sprintf(qra_data.objName.fs, "%s", "fstest:");
	sprintf(qra_data.objName.hl, "%s", "/hltest/");

	for (uint32_t n = 0; n < N; n++) {
		memset(&qra_data, 0, sizeof(qra_data));
		qra_data.insDate.year = rand() % N;
		qra_data.insDate.month = rand() % 12;
		qra_data.insDate.hour = rand() % 24;
		qra_data.insDate.minute = rand() % 60;
		qra_data.insDate.second = rand() % 60;
		sprintf(qra_data.objName.ll, "%s%u", "/lltest", n);
		rc = insert_qtable(&qtable, &qra_data);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	}
	CuAssertIntEquals(tc, N, chashtable_size(qtable.chashtable));
	rc = create_array(&qtable, SORT_DATE_DESCENDING);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	CuAssertIntEquals(tc, qtable.qarray.size,
			  chashtable_size(qtable.chashtable));

	order_correct = bTrue;
	for (uint32_t n = 0; n < N-1 && order_correct; n++) {
		qryRespArchiveData qra_data_a;
		rc = get_qra(&qtable, &qra_data_a, n);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		qryRespArchiveData qra_data_b;
		rc = get_qra(&qtable, &qra_data_b, n + 1);
		CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

		/* Verify objects are sorting in descending date order
		   (low to high). */
		order_correct = date_in_sec(&qra_data_a.insDate) >=
			date_in_sec(&qra_data_b.insDate) ? bTrue : bFalse;
	}

	CuAssertIntEquals_Msg(tc, "queries are sorted by date in descending order",
			      order_correct, bTrue);

	destroy_qtable(&qtable);
	CuAssertIntEquals(tc, 0, qtable.qarray.size);
	CuAssertPtrEquals(tc, NULL, qtable.chashtable);
	CuAssertPtrEquals(tc, NULL, qtable.qarray.data);

}

CuSuite* qtable_get_suite()
{
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test_qtable);
    SUITE_ADD_TEST(suite, test_qtable_sort_top);
    SUITE_ADD_TEST(suite, test_qtable_sort_all);
    SUITE_ADD_TEST(suite, test_qtable_replace_older1);
    SUITE_ADD_TEST(suite, test_qtable_replace_older2);
    SUITE_ADD_TEST(suite, test_qtable_multiple_init_destroy);
    SUITE_ADD_TEST(suite, test_qtable_sort_date_ascending);
    SUITE_ADD_TEST(suite, test_qtable_sort_date_descending);

    return suite;
}
