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
#include <strings.h>
#include <time.h>
#include "tsmapi.h"
#include "qarray.h"
#include "log.h"
#include "CuTest.h"

void test_qarray(CuTest *tc)
{
    dsInt16_t rc;
    rc = init_qarray();
    CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
    
    CuAssertIntEquals_Msg(tc, "Number of elements in initial darray 0", 0, qarray_size());
    const unsigned int N = 123;
    const unsigned int n_magic = 17;
    qryRespArchiveData query_data;
    bzero(&query_data, sizeof(query_data));
    for (unsigned int n = 0; n < N; n++) {

	if (n == n_magic) {
	    query_data.objId.lo = n;
	    query_data.objId.hi = n * n;
	}
	
	rc = add_query(&query_data);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
    }
    
    char msg[64] = {0};
    sprintf(msg, "Number of elements in setup darray %d\n", N);
    CuAssertIntEquals_Msg(tc, msg, N, qarray_size());

    bzero(&query_data, sizeof(query_data));
    rc = get_query(&query_data, n_magic);
    CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
    CuAssertIntEquals_Msg(tc, "qryRespArchiveData lo matches", n_magic, query_data.objId.lo);
    CuAssertIntEquals_Msg(tc, "qryRespArchiveData hi matches", n_magic * n_magic,
			  query_data.objId.hi);
    
    destroy_qarray();
}

void test_qarray_sort_top(CuTest *tc)
{
    dsInt16_t rc;
    rc = init_qarray();
    CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);

    const unsigned int N = 4043;
    qryRespArchiveData query_data;
    srand(time(NULL));
    
    for (unsigned int n = 0; n < N; n++) {
	bzero(&query_data, sizeof(query_data));
	query_data.restoreOrderExt.top = rand() % N;
	rc = add_query(&query_data);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
    }

    sort_qarray();

    dsBool_t order_correct = bTrue;
    for (unsigned int n = 0; n < N-1 && order_correct; n++) {
	qryRespArchiveData query_data_a;
	rc = get_query(&query_data_a, n);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	
	qryRespArchiveData query_data_b;
	rc = get_query(&query_data_b, n + 1);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	/* Verify objects are sorting in ascending order (low to high). */
	order_correct = query_data_a.restoreOrderExt.top <=
	    query_data_b.restoreOrderExt.top ? bTrue : bFalse;
    }
    
    CuAssertIntEquals_Msg(tc, "Sorting queries in ascending order",
			  order_correct, bTrue);
    
    destroy_qarray();
}

void test_qarray_sort_all(CuTest *tc)
{
    dsInt16_t rc;
    rc = init_qarray();
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
	rc = add_query(&query_data);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
    }

    sort_qarray();

    dsBool_t order_correct = bTrue;
    for (unsigned int n = 0; n < N-1 && order_correct; n++) {
	qryRespArchiveData query_data_a;
	rc = get_query(&query_data_a, n);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	
	qryRespArchiveData query_data_b;
	rc = get_query(&query_data_b, n + 1);
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	/* Verify objects are sorting in ascending order (low to high). */

	order_correct = (cmp_restore_order(&query_data_a, &query_data_b) == DS_LESSTHAN ||
			 (cmp_restore_order(&query_data_a, &query_data_b) == DS_EQUAL)) ?
	    bTrue : bFalse;
    }
    
    CuAssertIntEquals_Msg(tc, "Sorting queries in ascending order",
			  order_correct, bTrue);
    
    destroy_qarray();
}

void test_qarray_init_destroy(CuTest *tc)
{
    dsInt16_t rc;

    for (unsigned char i = 0; i < 128; i++) {
	rc = init_qarray();
	CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
	destroy_qarray();
    }

    rc = init_qarray();
    CuAssertTrue(tc, rc == DSM_RC_SUCCESSFUL);
    rc = init_qarray();
    CuAssertTrue(tc, rc == DSM_RC_UNSUCCESSFUL);
    destroy_qarray();

    /* Adding a query to non initialized qarray should give DSM_RC_UNSUCCESSFUL. */
    qryRespArchiveData query_data;
    rc = add_query(&query_data);
    CuAssertTrue(tc, rc == DSM_RC_UNSUCCESSFUL);
}

CuSuite* qarray_get_suite()
{
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test_qarray);
    SUITE_ADD_TEST(suite, test_qarray_sort_top);
    SUITE_ADD_TEST(suite, test_qarray_sort_all);
    SUITE_ADD_TEST(suite, test_qarray_init_destroy);

    return suite;
}
