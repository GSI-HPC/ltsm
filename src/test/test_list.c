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

#include <stdint.h>
#include "list.h"
#include "CuTest.h"

void test_list_1(CuTest *tc)
{
	list_t list;

	list_init(&list, free);
	CuAssertPtrEquals(tc, NULL, list.head);
	CuAssertPtrEquals(tc, NULL, list_head(&list));
	CuAssertPtrEquals(tc, NULL, list.tail);
	CuAssertPtrEquals(tc, NULL, list_tail(&list));
	CuAssertIntEquals(tc, 0, list_size(&list));

	list_destroy(&list);
	CuAssertPtrEquals(tc, NULL, list.head);
	CuAssertPtrEquals(tc, NULL, list_head(&list));
	CuAssertPtrEquals(tc, NULL, list.tail);
	CuAssertPtrEquals(tc, NULL, list_tail(&list));
	CuAssertIntEquals(tc, 0, list_size(&list));
}

void test_list_2(CuTest *tc)
{
	list_t list;
	list_init(&list, free);

	const uint16_t I = 4;
	int *data = NULL;
	int rc;

	/* We expect: head->4->3->2->1<-tail */
	for (uint16_t i = 1; i <= I; i++) {
		data = malloc(sizeof(int));
		CuAssertPtrNotNull(tc, data);
		*data = i;
		rc = list_ins_next(&list, NULL, data);
		CuAssertIntEquals(tc, 0, rc);
	}

	/* Insert node with data: 10 between ..->3->[10]->2->.. */
	list_node_t *mid_node = list_head(&list)->next;
	data = malloc(sizeof(int));
	CuAssertPtrNotNull(tc, data);
	*data = 10;
	rc = list_ins_next(&list, mid_node, data);
	CuAssertIntEquals(tc, 0, rc);
	CuAssertIntEquals(tc, 1, *(int *)list_data(list_tail(&list)));
	CuAssertIntEquals(tc, 4, *(int *)list_data(list_head(&list)));

	/* We expect: head->4->3->10->2->1<-tail */
	list_node_t *n = list_head(&list);
	CuAssertIntEquals(tc, 4, *(int *)list_data(n));
	CuAssertIntEquals(tc, 3, *(int *)list_data(list_next(n)));
	CuAssertIntEquals(tc, 10, *(int *)list_data(list_next(list_next(n))));
	CuAssertIntEquals(tc, 2, *(int *)list_data(list_next(list_next(list_next(n)))));
	CuAssertIntEquals(tc, 1, *(int *)list_data(list_next(list_next(list_next(list_next(n))))));

	/* Remove node with data 2. */
	list_node_t *rm_node = list_head(&list)->next->next;
	int *rd = NULL;
	rc = list_rem_next(&list, rm_node, (void **)&rd);
	CuAssertIntEquals(tc, 0, rc);
	CuAssertIntEquals(tc, 2, *rd);
	free(rd);

	/* We expect: head->4->3->10->1<-tail */
	n = list_head(&list);
	CuAssertIntEquals(tc, 4, *(int *)list_head(&list)->data);
	CuAssertIntEquals(tc, 4, *(int *)list_data(n));
	CuAssertIntEquals(tc, 3, *(int *)list_data(list_next(n)));
	CuAssertIntEquals(tc, 10, *(int *)list_data(list_next(list_next(n))));
	CuAssertIntEquals(tc, 1, *(int *)list_data(list_next(list_next(list_next(n)))));
	CuAssertIntEquals(tc, 1, *(int *)list_tail(&list)->data);

	list_destroy(&list);
	CuAssertPtrEquals(tc, NULL, list.head);
	CuAssertPtrEquals(tc, NULL, list_head(&list));
	CuAssertPtrEquals(tc, NULL, list.tail);
	CuAssertPtrEquals(tc, NULL, list_tail(&list));
	CuAssertIntEquals(tc, 0, list_size(&list));
}

CuSuite* list_get_suite()
{
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test_list_1);
    SUITE_ADD_TEST(suite, test_list_2);

    return suite;
}
