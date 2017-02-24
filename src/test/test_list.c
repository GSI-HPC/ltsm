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
 * Copyright (c) 2017 Thomas Stibor <t.stibor@gsi.de>
 */

#include <stdlib.h>
#include <strings.h>
#include <time.h>
#include "list.h"
#include "CuTest.h"

void test_list(CuTest *tc)
{
	list_t list;
	int rc;
	int *data;
	const unsigned int I = 20;

	list_init(&list, free);
	for (unsigned int i = 1; i <= I; i++) {
		data = malloc(sizeof(int));
		CuAssertPtrNotNull(tc, data);
		*data = i;
		rc = list_ins_next(&list, NULL, data);
		CuAssertIntEquals(tc, 0, rc);
	}
	list_node_t *node_head = list_head(&list);
	CuAssertPtrNotNull(tc, node_head);
	int *val_head = list_data(node_head);
	CuAssertIntEquals(tc, I, *val_head);

	list_node_t *node_tail = list_tail(&list);
	CuAssertPtrNotNull(tc, node_tail);
	int *val_tail = list_data(node_tail);
	CuAssertIntEquals(tc, 1, *val_tail);

	data = malloc(sizeof(int));
	CuAssertPtrNotNull(tc, data);
	*data = 0;
	rc = list_ins_next(&list, list_tail(&list), data);
	CuAssertIntEquals(tc, 0, rc);

	CuAssertIntEquals(tc, I + 1, list_size(&list));

	list_node_t *node = list_head(&list);
	rc = list_rem_next(&list, node, (void **)&data);
	CuAssertIntEquals(tc, 0, rc);
	free(data);

	CuAssertIntEquals(tc, I, list_size(&list));

	list_destroy(&list);
}

CuSuite* list_get_suite()
{
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test_list);

    return suite;
}
