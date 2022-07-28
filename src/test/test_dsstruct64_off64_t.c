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
 * Copyright (c) 2016, GSI Helmholtz Centre for Heavy Ion Research
 */

#include <stdlib.h>
#include <strings.h>
#include "ltsmapi.c"
#include "log.h"
#include "CuTest.h"

void test_dsstruct64_off64_t(CuTest *tc)
{
	off64_t size_off64_t;
	dsStruct64_t size_ds_struct64_t;
	off64_t size_off64_t_res;

	unsigned char shift = 0;

	while (shift < 64) {

		size_off64_t = 1ULL << shift;
		size_ds_struct64_t = to_dsStruct64_t(size_off64_t); /* Convert off64_t to dsStruct64_t */
		size_off64_t_res = to_off64_t(size_ds_struct64_t);   /* Convert dsStruct64_t to off64_t */
		printf("size_off64_t: %20jd, size_off64_t(dsStruct64_t): %20jd\n", size_off64_t, size_off64_t_res);
		CuAssertIntEquals_Msg(tc, "off64_t -> dsStruct64_t -> off64_t", size_off64_t, size_off64_t_res);

		size_off64_t_res = to_off64_t(size_ds_struct64_t);
		CuAssertIntEquals_Msg(tc, "dsStruct64_t -> off64_t", size_off64_t, size_off64_t_res);

		shift += 2;
	}
}

CuSuite* dsstruct64_off64_t_get_suite()
{
	CuSuite* suite = CuSuiteNew();
	SUITE_ADD_TEST(suite, test_dsstruct64_off64_t);

	return suite;
}
