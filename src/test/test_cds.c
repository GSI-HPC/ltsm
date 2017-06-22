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
 * Copyright (c) 2016, GSI Helmholtz Centre for Heavy Ion Research
 */

#include <stdio.h>
#include <stdlib.h>
#include "CuTest.h"

CuSuite* dsstruct64_off64_t_get_suite();
CuSuite* list_get_suite();
CuSuite* chashtable_get_suite();
CuSuite* qtable_get_suite();

void run_all_tests(void) {
	CuString *output = CuStringNew();
	CuSuite* suite = CuSuiteNew();
	CuSuite* dsstruct64_off64_t_suite = dsstruct64_off64_t_get_suite();
	CuSuite* list_suite = list_get_suite();
	CuSuite* chashtable_suite = chashtable_get_suite();
	CuSuite* qtable_suite = qtable_get_suite();

	CuSuiteAddSuite(suite, dsstruct64_off64_t_suite);
	CuSuiteAddSuite(suite, list_suite);
	CuSuiteAddSuite(suite, chashtable_suite);
	CuSuiteAddSuite(suite, qtable_suite);

	CuSuiteRun(suite);
	CuSuiteSummary(suite, output);
	CuSuiteDetails(suite, output);
	printf("%s\n", output->buffer);

	CuSuiteDelete(qtable_suite);
	CuSuiteDelete(chashtable_suite);
	CuSuiteDelete(list_suite);
	CuSuiteDelete(dsstruct64_off64_t_suite);

	free(suite);
	CuStringDelete(output);
}

int main(void)
{
	run_all_tests();
	return 0;
}
