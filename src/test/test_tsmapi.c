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
 * Copyright (c) 2016, Thomas Stibor <t.stibor@gsi.de>
 */

#include <stdio.h>
#include <stdlib.h>
#include "CuTest.h"

CuSuite* qarray_get_suite();
CuSuite* dsstruct64_off64_t_get_suite();

void run_all_tests(void) {
    CuString *output = CuStringNew();
    CuSuite* suite = CuSuiteNew();

    CuSuite* qarray_suite = qarray_get_suite();
    CuSuite* dsstruct64_off64_t_suite = dsstruct64_off64_t_get_suite();

    CuSuiteAddSuite(suite, qarray_suite);
    CuSuiteAddSuite(suite, dsstruct64_off64_t_suite);

    CuSuiteRun(suite);
    CuSuiteSummary(suite, output);
    CuSuiteDetails(suite, output);
    printf("%s\n", output->buffer);

    CuSuiteDelete(qarray_suite);
    CuSuiteDelete(dsstruct64_off64_t_suite);

    free(suite);
    CuStringDelete(output);
}

int main(void)
{
    run_all_tests();
}
