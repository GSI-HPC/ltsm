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

#include "CuTest.h"
#include "tsmapi.c"

#define SERVERNAME	"polaris-kvm-tsm-server"
#define NODE		"polaris"
#define PASSWORD	"polaris"
#define OWNER           ""

void test_fwrite(CuTest *tc)
{
	int rc;
	struct login_t login;
	const char *fpath = "/tmp/ltsm/testing/01.data";

	login_fill(&login, SERVERNAME, NODE, PASSWORD,
		   OWNER, LINUX_PLATFORM, DEFAULT_FSNAME,
		   DEFAULT_FSTYPE);

	struct session_t session;
	bzero(&session, sizeof(struct session_t));

	rc = tsm_init(DSM_SINGLETHREAD);
	CuAssertIntEquals(tc, DSM_RC_SUCCESSFUL, rc);

	rc = tsm_fopen(DEFAULT_FSNAME, fpath, "written by cutest",
		       &login, &session);
	CuAssertIntEquals(tc, DSM_RC_SUCCESSFUL, rc);

	char buf[65536 * 2] = {0};
	for (uint16_t i = 1; i <= 256; i *= 2) {
		ssize_t len;
		memset(buf, i, sizeof(buf));
		len = tsm_fwrite(buf, 1, sizeof(buf), &session);
		CuAssertIntEquals(tc, sizeof(buf), len);
	}

	rc = tsm_fclose(&session);
	CuAssertIntEquals(tc, DSM_RC_SUCCESSFUL, rc);

	tsm_cleanup(DSM_SINGLETHREAD);
}

void test_extract_hl_ll(CuTest *tc)
{
	const char *fpath = "/fs/hl/ll";
	const char *fs = "/fs";
	char hl[DSM_MAX_HL_LENGTH + 1] = {0};
	char ll[DSM_MAX_LL_LENGTH + 1] = {0};

	dsInt16_t rc;
	rc = extract_hl_ll(fpath, fs, hl, ll);
	CuAssertIntEquals(tc, DSM_RC_SUCCESSFUL, rc);
	CuAssertStrEquals(tc, "/hl", hl);
	CuAssertStrEquals(tc, "/ll", ll);
}

CuSuite* tsmapi_get_suite()
{
    CuSuite* suite = CuSuiteNew();
#if TEST_F_OPEN_WRITE_CLOSE
    SUITE_ADD_TEST(suite, test_fwrite);
#endif
    SUITE_ADD_TEST(suite, test_extract_hl_ll);

    return suite;
}

void run_all_tests(void)
{
	CuString *output = CuStringNew();
	CuSuite *suite = CuSuiteNew();
	CuSuite *tsmapi_suite = tsmapi_get_suite();

	CuSuiteAddSuite(suite, tsmapi_suite);

	CuSuiteRun(suite);
	CuSuiteSummary(suite, output);
	CuSuiteDetails(suite, output);
	printf("%s\n", output->buffer);

	CuSuiteDelete(tsmapi_suite);

	free(suite);
	CuStringDelete(output);
}

int main(void)
{
	run_all_tests();
	return 0;
}
