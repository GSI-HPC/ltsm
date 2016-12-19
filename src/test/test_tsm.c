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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Copyright (c) 2016, Thomas Stibor <t.stibor@gsi.de>
 */

#include <stdlib.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <time.h>
#include "tsmapi.h"
#include "setup_env.h"
#include "CuTest.h"

#define SIZE_BUF (1 << 16)	/* 2^16 = 65536 */
#define TEMPLATE "/tmp/random.XXXXXX"

dsInt16_t create_rand_fpath(char *fpath)
{
	int fd;
	snprintf(fpath, strlen(TEMPLATE) + 1, "%s", TEMPLATE);

	fd = mkstemp(fpath);
	if (fd < 0) {
		CT_ERROR(errno, "mkstemp");
		return DSM_RC_UNSUCCESSFUL;
	}
	return DSM_RC_SUCCESSFUL;
}

void fill_buf_rand(char *buf, size_t length)
{
	srand(time(NULL));
	for (size_t i = 0; i < length; i++)
		buf[i] = rand();
}

#if 0
dsInt16_t create_data_fpath(char *_fpath, size_t size)
{
	return DSM_RC_UNSUCCESSFUL;
}
#endif

void test_tsm_archive_fpath(CuTest *tc)
{
	dsInt16_t rc;
#if 0
	char fpath[PATH_MAX + 1] = {0};
#endif
	login_t login;

	init_login(&login);
	rc = tsm_init(&login);
	CuAssertIntEquals_Msg(tc, "tsm_init failed", DSM_RC_SUCCESSFUL, rc);
#if 0
	create_data_fpath(fpath, (1 << 20));
	rc = tsm_archive_fpath(login.fsname, fpath, NULL);
	CuAssertIntEquals_Msg(tc, "tsm_archive_fpath failed", DSM_RC_SUCCESSFUL, rc);
#endif
	tsm_quit();
}

CuSuite* tsm_get_suite()
{
	CuSuite* suite = CuSuiteNew();
	SUITE_ADD_TEST(suite, test_tsm_archive_fpath);

	return suite;
}
