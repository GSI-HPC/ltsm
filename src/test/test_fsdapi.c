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
 * Copyright (c) 2019, GSI Helmholtz Centre for Heavy Ion Research
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/xattr.h>
#include "CuTest.h"
#include "common.h"
#include "fsdapi.c"
#include "xattr.h"
#include "test_utils.h"

#define NODE		"polaris"
#define PASSWORD	"polaris1234"
#define OWNER           ""

#define FSD_HOSTNAME    "localhost"
#define FSD_PORT         7625

#define NUM_FILES       10
#define LEN_RND_STR     6

void test_fsd_xattr(CuTest *tc)
{
	int rc;
	char rnd_chars[0xffff] = {0};
	char fpath[NUM_FILES][PATH_MAX];

	memset(fpath, 0, sizeof(char) * NUM_FILES * PATH_MAX);

	for (uint8_t r = 0; r < NUM_FILES; r++) {

		FILE *file;
		char rnd_s[LEN_RND_STR + 1] = {0};

		rnd_str(rnd_s, LEN_RND_STR);
		snprintf(fpath[r], PATH_MAX, "/tmp/%s", rnd_s);

		file = fopen(fpath[r], "w+");
		CuAssertPtrNotNull(tc, file);

		for (uint8_t b = 0; b < rand() % 0xff; b++) {

			const size_t len = rand() % sizeof(rnd_chars);
			ssize_t bytes_written;

			rnd_str(rnd_chars, len);

			bytes_written = fwrite(rnd_chars, 1, len, file);
			CuAssertIntEquals(tc, len, bytes_written);
		}
		rc = fclose(file);
		CuAssertIntEquals(tc, 0, rc);

		const uint32_t fsd_action_states[] = {
			STATE_FSD_COPY_DONE,
			STATE_LUSTRE_COPY_RUN,
			STATE_LUSTRE_COPY_ERROR,
			STATE_LUSTRE_COPY_DONE,
			STATE_TSM_ARCHIVE_RUN,
			STATE_TSM_ARCHIVE_ERROR,
			STATE_TSM_ARCHIVE_DONE,
			STATE_FILE_OMITTED
		};
		uint32_t fsd_action_state = fsd_action_states[rand() %
							      (sizeof(fsd_action_states) /
							       sizeof(fsd_action_states[0]))];
		const int archive_id = rand() % 0xff;
		char desc[DSM_MAX_DESCR_LENGTH + 1] = {0};
		rnd_str(desc, rand() % DSM_MAX_DESCR_LENGTH);

		uint32_t fsd_action_state_ac = 0;
		int archive_id_ac = 0;
		char desc_ac[DSM_MAX_DESCR_LENGTH + 1] = {0};

		rc = xattr_set_fsd(fpath[r], fsd_action_state, archive_id, desc);
		CuAssertIntEquals(tc, 0, rc);

		rc = xattr_get_fsd(fpath[r], &fsd_action_state_ac, &archive_id_ac, desc_ac);
		CuAssertIntEquals(tc, 0, rc);

		CuAssertIntEquals(tc, fsd_action_state, fsd_action_state_ac);
		CuAssertIntEquals(tc, archive_id, archive_id_ac);
		CuAssertStrEquals(tc, desc, desc_ac);

		struct fsd_action_item_t fsd_action_item;
		memset(&fsd_action_item, 0, sizeof(struct fsd_action_item_t));
		strncpy(fsd_action_item.fpath_local, fpath[r], PATH_MAX);
		fsd_action_state = fsd_action_states[rand() %
						     (sizeof(fsd_action_states) /
						      sizeof(fsd_action_states[0]))];
		rc = xattr_update_fsd_state(&fsd_action_item, fsd_action_state);
		CuAssertIntEquals(tc, 0, rc);
		CuAssertIntEquals(tc, fsd_action_state, fsd_action_item.fsd_action_state);

		rc = unlink(fpath[r]);
		CuAssertIntEquals(tc, 0, rc);
	}
}

void test_fsd_fcalls(CuTest *tc)
{
	int rc;
	struct fsd_session_t fsd_session;
	struct fsd_login_t fsd_login = {
		.node		     = NODE,
		.password	     = PASSWORD,
		.hostname	     = FSD_HOSTNAME,
		.port		     = FSD_PORT
	};
	char rnd_chars[0xffff] = {0};
	char fpath[NUM_FILES][PATH_MAX];

	memset(fpath, 0, sizeof(char) * NUM_FILES * PATH_MAX);
	memset(&fsd_session, 0, sizeof(struct fsd_session_t));

	rc = fsd_fconnect(&fsd_login, &fsd_session);
	CuAssertIntEquals(tc, 0, rc);

	for (uint8_t r = 0; r < NUM_FILES; r++) {

		char rnd_s[LEN_RND_STR + 1] = {0};
		uint32_t crc32sum_buf = 0;
#if 0
		uint32_t crc32sum_file = 0;
#endif

		rnd_str(rnd_s, LEN_RND_STR);
		snprintf(fpath[r], PATH_MAX, "/lustre/%s", rnd_s);

		rc = fsd_fopen("/", fpath[r], NULL, &fsd_session);
		CuAssertIntEquals(tc, 0, rc);

		for (uint8_t b = 0; b < rand() % 0xff; b++) {

			const size_t len = rand() % sizeof(rnd_chars);
			ssize_t bytes_written;

			rnd_str(rnd_chars, len);

			bytes_written = fsd_fwrite(rnd_chars, len, 1, &fsd_session);
			CuAssertIntEquals(tc, len, bytes_written);

			crc32sum_buf = crc32(crc32sum_buf, (const unsigned char *)rnd_chars, len);
		}

		rc = fsd_fclose(&fsd_session);
		CuAssertIntEquals(tc, 0, rc);
#if 0
		/* Verify data is correctly copied to fsd server. */
		snprintf(fpath[r], PATH_MAX, "/fsddata/lustre/%s", rnd_s);
		CT_DEBUG("fpath fsd '%s'", fpath[r]);
		sleep(2); /* Give Linux some time to flush data to disk. */
		rc = crc32file(fpath[r], &crc32sum_file);
		CT_INFO("buf crc32 %lu, file crc32 %lu", crc32sum_buf, crc32sum_file);
		CuAssertIntEquals(tc, 0, rc);
		CuAssertTrue(tc, crc32sum_buf == crc32sum_file);

		/* Verify data is correctly copied to lustre. */
		snprintf(fpath[r], PATH_MAX, "/lustre/%s", rnd_s);
		CT_DEBUG("fpath lustre '%s'", fpath[r]);
		sleep(1); /* Give Linux some time to flush data to disk. */
		rc = crc32file(fpath[r], &crc32sum_file);
		CT_INFO("buf crc32 %lu, file crc32 %lu", crc32sum_buf, crc32sum_file);
		CuAssertIntEquals(tc, 0, rc);
		CuAssertTrue(tc, crc32sum_buf == crc32sum_file);
#endif
	}

	fsd_fdisconnect(&fsd_session);
}

CuSuite* fsdapi_get_suite()
{
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test_fsd_xattr);
    SUITE_ADD_TEST(suite, test_fsd_fcalls);

    return suite;
}

void run_all_tests(void)
{
	api_msg_set_level(API_MSG_DEBUG);

	CuString *output = CuStringNew();
	CuSuite *suite = CuSuiteNew();
	CuSuite *fsdapi_suite = fsdapi_get_suite();

	CuSuiteAddSuite(suite, fsdapi_suite);

	CuSuiteRun(suite);
	CuSuiteSummary(suite, output);
	CuSuiteDetails(suite, output);
	printf("%s\n", output->buffer);

	CuSuiteDelete(fsdapi_suite);

	free(suite);
	CuStringDelete(output);
}

int main(void)
{
	struct timespec tspec = {0};

	clock_gettime(CLOCK_MONOTONIC, &tspec);
	srand(time(NULL) + tspec.tv_nsec);
	run_all_tests();

	return 0;
}
