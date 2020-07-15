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
 * Copyright (c) 2019-2020, GSI Helmholtz Centre for Heavy Ion Research
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

#define NODE			"polaris"
#define PASSWORD		"polaris1234"
#define OWNER			""
#define FSD_HOSTNAME		"localhost"
#define FSD_PORT		7625
#define NUM_FILES_XATTR		500
#define NUM_FILES_FSD_CRC32	5
#define NUM_FILES_FSD		50
#define LEN_RND_STR		8
#define LUSTRE_MOUNTP		"/lustre"
#define FSD_MOUNTP		"/fsddata"

void test_fsd_xattr(CuTest *tc)
{
	char rnd_chars[0xffff] = {0};
	char fpath[NUM_FILES_XATTR][PATH_MAX];

	memset(fpath, 0, sizeof(char) * NUM_FILES_XATTR * PATH_MAX);

	for (uint16_t r = 0; r < NUM_FILES_XATTR; r++) {

		int rc;
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
		struct fsd_info_t fsd_info = {
			.fs		   = {0},
			.fpath		   = {0},
			.desc		   = {0},
			.fsd_storage_dest  = 0
		};
		enum fsd_storage_dest_t fsd_storage_dest[] = {
			FSD_STORAGE_LOCAL,
			FSD_STORAGE_LUSTRE,
			FSD_STORAGE_LUSTRE_TSM,
			FSD_STORAGE_TSM
		};

		rnd_str(fsd_info.fs, rand() % DSM_MAX_FSNAME_LENGTH);
		rnd_str(fsd_info.fpath, rand() % PATH_MAX_COMPAT);
		rnd_str(fsd_info.desc, rand() % DSM_MAX_DESCR_LENGTH);
		fsd_info.fsd_storage_dest = fsd_storage_dest[rand() %
							     (sizeof(fsd_storage_dest) /
							      sizeof(fsd_storage_dest[0]))];

		uint32_t fsd_action_state_ac = 0;
		int archive_id_ac = 0;
		struct fsd_info_t fsd_info_ac = {
			.fs		      = {0},
			.fpath		      = {0},
			.desc		      = {0},
			.fsd_storage_dest  = 0
		};

		rc = xattr_set_fsd(fpath[r], fsd_action_state, archive_id, &fsd_info);
		CuAssertIntEquals(tc, 0, rc);

		rc = xattr_get_fsd(fpath[r], &fsd_action_state_ac, &archive_id_ac, &fsd_info_ac);
		CuAssertIntEquals(tc, 0, rc);

		CuAssertIntEquals(tc, fsd_action_state, fsd_action_state_ac);
		CuAssertIntEquals(tc, archive_id, archive_id_ac);
		CuAssertStrEquals(tc, fsd_info.fs, fsd_info_ac.fs);
		CuAssertStrEquals(tc, fsd_info.fpath, fsd_info_ac.fpath);
		CuAssertStrEquals(tc, fsd_info.desc, fsd_info_ac.desc);
		CuAssertIntEquals(tc, fsd_info.fsd_storage_dest, fsd_info_ac.fsd_storage_dest);

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
	char fpath[NUM_FILES_FSD][PATH_MAX];

	memset(fpath, 0, sizeof(char) * NUM_FILES_FSD * PATH_MAX);
	memset(&fsd_session, 0, sizeof(struct fsd_session_t));

	rc = fsd_fconnect(&fsd_login, &fsd_session);
	CuAssertIntEquals(tc, 0, rc);

	for (uint16_t r = 0; r < NUM_FILES_FSD; r++) {

		char fpath_rnd[PATH_MAX + 1] = {0};
		char str_rnd[LEN_RND_STR + 1] = {0};

		for (uint8_t d = 0; d < (rand() % 0x0a) + 1; d++) {
			rnd_str(str_rnd, LEN_RND_STR);
			CuAssertPtrNotNull(tc, strcat(fpath_rnd, "/"));
			CuAssertPtrNotNull(tc, strcat(fpath_rnd, str_rnd));
		}

		sprintf(fpath[r], "%s%s", LUSTRE_MOUNTP, fpath_rnd);
		CT_DEBUG("fpath '%s'", fpath[r]);

		rc = fsd_fopen(LUSTRE_MOUNTP, fpath[r], NULL, &fsd_session);
		CuAssertIntEquals(tc, 0, rc);

		for (uint8_t b = 0; b < rand() % 0xff; b++) {

			const size_t len = rand() % sizeof(rnd_chars);
			ssize_t bytes_written;

			rnd_str(rnd_chars, len);

			bytes_written = fsd_fwrite(rnd_chars, len, 1, &fsd_session);
			CuAssertIntEquals(tc, len, bytes_written);
		}

		rc = fsd_fclose(&fsd_session);
		CuAssertIntEquals(tc, 0, rc);
	}

	fsd_fdisconnect(&fsd_session);
}

void test_fsd_fcalls_with_crc32(CuTest *tc)
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
	char fpath[NUM_FILES_FSD_CRC32][PATH_MAX];

	memset(fpath, 0, sizeof(char) * NUM_FILES_FSD_CRC32 * PATH_MAX);
	memset(&fsd_session, 0, sizeof(struct fsd_session_t));

	rc = fsd_fconnect(&fsd_login, &fsd_session);
	CuAssertIntEquals(tc, 0, rc);

	for (uint16_t r = 0; r < NUM_FILES_FSD_CRC32; r++) {

		char fpath_rnd[PATH_MAX + 1] = {0};
		char str_rnd[LEN_RND_STR + 1] = {0};
		uint32_t crc32sum_buf = 0;
		uint32_t crc32sum_file = 0;

		for (uint8_t d = 0; d < (rand() % 0x0a) + 1; d++) {
			rnd_str(str_rnd, LEN_RND_STR);
			CuAssertPtrNotNull(tc, strcat(fpath_rnd, "/"));
			CuAssertPtrNotNull(tc, strcat(fpath_rnd, str_rnd));
		}

		sprintf(fpath[r], "%s%s", LUSTRE_MOUNTP, fpath_rnd);
		CT_DEBUG("fpath '%s'", fpath[r]);

		rc = fsd_fopen(LUSTRE_MOUNTP, fpath[r], NULL, &fsd_session);
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

		/* Make sure rc = unlink(...) is commented out, to keep the
		   file for crc32 verification. */
#if 0
		/* Verify data is correctly copied to fsd server. */
		sprintf(fpath[r], "%s%s", FSD_MOUNTP, fpath_rnd);
		CT_DEBUG("fpath fsd '%s'", fpath[r]);
		sleep(2); /* Give Linux some time to flush data to disk. */
		rc = crc32file(fpath[r], &crc32sum_file);
		CT_INFO("buf:fsd crc32 (0x%08x, 0x%08x) '%s'",
			crc32sum_buf, crc32sum_file, fpath[r]);
		CuAssertIntEquals(tc, 0, rc);
		CuAssertTrue(tc, crc32sum_buf == crc32sum_file);
#endif

		/* Verify data is correctly copied to lustre. */
		sprintf(fpath[r], "%s%s", LUSTRE_MOUNTP, fpath_rnd);
		CT_DEBUG("fpath lustre '%s'", fpath[r]);
		sleep(3); /* Give Linux some time to flush data to disk. */
		rc = crc32file(fpath[r], &crc32sum_file);
		CT_INFO("buf:lustre crc32 (0x%08x, 0x%08x) '%s'",
			crc32sum_buf, crc32sum_file, fpath[r]);
		CuAssertIntEquals(tc, 0, rc);
		CuAssertTrue(tc, crc32sum_buf == crc32sum_file);
	}

	fsd_fdisconnect(&fsd_session);
}

CuSuite* fsdapi_get_suite()
{
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test_fsd_xattr);
    SUITE_ADD_TEST(suite, test_fsd_fcalls_with_crc32);
    SUITE_ADD_TEST(suite, test_fsd_fcalls);

    return suite;
}

void run_all_tests(void)
{
	api_msg_set_level(API_MSG_INFO);

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
