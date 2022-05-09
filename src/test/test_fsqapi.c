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
 * Copyright (c) 2019-2022, GSI Helmholtz Centre for Heavy Ion Research
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
#include "fsqapi.c"
#include "xattr.h"
#include "test_utils.h"

#define NODE			"polaris"
#define PASSWORD		"polaris1234"
#define OWNER			""
#define FSQ_HOSTNAME		"localhost"
#define NUM_FILES_XATTR		500
#define NUM_FILES_FSQ		15
#define LEN_RND_STR		8
#define LUSTRE_MOUNTP		"/lustre"
#define FSQ_MOUNTP		"/fsqdata"

void test_fsq_xattr(CuTest *tc)
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

		const uint32_t fsq_action_states[] = {
			STATE_LOCAL_COPY_DONE,
			STATE_LUSTRE_COPY_RUN,
			STATE_LUSTRE_COPY_ERROR,
			STATE_LUSTRE_COPY_DONE,
			STATE_TSM_ARCHIVE_RUN,
			STATE_TSM_ARCHIVE_ERROR,
			STATE_TSM_ARCHIVE_DONE,
			STATE_FILE_OMITTED,
			STATE_FILE_KEEP
		};
		uint32_t fsq_action_state = fsq_action_states[rand() %
							      (sizeof(fsq_action_states) /
							       sizeof(fsq_action_states[0]))];
		const int archive_id = rand() % 0xff;
		struct fsq_info_t fsq_info = {
			.fs		   = {0},
			.fpath		   = {0},
			.desc		   = {0},
			.fsq_storage_dest  = 0
		};
		enum fsq_storage_dest_t fsq_storage_dest[] = {
			FSQ_STORAGE_LOCAL,
			FSQ_STORAGE_LUSTRE,
			FSQ_STORAGE_LUSTRE_TSM,
			FSQ_STORAGE_TSM,
                        FSQ_STORAGE_NULL
		};

		rnd_str(fsq_info.fs, rand() % DSM_MAX_FSNAME_LENGTH);
		rnd_str(fsq_info.fpath, rand() % PATH_MAX_COMPAT);
		rnd_str(fsq_info.desc, rand() % DSM_MAX_DESCR_LENGTH);
		fsq_info.fsq_storage_dest = fsq_storage_dest[rand() %
							     (sizeof(fsq_storage_dest) /
							      sizeof(fsq_storage_dest[0]))];

		uint32_t fsq_action_state_ac = 0;
		int archive_id_ac = 0;
		struct fsq_info_t fsq_info_ac = {
			.fs		      = {0},
			.fpath		      = {0},
			.desc		      = {0},
			.fsq_storage_dest  = 0
		};

		rc = xattr_set_fsq(fpath[r], fsq_action_state, archive_id, &fsq_info);
		CuAssertIntEquals(tc, 0, rc);

		rc = xattr_get_fsq(fpath[r], &fsq_action_state_ac, &archive_id_ac, &fsq_info_ac);
		CuAssertIntEquals(tc, 0, rc);

		CuAssertIntEquals(tc, fsq_action_state, fsq_action_state_ac);
		CuAssertIntEquals(tc, archive_id, archive_id_ac);
		CuAssertStrEquals(tc, fsq_info.fs, fsq_info_ac.fs);
		CuAssertStrEquals(tc, fsq_info.fpath, fsq_info_ac.fpath);
		CuAssertStrEquals(tc, fsq_info.desc, fsq_info_ac.desc);
		CuAssertIntEquals(tc, fsq_info.fsq_storage_dest, fsq_info_ac.fsq_storage_dest);

		struct fsq_action_item_t fsq_action_item;
		memset(&fsq_action_item, 0, sizeof(struct fsq_action_item_t));
		strcpy(fsq_action_item.fpath_local, fpath[r]);
		fsq_action_state = fsq_action_states[rand() %
						     (sizeof(fsq_action_states) /
						      sizeof(fsq_action_states[0]))];
		rc = xattr_update_fsq_state(&fsq_action_item, fsq_action_state);
		CuAssertIntEquals(tc, 0, rc);
		CuAssertIntEquals(tc, fsq_action_state, fsq_action_item.fsq_action_state);

		rc = unlink(fpath[r]);
		CuAssertIntEquals(tc, 0, rc);
	}
}

void test_fsq_fcalls(CuTest *tc)
{
	int rc;
	struct fsq_session_t fsq_session;
	struct fsq_login_t fsq_login = {
		.node		     = NODE,
		.password	     = PASSWORD,
		.hostname	     = FSQ_HOSTNAME,
		.port		     = FSQ_PORT_DEFAULT
	};
	char rnd_chars[0xffff] = {0};
	char fpath[NUM_FILES_FSQ][PATH_MAX];
	struct fsq_login_t fsq_login_init;
	char node_overflow[DSM_MAX_NODE_LENGTH + 2];

	memset(fpath, 0, sizeof(char) * NUM_FILES_FSQ * PATH_MAX);
	memset(&fsq_session, 0, sizeof(struct fsq_session_t));

	rc = fsq_init(NULL, NULL, NULL, NULL);
	CuAssertIntEquals(tc, -EFAULT, rc);

	rc = fsq_init(&fsq_login_init, NULL, NULL, NULL);
	CuAssertIntEquals(tc, -EFAULT, rc);

	rc = fsq_init(&fsq_login_init, NODE, NULL, NULL);
	CuAssertIntEquals(tc, -EFAULT, rc);

	rc = fsq_init(&fsq_login_init, NODE, PASSWORD, NULL);
	CuAssertIntEquals(tc, -EFAULT, rc);

	rc = fsq_init(&fsq_login_init, NODE, NULL, FSQ_HOSTNAME);
	CuAssertIntEquals(tc, -EFAULT, rc);

	memset(node_overflow, 'x', sizeof(node_overflow));
	rc = fsq_init(&fsq_login_init, node_overflow, PASSWORD, FSQ_HOSTNAME);
	CuAssertIntEquals(tc, -EOVERFLOW, rc);

	rc = fsq_init(&fsq_login_init, NODE, PASSWORD, FSQ_HOSTNAME);
	CuAssertIntEquals(tc, 0, rc);

	CuAssertStrEquals(tc, fsq_login.node, fsq_login_init.node);
	CuAssertStrEquals(tc, fsq_login.password, fsq_login_init.password);
	CuAssertStrEquals(tc, fsq_login.hostname, fsq_login_init.hostname);
	CuAssertIntEquals(tc, fsq_login.port, fsq_login_init.port);

	rc = fsq_fconnect(&fsq_login, &fsq_session);
	CuAssertIntEquals(tc, 0, rc);

	for (uint8_t fsq_storage_dest = FSQ_STORAGE_NULL;
	     fsq_storage_dest <= FSQ_STORAGE_LUSTRE; ++fsq_storage_dest) {

		for (uint16_t r = 0; r < NUM_FILES_FSQ; r++) {

			char fpath_rnd[PATH_MAX + 1] = {0};
			char str_rnd[LEN_RND_STR + 1] = {0};

			for (uint8_t d = 0; d < (rand() % 0x0a) + 1; d++) {
				rnd_str(str_rnd, LEN_RND_STR);
				CuAssertPtrNotNull(tc, strcat(fpath_rnd, "/"));
				CuAssertPtrNotNull(tc,
						   strcat(fpath_rnd, str_rnd));
			}

			sprintf(fpath[r], "%s%s", LUSTRE_MOUNTP, fpath_rnd);
			CT_DEBUG("fpath '%s'", fpath[r]);

			rc = fsq_fdopen(LUSTRE_MOUNTP, fpath[r], NULL,
					fsq_storage_dest, &fsq_session);
			CuAssertIntEquals(tc, 0, rc);

			uint32_t crc32sum_buf = 0;
			uint32_t crc32sum_file = 0;
			struct fsq_info_t fsq_info;

			memset(&fsq_info, 0, sizeof(fsq_info));
			memcpy(&fsq_info, &fsq_session.fsq_packet.fsq_info, sizeof(fsq_info));

			CT_INFO("fs: '%s', fpath: '%s', desc: '%s', storage dest: '%s'",
				fsq_info.fs, fsq_info.fpath, fsq_info.desc,
				FSQ_STORAGE_DEST_STR(fsq_info.fsq_storage_dest));

			for (uint8_t b = 0; b < rand() % 0xff; b++) {

				const size_t len = rand() % sizeof(rnd_chars);
				ssize_t bytes_written;

				rnd_str(rnd_chars, len);

				bytes_written = fsq_fwrite(rnd_chars, len, 1, &fsq_session);
				CuAssertIntEquals(tc, len, bytes_written);

				crc32sum_buf = crc32(crc32sum_buf, (const unsigned char *)rnd_chars, len);
			}

			rc = fsq_fclose(&fsq_session);
			CuAssertIntEquals(tc, 0, rc);

			if (fsq_storage_dest == FSQ_STORAGE_LOCAL || fsq_storage_dest == FSQ_STORAGE_LUSTRE) {
				sleep(2); /* Give Linux some time to flush data to disk. */

				/* Verify data is correctly copied to fsq server. */
				sprintf(fpath[r], "%s%s", fsq_storage_dest == FSQ_STORAGE_LOCAL ?
					FSQ_MOUNTP : LUSTRE_MOUNTP, fpath_rnd);
				rc = crc32file(fpath[r], &crc32sum_file);
				CT_INFO("%s crc32 (0x%08x, 0x%08x) '%s'",
					FSQ_STORAGE_DEST_STR(fsq_storage_dest),
					crc32sum_buf, crc32sum_file, fpath[r]);
				CuAssertIntEquals(tc, 0, rc);
				CuAssertTrue(tc, crc32sum_buf == crc32sum_file);
			}
		}
	}

	fsq_fdisconnect(&fsq_session);
}

CuSuite* fsqapi_get_suite()
{
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, test_fsq_xattr);
    SUITE_ADD_TEST(suite, test_fsq_fcalls);

    return suite;
}

void run_all_tests(void)
{
	api_msg_set_level(API_MSG_INFO);

	CuString *output = CuStringNew();
	CuSuite *suite = CuSuiteNew();
	CuSuite *fsqapi_suite = fsqapi_get_suite();

	CuSuiteAddSuite(suite, fsqapi_suite);

	CuSuiteRun(suite);
	CuSuiteSummary(suite, output);
	CuSuiteDetails(suite, output);
	printf("%s\n", output->buffer);

	CuSuiteDelete(fsqapi_suite);

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
