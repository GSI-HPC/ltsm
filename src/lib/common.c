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
 * Copyright (c) 2019-2020, GSI Helmholtz Centre for Heavy Ion Research
 */

#include "common.h"

static int parse_line(char *line, struct kv_opt *kv_opt)
{
	if (line[0] == '#' || line[0] == '\n')
		return 0;

	const char *delim = " \t\r\n";
	char *token;
	char *saveptr;
	uint16_t cnt = 0;
	struct kv _kv = {.key = {0},
			 .val = {0}};

	token = strtok_r(line, delim, &saveptr);
	while(token != NULL) {
		if (token[0] == '#')
			break;
		strncpy(_kv.key, token, MAX_OPTIONS_LENGTH);
		cnt++;
		token = strtok_r(NULL, delim, &saveptr);
		if (token) {
			strncpy(_kv.val, token, MAX_OPTIONS_LENGTH);
			cnt++;
		}
		token = strtok_r(NULL, delim, &saveptr);
	}
	if (cnt != 2)
		return -EINVAL;

	kv_opt->kv = realloc(kv_opt->kv, sizeof(struct kv) * (kv_opt->N + 1));
	if (!kv_opt->kv)
		return -ENOMEM;

	memset(kv_opt->kv[kv_opt->N].key, 0, MAX_OPTIONS_LENGTH + 1);
	memset(kv_opt->kv[kv_opt->N].val, 0, MAX_OPTIONS_LENGTH + 1);
	strncpy(kv_opt->kv[kv_opt->N].key, _kv.key, MAX_OPTIONS_LENGTH + 1);
	strncpy(kv_opt->kv[kv_opt->N].val, _kv.val, MAX_OPTIONS_LENGTH + 1);
	kv_opt->N++;

	return 0;
}

ssize_t read_size(int fd, void *ptr, size_t n)
{
	size_t bytes_total = 0;
	char *buf;

	buf = ptr;
	while (bytes_total < n) {
		ssize_t bytes_read;

		bytes_read = read(fd, buf, n - bytes_total);

		if (bytes_read == 0)
			return bytes_total;
		if (bytes_read == -1)
			return -errno;

		bytes_total += bytes_read;
		buf += bytes_read;
	}

	return bytes_total;
}

ssize_t write_size(int fd, const void *ptr, size_t n)
{
	size_t bytes_total = 0;
	const char *buf;

	buf = ptr;
	while (bytes_total < n) {
		ssize_t bytes_written;

		bytes_written = write(fd, buf, n - bytes_total);

		if (bytes_written == 0)
			return bytes_total;
		if (bytes_written == -1)
			return -errno;

		bytes_total += bytes_written;
		buf += bytes_written;
	}

	return bytes_total;
}

int parse_conf(const char *filename, struct kv_opt *kv_opt)
{
	FILE *file = NULL;
	char *line = NULL;
	size_t len = 0;
	ssize_t nread;
	int rc = 0;

	file = fopen(filename, "r");
	if (!file) {
		CT_ERROR(errno, "fopen failed on '%s'", filename);
		return -errno;
	}

	errno = 0;
	while ((nread = getline(&line, &len, file)) != -1) {
		rc = parse_line(line, kv_opt);
		if (rc == -EINVAL)
			CT_WARN("malformed option '%s' in conf file '%s'",
				line, filename);
		else if (rc == -ENOMEM) {
			CT_ERROR(rc, "realloc");
			goto cleanup;
		}
	}

	if (errno) {
		rc = -errno;
		CT_ERROR(errno, "getline failed");
	}

cleanup:
	if (line) {
		free(line);
		line = NULL;
	}
	fclose(file);

	return rc;
}

int crc32file(const char *filename, uint32_t *crc32result)
{
	int rc = 0;
	FILE *file;
	size_t cur_read;
	uint32_t crc32sum = 0;
	unsigned char buf[TSM_BUF_LENGTH] = {0};

	file = fopen(filename, "r");
	if (file == NULL) {
		rc = -errno;
		CT_ERROR(rc, "fopen failed on '%s'", filename);

		return rc;
	}

	do {
		cur_read = fread(buf, 1, TSM_BUF_LENGTH, file);
		if (ferror(file)) {
			rc = -EIO;
			CT_ERROR(rc, "fread failed on '%s'", filename);
			break;
		}
		crc32sum = crc32(crc32sum, (const unsigned char *)buf,
				 cur_read);

	} while (!feof(file));

	int rc_minor;

	rc_minor = fclose(file);
	if (rc_minor) {
		rc_minor = -errno;
		CT_ERROR(rc_minor, "fclose failed on '%s'", filename);

		return rc_minor;
	}

	*crc32result = crc32sum;

	return rc;
}

void login_init(struct login_t *login, const char *servername,
                const char *node, const char *password,
                const char *owner, const char *platform,
                const char *fsname, const char *fstype)
{
        if (!login || !servername)
                return;

        const uint16_t s_arg_len = 1 + strlen(servername) +
                strlen("-se=");
        if (s_arg_len < MAX_OPTIONS_LENGTH)
                snprintf(login->options, s_arg_len, "-se=%s", servername);
        else
                CT_WARN("Option parameter \'-se=%s\' is larger than "
                        "MAX_OPTIONS_LENGTH: %d and is ignored\n",
                        servername, MAX_OPTIONS_LENGTH);

        if (node)
                strncpy(login->node, node, DSM_MAX_NODE_LENGTH);
        else
                login->node[0] = '\0';
        if (password)
                strncpy(login->password, password, DSM_MAX_VERIFIER_LENGTH);
        else
                login->password[0] = '\0';
        if (owner)
                strncpy(login->owner, owner, DSM_MAX_OWNER_LENGTH);
        else
                login->owner[0] = '\0';
        if (platform)
                strncpy(login->platform, platform, DSM_MAX_PLATFORM_LENGTH);
        else
                login->platform[0] = '\0';
        if (fsname)
                strncpy(login->fsname, fsname, DSM_MAX_FSNAME_LENGTH);
        else
                login->fsname[0] = '\0';
        if (fstype)
                strncpy(login->fstype, fstype, DSM_MAX_FSTYPE_LENGTH);
        else
                login->fstype[0] = '\0';
}
