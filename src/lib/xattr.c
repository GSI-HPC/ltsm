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

#include <pthread.h>
#include <sys/xattr.h>
#include "xattr.h"

static pthread_mutex_t xattr_mutex = PTHREAD_MUTEX_INITIALIZER;

int xattr_get_fsd(const char *fpath_local,
		  uint32_t *fsd_action_state,
		  int *archive_id,
		  struct fsd_info_t *fsd_info)
{
	int rc;
	struct fsd_info_t _fsd_info = {
		.fs		    = {0},
		.fpath		    = {0},
		.desc		    = {0}
	};

	pthread_mutex_lock(&xattr_mutex);

	rc = getxattr(fpath_local, XATTR_FSD_STATE,
		      (uint32_t *)fsd_action_state, sizeof(uint32_t));
	if (rc < 0) {
		rc = -errno;
		goto out;
	}

	rc = getxattr(fpath_local, XATTR_FSD_ARCHIVE_ID,
		      (int *)archive_id, sizeof(int));
	if (rc < 0) {
		rc = -errno;
		goto out;
	}

	rc = getxattr(fpath_local, XATTR_FSD_FS,
		      (char *)_fsd_info.fs, DSM_MAX_FSNAME_LENGTH);
	if (rc < 0) {
		rc = -errno;
		goto out;
	} else
		strncpy(fsd_info->fs, _fsd_info.fs, sizeof(fsd_info->fs));

	rc = getxattr(fpath_local, XATTR_FSD_FPATH,
		      (char *)_fsd_info.fpath, PATH_MAX_COMPAT);
	if (rc < 0) {
		rc = -errno;
		goto out;
	} else
		strncpy(fsd_info->fpath, _fsd_info.fpath, sizeof(fsd_info->fpath));

	rc = getxattr(fpath_local, XATTR_FSD_DESC,
		      (char *)_fsd_info.desc, DSM_MAX_DESCR_LENGTH);
	if (rc < 0) {
		rc = -errno;
		goto out;
	} else
		strncpy(fsd_info->desc, _fsd_info.desc, sizeof(fsd_info->desc));

out:
	pthread_mutex_unlock(&xattr_mutex);

	return (rc < 0 ? rc : 0);
}

int xattr_set_fsd(const char *fpath_local,
		  const uint32_t fsd_action_state,
		  const int archive_id,
		  const struct fsd_info_t *fsd_info)
{
	int rc;

	pthread_mutex_lock(&xattr_mutex);

	rc = setxattr(fpath_local, XATTR_FSD_STATE,
		      (uint32_t *)&fsd_action_state, sizeof(uint32_t), 0);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "setxattr '%s %s'", fpath_local, XATTR_FSD_STATE);
		goto out;
	}

	rc = setxattr(fpath_local, XATTR_FSD_ARCHIVE_ID,
		      (int *)&archive_id, sizeof(int), 0);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "setxattr '%s %s'", fpath_local, XATTR_FSD_ARCHIVE_ID);
		goto out;
	}

	rc = setxattr(fpath_local, XATTR_FSD_FS,
		      (char *)fsd_info->fs, DSM_MAX_FSNAME_LENGTH, 0);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "setxattr '%s %s'", fpath_local, XATTR_FSD_FS);
		goto out;
	}

	rc = setxattr(fpath_local, XATTR_FSD_FPATH,
		      (char *)fsd_info->fpath, PATH_MAX_COMPAT, 0);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "setxattr '%s %s'", fpath_local, XATTR_FSD_FPATH);
		goto out;
	}

	rc = setxattr(fpath_local, XATTR_FSD_DESC,
		      (char *)fsd_info->desc, DSM_MAX_DESCR_LENGTH, 0);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "setxattr '%s %s'", fpath_local, XATTR_FSD_DESC);
	}

out:
	pthread_mutex_unlock(&xattr_mutex);

	return rc;
}

int xattr_update_fsd_state(struct fsd_action_item_t *fsd_action_item,
			   const uint32_t fsd_action_state)
{
	int rc;

	pthread_mutex_lock(&xattr_mutex);
	rc = setxattr(fsd_action_item->fpath_local, XATTR_FSD_STATE,
		      (uint32_t *)&fsd_action_state, sizeof(uint32_t), 0);
	pthread_mutex_unlock(&xattr_mutex);
	if (rc < 0) {
		rc = -errno;
		CT_ERROR(rc, "setxattr '%s %s'", fsd_action_item->fpath_local,
			 XATTR_FSD_STATE);
	} else
		fsd_action_item->fsd_action_state = fsd_action_state;

	return rc;
}
