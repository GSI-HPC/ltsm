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

#ifndef XATTR_H
#define XATTR_H

#include "fsqapi.h"

/* PATH_MAX = 4096 does not work on EXT4. It results in
   No space left on device (28). PATH_MAX though works on XFS. */
#define PATH_MAX_COMPAT 2048

/* Note: extended attributes can be listed with cmd:
   getfattr -d -m ".*" -e hex <FILE> */

int xattr_get_fsq(const char *fpath_local,
		  uint32_t *fsq_action_state,
		  int *archive_id,
		  struct fsq_info_t *fsq_info);

int xattr_set_fsq(const char *fpath_local,
		  const uint32_t fsq_action_state,
		  const int archive_id,
		  const struct fsq_info_t *fsq_info);

int xattr_update_fsq_state(struct fsq_action_item_t *fsq_action_item,
			   const uint32_t fsq_action_state);

#endif	/* XATTR_H */
