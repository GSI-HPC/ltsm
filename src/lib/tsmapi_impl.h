/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Definitions for internal use of the tsmapi library.
 */

#ifndef TSMAPI_IMPL_H
#define TSMAPI_IMPL_H

#include "tsmapi.h"

/* archive fsm definitions */
struct file_cursor_t {
	int		fd;	/* 0 if not valid */
	off_t		pos;
	size_t		len;
	dsBool_t	local;

	size_t		sent;
};

enum archive_states_t {
		TSMAPI_BEGIN_TX,
		TSMAPI_END_TX,
		TSMAPI_GET_FILE,
		TSMAPI_SEND_OBJ,
		TSMAPI_END_SEND_OBJ,
		TSMAPI_SEND_DATA,
		TSMAPI_FINISHED
};

struct archive_fsm_state_t {

	enum archive_states_t	state;

	struct session_t 	*session;

	struct archive_info_t	*archive_info;
	struct file_cursor_t	file;
	dsBool_t		tx_open;

	void			*data_buf;
};

#define ARCHIVE_FSM_INITIALIZER()	{ .state = TSMAPI_BEGIN_TX }



#endif /* TSMAPI_IMPL_H */
