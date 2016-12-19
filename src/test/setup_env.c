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

#include "setup_env.h"

#define ENV_TO_LOGIN(env_str, login_str, length)	     \
do {							     \
	if (getenv(env_str))				     \
		strncpy(login_str, getenv(env_str), length); \
} while (0)


void init_login(login_t *login)
{
	char *servername = getenv(ENV_LTSM_SERVERNAME);
	memset(&(*login), 0, sizeof(*login));
	ENV_TO_LOGIN(ENV_LTSM_NODE, login->node, DSM_MAX_NODE_LENGTH);
	ENV_TO_LOGIN(ENV_LTSM_OWNER, login->owner, DSM_MAX_OWNER_LENGTH);
	ENV_TO_LOGIN(ENV_LTSM_PASSWORD, login->password,
		     DSM_MAX_VERIFIER_LENGTH);
	ENV_TO_LOGIN(ENV_LTSM_FSPACE_NAME, login->fsname,
		     DSM_MAX_FSNAME_LENGTH);

	strcpy(login->platform, LOGIN_PLATFORM);
	strcpy(login->fstype, FSPACE_TYPE);

	if (servername)
		snprintf(login->options, 1 + strlen(servername) +
			 strlen("-se="), "-se=%s", servername);
}
