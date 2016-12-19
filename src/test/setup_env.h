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

#ifndef SETUP_ENV_H
#define SETUP_ENV_H

#include "tsmapi.h"

#define ENV_LTSM_NODE	     "LTSM_NODE"
#define ENV_LTSM_OWNER	     "LTSM_OWNER"
#define ENV_LTSM_PASSWORD    "LTSM_PASSWORD"
#define ENV_LTSM_FSPACE_NAME "LTSM_FSPACE_NAME"
#define ENV_LTSM_SERVERNAME  "LTSM_SERVERNAME"

void init_login(login_t *login);

#endif
