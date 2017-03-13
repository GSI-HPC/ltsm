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
 * Copyright (c) 2017, Thomas Stibor <t.stibor@gsi.de>
 */

#ifndef QTABLE_H
#define QTABLE_H

#include "tsmapi.h"

dsInt16_t init_qtable(struct qtable_t *qtable);
void destroy_qtable(struct qtable_t *qtable);
dsInt16_t insert_qtable(struct qtable_t *qtable,
		  const qryRespArchiveData *qra_data);
dsInt16_t create_sarray(struct qtable_t *qtable);
void free_sarray(struct qtable_t *qtable);
dsInt16_t get_sarray(const struct qtable_t *qtable,
		     qryRespArchiveData *qra_data, const uint32_t n);

int cmp_restore_order_new(const void *a, const void *b);
#endif /* QTABLE_H */
