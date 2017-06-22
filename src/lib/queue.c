/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 only,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	See the GNU
 * General Public License version 2 for more details (a copy is included
 * in the LICENSE file that accompanied this code).
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Copyright (c) 2017, GSI Helmholtz Centre for Heavy Ion Research
 */

/*
 * This code is based on the book: Mastering Algorithms with C,
 *				   Kyle London, First Edition 1999.
 */

#include <stdlib.h>
#include "list.h"
#include "queue.h"

int queue_enqueue(queue_t *queue, const void *data)
{
	return list_ins_next(queue, list_tail(queue), data);
}

int queue_dequeue(queue_t *queue, void **data)
{
	return list_rem_next(queue, NULL, data);
}
