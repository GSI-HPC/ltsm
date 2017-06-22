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

#ifndef LIST_H
#define LIST_H

#include <stdlib.h>

#define RC_ERROR                 -1
#define RC_SUCCESS                0
#define RC_DATA_FOUND             1
#define RC_DATA_NOT_FOUND         2
#define RC_DATA_ALREADY_INSERTED  3

typedef struct list_node_ {
	void              *data;
	struct list_node_ *next;
} list_node_t;

typedef struct {
	size_t size;
	void (*destroy)(void *data);
	list_node_t *head;
	list_node_t *tail;
} list_t;

#define list_size(list) ((list)->size)
#define list_head(list) ((list)->head)
#define list_tail(list) ((list)->tail)
#define list_is_head(list, node) ((node) == (list)->head ? 1 : 0)
#define list_is_tail(node) ((node)->next == NULL ? 1 : 0)
#define list_data(node) ((node)->data)
#define list_next(node) ((node)->next)

void list_init(list_t *list, void (*destroy)(void *data));
void list_destroy(list_t *list);
int list_ins_next(list_t *list, list_node_t *node, const void *data);
int list_rem_next(list_t *list, list_node_t *node, void **data);
void list_for_each(const list_t *list, void (*callback)(void *data));

#endif
