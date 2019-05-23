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
#include <string.h>
#include "list.h"

void list_init(list_t *list, void (*destroy)(void *data))
{
	list->size = 0;
	list->destroy = destroy;
	list->head = NULL;
	list->tail = NULL;
}

void list_destroy(list_t *list)
{
	void *data;

	while (list_size(list) > 0) {
		if (list_rem_next(list, NULL, (void **)&data) == 0 &&
		    list->destroy != NULL)
			list->destroy(data);

	}
	memset(list, 0, sizeof(list_t));
}

int list_ins_next(list_t *list, list_node_t *node, const void *data)
{
	list_node_t *new_node;

	new_node = malloc(sizeof(list_node_t));
	if (new_node == NULL)
		return RC_ERROR;

	new_node->data = (void *)data;
	/* Insertation at the head of the list. */
	if (node == NULL) {
		if (list_size(list) == 0)
			list->tail = new_node;

		new_node->next = list->head;
		list->head = new_node;
	} else {
		/* Insertion somewhere other than at the head. */
		if (node->next == NULL)
			list->tail = new_node;
		new_node->next = node->next;
		node->next = new_node;
	}
	list->size++;

	return RC_SUCCESS;
}

int list_rem_next(list_t *list, list_node_t *node, void **data)
{
	list_node_t *old_node;

	/* No removal from an empty list. */
	if (list_size(list) == 0)
		return RC_ERROR;

	/* Removal from the head of list. */
	if (node == NULL) {
		*data = list->head->data;
		old_node = list->head;
		list->head = list->head->next;
		if (list_size(list) == 1)
			list->tail = NULL;
	} else {
		/* Removal from somewhere other than the head. */
		if (node->next == NULL)
			return RC_ERROR;
		*data = node->next->data;
		old_node = node->next;
		node->next = node->next->next;
		if (node->next == NULL)
			list->tail = node;
	}
	free(old_node);
	list->size--;

	return RC_SUCCESS;
}

void list_for_each(const list_t *list, void (*callback)(void *data))
{
	list_node_t *node = list_head(list);
	while (node) {
		callback(list_data(node));
		node = list_next(node);
	}
}
