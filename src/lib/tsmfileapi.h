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
 * Copyright (c) 2016, 2017 Thomas Stibor <t.stibor@gsi.de>
 */

/* Important note: The API can only retrieve objects that were
   archived using TSM API calls, that is, data archived with dsmc cannot
   be retrieved with these TSM API calls or console client ltsmc.
   Moreover the API doesn't support subdir opperations, that is, hl/ll
   queries must be constructed (in a clever way) with wildcard (*) and
   question mark (?) to match sub directories and files. For more detail c.f.
   PDF Dokument: Using the Application Programming Interface
   (http://www.ibm.com/support/knowledgecenter/SSGSG7_7.1.3/api/b_api_using.pdf)
*/

#ifndef TSMFILEAPI_H
#define TSMFILEAPI_H

#include <sys/types.h>
#include "dsmapitd.h"
#include "dsmapifp.h"
#include "dsmapips.h"
#include "dsmrc.h"
#include "dapint64.h"
#include "tsmapi.h"


#define TSM_FILE_MODE_WRITE 1
#define TSM_FILE_MODE_READ 2


struct tsm_filehandle_t
{
	int mode; //mode in which the file is opened
	struct login_t *login; //login information of the connection
	struct session_t session; //hols the dsm session (and fs name)
	struct archive_info_t archive_info; //holds hl/ll name parts
	mcBindKey mc_bind_key; //used in file open write to bind mc
	DataBlk data_blk; //used for read and write operations as buffer
	ObjAttr obj_attr; //used in write as handle
	char o_desc[DSM_MAX_DESCR_LENGTH + 1];
};

int tsm_file_open(struct tsm_filehandle_t *fh, struct login_t* login,
	char* path, char* desc, int mode);

int tsm_file_write(struct tsm_filehandle_t *fh, void* data_ptr,
	size_t datasize, size_t elements);

int tsm_file_read(struct tsm_filehandle_t *fh, void* data_ptr,
	size_t datasize, size_t elements, size_t *read);

int tsm_file_close(struct tsm_filehandle_t *fh);

#endif /* TSMFILEAPI_H */
