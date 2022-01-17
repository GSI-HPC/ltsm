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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Copyright (c) 2022, GSI Helmholtz Centre for Heavy Ion Research
 */

#include "log.h"

static unsigned int api_msg_level = API_MSG_NORMAL;
typedef void (*api_log_callback_t)(enum api_message_level level, int err,
				   const char *fmt, va_list ap);

inline double time_now(void)
{
	struct timeval tv;

	gettimeofday(&tv, NULL);

	return tv.tv_sec + 0.000001 * tv.tv_usec;
}

int api_msg_get_level(void)
{
	return api_msg_level;
}

void api_msg_set_level(int level)
{
        /* ensure level is in the good range */
        if (level < API_MSG_OFF)
                api_msg_level = API_MSG_OFF;
        else if (level > API_MSG_MAX)
                api_msg_level = API_MSG_MAX;
        else
                api_msg_level = level;
}

static void error_callback_default(enum api_message_level level, int err,
				   const char *fmt, va_list ap)
{
	vfprintf(stderr, fmt, ap);
	if (level & API_MSG_NO_ERRNO || !err)
		fprintf(stderr, "\n");
	else
		fprintf(stderr, ": %s (%d)\n", strerror(err), err);
}

static void info_callback_default(enum api_message_level level, int err,
				  const char *fmt, va_list ap)
{
	UNUSED(level);
	UNUSED(err);
	vfprintf(stdout, fmt, ap);
}

static api_log_callback_t api_error_callback = error_callback_default;
static api_log_callback_t api_info_callback = info_callback_default;

api_log_callback_t api_error_callback_set(api_log_callback_t cb)
{
	api_log_callback_t old = api_error_callback;

	if (cb != NULL)
		api_error_callback = cb;
	else
		api_error_callback = error_callback_default;

	return old;
}

api_log_callback_t api_info_callback_set(api_log_callback_t cb)
{
	api_log_callback_t old = api_info_callback;

	if (cb != NULL)
		api_info_callback = cb;
	else
		api_info_callback = info_callback_default;

	return old;
}

void api_error(enum api_message_level level, int err, const char *fmt, ...)
{
	static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
        va_list args;
        int tmp_errno = errno;

        if ((level & API_MSG_MASK) > api_msg_level)
                return;

	pthread_mutex_lock(&log_mutex);
	va_start(args, fmt);
        api_error_callback(level, abs(err), fmt, args);
        va_end(args);
	errno = tmp_errno;
	pthread_mutex_unlock(&log_mutex);
}
