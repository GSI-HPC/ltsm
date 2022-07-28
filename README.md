FSQ - File storage queue server, client and library.
==============

[![Tag Version](https://img.shields.io/github/tag/tstibor/fsq.svg)](https://github.com/tstibor/fsq/tags)
[![License](http://img.shields.io/:license-gpl2-blue.svg)](http://www.gnu.org/licenses/gpl-2.0.html)

The goal of the file storage queue (short fsq) is to
efficiently and robustly transfer data to a Lustre file system and
additionally archive the data seamlessly on a TSM server.
A deployed Lustre file system is frequently shared and accessed by thousands of users and
can suffer from latency delays. To overcome this problem,
the daemon implements a straightforward socket communication protocol and employs
(similar to the copytool architecture) a *queue* data-structure and
*multiple producer-consumer* model to leverage asynchronous data transfer by using an intermediate
local file system. The daemon can be started as follows:
```
>fsqd -l /fsqdata -i identmap.conf -s 16 -q 12 -v info /lustre
[I] 1579167563.378896 [174822] fsqd.c:137 node: 'polaris', servername: 'tsmserver-8', archive_id: 15, uid: 1000, gid: 2000
[M] 1579167563.382178 [174822] fsqd.c:1618 listening on port 7625 with 16 socket threads, 12 queue worker threads, local fs '/fsqdata' and number of tolerated file errors 16
[M] 1579167563.382268 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/0
[M] 1579167563.382309 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/1
[M] 1579167563.382353 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/2
[M] 1579167563.382387 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/3
[M] 1579167563.382418 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/4
[M] 1579167563.382448 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/5
[M] 1579167563.382480 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/6
[M] 1579167563.382511 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/7
[M] 1579167563.382544 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/8
[M] 1579167563.382575 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/9
[M] 1579167563.382606 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/10
[M] 1579167563.382641 [174822] fsqd.c:1539 created queue consumer thread fsq_queue/11
```

For interfacing the server from a client, use the function calls provided in: [fsqapi.h](http://github.com/tstibor/fsq/blob/master/src/lib/fsqapi.h)
```
int fsq_init(struct fsq_login_t *fsq_login,
	     const char *node, const char *password, const char *hostname);
int fsq_fconnect(struct fsq_login_t *fsq_login,
		 struct fsq_session_t *fsq_session);
void fsq_fdisconnect(struct fsq_session_t *fsq_session);
int fsq_fopen(const char *fs, const char *fpath, const char *desc,
	      struct fsq_session_t *fsq_session);
int fsq_fdopen(const char *fs, const char *fpath, const char *desc,
	       enum fsq_storage_dest_t fsq_storage_dest,
	       struct fsq_session_t *fsq_session);
ssize_t fsq_fwrite(const void *ptr, size_t size, size_t nmemb,
		   struct fsq_session_t *fsq_session);
int fsq_fclose(struct fsq_session_t *fsq_session);
```

## License
This project itself is licensed under the [GPL-2 license](http://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html).

## Warranty
Note, this project is still under development and in a beta development state. It is however ready to be tested.
