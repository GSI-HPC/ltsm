libfsdapi_a_CFLAGS = -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib
libfsdapi_a_SOURCES = fsdapi.c common.c log.c

libtsmapi_a_SOURCES = xattr.c
libtsmapi_a_CFLAGS = -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib

noinst_LIBRARIES = libfsdapi.a libtsmapi.a
noinst_HEADERS = tsmapi.h fsdapi.h xattr.h common.h log.h list.h queue.h chashtable.h qtable.h measurement.h

if HAVE_TSM
    libtsmapi_a_CFLAGS += -I@TSM_SRC_DIR@/
    libtsmapi_a_SOURCES += tsmapi.c common.c log.c list.c queue.c chashtable.c qtable.c
endif

if HAVE_LUSTRE
    libtsmapi_a_CFLAGS += -I@LUSTRE_SRC_DIR@/lustre/include -I@LUSTRE_SRC_DIR@/lustre/include/uapi
endif
