libtsmapi_a_CFLAGS = -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib -I@TSM_SRC_DIR@/
libfsdapi_a_CFLAGS = -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib

if HAVE_LUSTRE
    libtsmapi_a_CFLAGS += -I@LUSTRE_SRC_DIR@/lustre/include -I@LUSTRE_SRC_DIR@/lustre/include/uapi
endif

noinst_LIBRARIES = libtsmapi.a libfsdapi.a
noinst_HEADERS = tsmapi.h fsdapi.h common.h log.h list.h queue.h chashtable.h qtable.h measurement.h
libtsmapi_a_SOURCES = tsmapi.c common.c log.c list.c queue.c chashtable.c qtable.c
libfsdapi_a_SOURCES = fsdapi.c common.c log.c
