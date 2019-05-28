libtsmapi_a_CFLAGS = -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib -I@TSM_SRC_DIR@/

if HAVE_LUSTRE
    libtsmapi_a_CFLAGS += -I@LUSTRE_SRC_DIR@/lustre/include -I@LUSTRE_SRC_DIR@/lustre/include/uapi
endif

noinst_LIBRARIES = libtsmapi.a
noinst_HEADERS = tsmapi.h log.h list.h queue.h chashtable.h qtable.h measurement.h
libtsmapi_a_SOURCES = tsmapi.c log.c list.c queue.c chashtable.c qtable.c
