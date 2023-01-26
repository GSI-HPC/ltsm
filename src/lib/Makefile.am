libltsmapi_la_CFLAGS = -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib

noinst_HEADERS = common.h log.h list.h queue.h chashtable.h qtable.h measurement.h ltsmapi.h

if HAVE_TSM
    libltsmapi_la_CFLAGS += -I@TSM_SRC_DIR@/
    libltsmapi_la_SOURCES = ltsmapi.c common.c log.c list.c queue.c chashtable.c qtable.c
endif

if HAVE_LUSTRE
    libltsmapi_la_CFLAGS += -I@LUSTRE_SRC_DIR@/lustre/include -I@LUSTRE_SRC_DIR@/lustre/include/uapi
endif

lib_LTLIBRARIES = libltsmapi.la
libltsmapi_la_LDFLAGS = -version-info 1:0
