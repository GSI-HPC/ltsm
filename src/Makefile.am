SUBDIRS = lib

if TESTS
    SUBDIRS += test
endif

AUTOMAKE_OPTIONS = subdir-objects

ltsmc_CFLAGS =

if HAVE_TSM
    ltsmc_CFLAGS += -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib -I@TSM_SRC_DIR@/
    bin_PROGRAMS = ltsmc
    ltsmc_SOURCES = ltsmc.c
    ltsmc_LDADD = $(top_srcdir)/src/lib/libtsmapi.a
endif

if HAVE_TSM_LUSTRE
    ltsmc_CFLAGS += -I@LUSTRE_SRC_DIR@/lustre/include
    lhsmtool_tsm_CFLAGS = -I@LUSTRE_SRC_DIR@/lustre/include -I@LUSTRE_SRC_DIR@/lustre/include/uapi -I@TSM_SRC_DIR@/ -I$(top_srcdir)/src/lib
    sbin_PROGRAMS = lhsmtool_tsm
    lhsmtool_tsm_SOURCES = lhsmtool_tsm.c
    lhsmtool_tsm_LDADD = $(top_srcdir)/src/lib/libtsmapi.a

    ltsmfsd_CFLAGS = -I@LUSTRE_SRC_DIR@/lustre/include -I@LUSTRE_SRC_DIR@/lustre/include/uapi -I@TSM_SRC_DIR@/ -I$(top_srcdir)/src/lib
    sbin_PROGRAMS += ltsmfsd
    ltsmfsd_SOURCES = ltsmfsd.c $(top_srcdir)/src/lib/xattr.c
    ltsmfsd_LDADD = $(top_srcdir)/src/lib/libtsmapi.a $(top_srcdir)/src/lib/libfsdapi.a
endif
