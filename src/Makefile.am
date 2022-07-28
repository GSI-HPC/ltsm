SUBDIRS = lib

if TESTS
    SUBDIRS += test
endif

AUTOMAKE_OPTIONS = subdir-objects

if HAVE_TSM_LTSM_LUSTRE
    fsqd_CFLAGS = -I@LUSTRE_SRC_DIR@/lustre/include -I@LUSTRE_SRC_DIR@/lustre/include/uapi -I@TSM_SRC_DIR@/ -I@LTSM_SRC_DIR@/ -I$(top_srcdir)/src/lib
    sbin_PROGRAMS = fsqd
    fsqd_SOURCES = fsqd.c
    fsqd_LDADD = $(top_srcdir)/src/lib/libfsqapi.a
endif

fsqc_CFLAGS = -I$(top_srcdir)/src/lib
bin_PROGRAMS = fsqc
fsqc_SOURCES = fsqc.c
fsqc_LDADD = $(top_srcdir)/src/lib/libfsqapi.a
