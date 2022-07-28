AUTOMAKE_OPTIONS = subdir-objects

noinst_HEADERS = CuTest.h test_utils.h

if HAVE_TSM
    test_cds_CFLAGS = -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib -I@TSM_SRC_DIR@/ -I@LUSTRE_SRC_DIR@/lustre/include -I@LUSTRE_SRC_DIR@/lustre/include/uapi
    bin_PROGRAMS = test_cds
    test_cds_SOURCES = test_cds.c CuTest.c test_dsstruct64_off64_t.c test_list.c test_chashtable.c test_qtable.c test_utils.c
    test_cds_LDADD = $(top_srcdir)/src/lib/libltsmapi.la

    test_ltsmapi_CFLAGS = -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib -I@TSM_SRC_DIR@/ -I@LUSTRE_SRC_DIR@/lustre/include -I@LUSTRE_SRC_DIR@/lustre/include/uapi
    bin_PROGRAMS += test_ltsmapi
    test_ltsmapi_SOURCES = test_ltsmapi.c CuTest.c test_utils.c
    test_ltsmapi_LDADD = $(top_srcdir)/src/lib/libltsmapi.la

    ltsmbench_CFLAGS = -m64 -DLINUX_CLIENT -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -I$(top_srcdir)/src/lib -I@TSM_SRC_DIR@/
    bin_PROGRAMS += ltsmbench
    ltsmbench_SOURCES = ltsmbench.c test_utils.c
    ltsmbench_LDADD = $(top_srcdir)/src/lib/libltsmapi.la
endif
