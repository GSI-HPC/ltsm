#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])

# Obtain git version.
m4_define([GIT_VERSION_STRING], m4_esyscmd([git describe --always | tr -d '\n']))

# Define package information.
AC_INIT([ltsm], [GIT_VERSION_STRING], [t.stibor@gsi.de], [], [https://github.com/tstibor/ltsm])

m4_include([m4/ax_pthread.m4])
m4_include([m4/ax_check_zlib.m4])
AC_CONFIG_SRCDIR([src/ltsmc.c])
AM_INIT_AUTOMAKE

AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_AWK
AC_PROG_CC
AC_PROG_CC_STDC
AC_PROG_MAKE_SET
AC_PROG_CXX
AC_PROG_RANLIB

# Use the C language and compiler for the following checks.
AC_LANG([C])

LT_INIT
AC_CONFIG_MACRO_DIR([m4])

# Path to Lustre src directory.
AC_ARG_WITH([lustre], AS_HELP_STRING([--with-lustre[=PATH]],[path to location for Lustre sources]),
	LUSTRE_SRC_DIR="$withval", LUSTRE_SRC_DIR="lustre-release")

AS_IF([test -x "$with_lustre"],
	[
	# Check for Lustre header file
	AC_CHECK_FILE("$LUSTRE_SRC_DIR/lustre/include/lustre/lustre_user.h",
		[],
		[AC_MSG_ERROR("cannot find Lustre source tree. Use --with-lustre[=PATH]")]
	)
	# Check for Lustre library
	AC_CHECK_LIB([lustreapi],
		 [llapi_error],
		 [],
		 [AC_MSG_ERROR([cannot find Lustre API library lustreapi, fix it by providing library path e.g. ./configure LDFLAGS='-L/<PATH_TO_LIB>'])]
        )
	AC_DEFINE([HAVE_LUSTRE], [1], [define to 1 if Lustre source is available])
	AM_CONDITIONAL([HAVE_LUSTRE], [true])
	],
	[
	AM_CONDITIONAL([HAVE_LUSTRE], [false])
	AC_MSG_WARN("cannot find Lustre source tree and Lustre library. Only TSM console client ltsmc will be build")
	]
)

# Path to TSM API src directory.
AC_ARG_WITH([tsm-headers], AS_HELP_STRING([--with-tsm-headers[=PATH]],[path to location for IBM TSM header files [default=/opt/tivoli/tsm/client/api/bin64/sample]]),
	TSM_SRC_DIR="$withval", TSM_SRC_DIR="/opt/tivoli/tsm/client/api/bin64/sample")
AC_CHECK_FILE("$TSM_SRC_DIR/dsmapitd.h",,
	AC_MSG_ERROR("cannot find proprietary IBM TSM header files. Use --with-tsm-headers=PATH"))

# Checks for other libraries.
AC_CHECK_LIB([m],[ceil])
AC_CHECK_LIB([ApiTSM64], [dsmInitEx], [], [AC_MSG_ERROR([cannot find proprietary IBM library ApiTSM64, provide library path e.g. ./configure LDFLAGS='-L/<PATH_TO_LIB>'])])

# Check for required zlib library.
AX_CHECK_ZLIB(, AC_MSG_ERROR([zlib is required and could not be found!]))

# Check for required xattr library.
AC_CHECK_LIB([attr], [fgetxattr], [], [AC_MSG_ERROR([libattr is required and could not be found!])])

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h stdint.h stdlib.h string.h strings.h sys/param.h sys/time.h unistd.h pthread.h float.h attr/xattr.h])

AX_PTHREAD([AC_DEFINE(HAVE_PTHREAD, 1, [Define if you have POSIX threads libraries and header files.])
            CLIBS="$PTHREAD_LIBS $LIBS"
            CFLAGS="$CFLAGS $PTHREAD_CFLAGS"
            LDFLAGS="$LDFLAGS $PTHREAD_CFLAGS"
            CC="$PTHREAD_CC"],[])
AC_SEARCH_LIBS(pthread_create, pthread)

# Propage flags and dirs among final Makefiles.
AC_SUBST([AM_CFLAGS])
AC_SUBST([LUSTRE_SRC_DIR])
AC_SUBST([TSM_SRC_DIR])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([bzero gettimeofday memmove memset mkdir realpath strerror atexit sqrt])

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 src/lib/Makefile
		 src/test/Makefile])

# Remove unneeded libraries.
LDFLAGS="$LDFLAGS -Wl,--as-needed"

# Halt on warnings.
CFLAGS="$CFLAGS -Wall -Werror"

AC_OUTPUT
