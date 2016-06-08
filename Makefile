# --------------- Directory paths for src, src/test, obj and bin.
SRCDIR          = src
SRCTESTDIR      = $(SRCDIR)/test
OBJDIR          = obj
BINDIR          = bin
TSM_CL_API_INCS = /opt/tivoli/tsm/client/api/bin64/sample
# --------------- Name and location of executable files.
EXE_LTSM_NAME   = ltsmc
EXE_TEST_NAME   = ltsmc_testsuite
EXE_LTSM        = $(BINDIR)/$(EXE_LTSM_NAME)
EXE_LTSM_TEST   = $(BINDIR)/$(EXE_TEST_NAME)
# --------------- Location of c and object files.
SRC             = $(wildcard $(SRCDIR)/*.c)
SRCTEST         = $(wildcard $(SRCTESTDIR)/*.c)
OBJ             = $(patsubst %.c, $(OBJDIR)/%.o, $(SRC))
_OBJ            = $(subst $(OBJDIR)/$(SRCDIR)/$(EXE_LTSM_NAME).o,, $(OBJ))
OBJTEST         = $(patsubst %.c, $(OBJDIR)/%.o, $(SRCTEST))
# --------------- Compiler settings, include and library paths.
CC              = gcc
DFLAGS          = -D_LARGEFILE64_SOURCE # -D_FILE_OFFSET_BITS=64
CFLAGS          = -m64 -DLINUX_CLIENT -std=c99 -Wall -Wextra -Werror $(DFLAGS)
INCS_IBMTSM     = -I ibmtsm -I $(TSM_CL_API_INCS)
INCS_CUTEST     = -I cutest
INC_TSMAPI      = -I $(SRCDIR)
LIBS_IBMTSM     = -lApiTSM64
LIBS_MISC       = -lm
# --------------- TAG files for emacs navigation.
GTAGS           = GPATH GRTAGS GSYMS GTAGS
# --------------- TSM error log files.
TSM_LOG_FILES   = dsierror.log dsmerror.log

# Compile with debug:~>DEBUG=1 make
ifeq ($(DEBUG),1)
    CFLAGS += -g -DDEBUG
else
    CFLAGS += -O2
endif

# Compile with verbose output:~>VERBOSE=1 make
ifeq ($(VERBOSE),1)
    CFLAGS += -DVERBOSE
endif

# Obtain git version tag information and provide it as definition flag VERSION
GIT_VERSION ?= $(shell git describe)
CFLAGS += -DVERSION=\"$(GIT_VERSION)\"

all: build $(EXE_LTSM) $(EXE_LTSM_TEST)

build:
	@mkdir -p $(BINDIR) $(OBJDIR) $(OBJDIR)/$(SRCDIR) $(OBJDIR)/$(SRCTESTDIR)

clean:
	@rm -rf $(OBJDIR) $(BINDIR) $(GTAGS) $(TSM_LOG_FILES)

tags:
	@gtags .

$(OBJ): $(OBJDIR)/$(SRCDIR)/%.o : $(SRCDIR)/%.c
	$(CC) $(CFLAGS) -c $< $(INCS_IBMTSM) -o $@

$(OBJTEST): $(OBJDIR)/$(SRCTESTDIR)/%.o : $(SRCTESTDIR)/%.c
	$(CC) $(CFLAGS) -c $< $(INC_TSMAPI) $(INCS_IBMTSM) $(INCS_CUTEST) -o $@

$(EXE_LTSM): $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(RUNTIME_PATH) $(OBJ) $(LIBS_IBMTSM) $(LIBS_MISC)

$(EXE_LTSM_TEST):  $(OBJTEST)
	$(CC) $(CFLAGS) -o $@ $(RUNTIME_PATH) $(OBJTEST) $(_OBJ) $(LIBS_IBMTSM) $(LIBS_MISC)

backup:
	/bin/tar jcf - . | /usr/bin/ssh tstibor@lx-pool.gsi.de "/bin/cat > /u/tstibor/backups/backup_tsm_stibor_`/bin/date +"%u"`.tar.bz2"
