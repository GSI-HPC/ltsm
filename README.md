# LTSMAPI/LTSMC - Lightweight TSMAPI and TSM Client for Archiving Data

A lightweight high-level TSM API/library (called *ltsmapi*) supporting operations
* *archiving*
* *retrieving*
* *deleting*
* and *querying*.

In addition a lightweight console client (called *ltsmc*) is provided which demonstrates the use of the
high-level TSMAPI calls.

## Getting Started <a id="getting.started"></a>
Before using LTSMAPI/LTSMC a working access to a TSM server is required. One can install for testing purposes a fully working
TSM server (e.g. inside a KVM) for a period of 30 days before the license expires. A complete installation and setup guide is provided
at [TSM Server Installation Guide](http://web-docs.gsi.de/~tstibor/tsm/).

To compile LTSMAPI/LTSMC, first clone the git repository first
```
git clone https://github.com/tstibor/ltsm
```

The building of LTSMAPI/LTSMC is based on simple `Makefile`
```
make
```
Gives result when IBM's header files and library is provided at the proper place:
```
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -O2 -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/qarray.c -I ibmtsm -o obj/src/qarray.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -O2 -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/tsmapi.c -I ibmtsm -o obj/src/tsmapi.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -O2 -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/ltsmc.c -I ibmtsm -o obj/src/ltsmc.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -O2 -DVERSION=\"0.0.1-19-g3f7bbbe\" -o bin/ltsmc   obj/src/qarray.o  obj/src/tsmapi.o  obj/src/ltsmc.o -lApiTSM64 -lm
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -O2 -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/test/test_dsstruct64_off_t.c -I src -I ibmtsm -I cutest -o obj/src/test/test_dsstruct64_off_t.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -O2 -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/test/test_qarray.c -I src -I ibmtsm -I cutest -o obj/src/test/test_qarray.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -O2 -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/test/ltsmc_testsuite.c -I src -I ibmtsm -I cutest -o obj/src/test/ltsmc_testsuite.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -O2 -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/test/CuTest.c -I src -I ibmtsm -I cutest -o obj/src/test/CuTest.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -O2 -DVERSION=\"0.0.1-19-g3f7bbbe\" -o bin/ltsmc_testsuite   obj/src/test/test_dsstruct64_off_t.o  obj/src/test/test_qarray.o  obj/src/test/ltsmc_testsuite.o  obj/src/test/CuTest.o   obj/src/qarray.o  obj/src/tsmapi.o   -lApiTSM64 -lm
```
To compile with DEBUG and VERBOSE flags for verbose output details set `ENV` variables `DEBUG=1 VERBOSE=1`:
```
DEBUG=1 VERBOSE=1 make
```
Gives result:
```
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -DDEBUG -DVERBOSE -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/qarray.c -I ibmtsm -o obj/src/qarray.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -DDEBUG -DVERBOSE -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/tsmapi.c -I ibmtsm -o obj/src/tsmapi.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -DDEBUG -DVERBOSE -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/ltsmc.c -I ibmtsm -o obj/src/ltsmc.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -DDEBUG -DVERBOSE -DVERSION=\"0.0.1-19-g3f7bbbe\" -o bin/ltsmc   obj/src/qarray.o  obj/src/tsmapi.o  obj/src/ltsmc.o -lApiTSM64 -lm
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -DDEBUG -DVERBOSE -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/test/test_dsstruct64_off_t.c -I src -I ibmtsm -I cutest -o obj/src/test/test_dsstruct64_off_t.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -DDEBUG -DVERBOSE -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/test/test_qarray.c -I src -I ibmtsm -I cutest -o obj/src/test/test_qarray.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -DDEBUG -DVERBOSE -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/test/ltsmc_testsuite.c -I src -I ibmtsm -I cutest -o obj/src/test/ltsmc_testsuite.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -DDEBUG -DVERBOSE -DVERSION=\"0.0.1-19-g3f7bbbe\" -c src/test/CuTest.c -I src -I ibmtsm -I cutest -o obj/src/test/CuTest.o
gcc -m64 -DLINUX_CLIENT -std=c99 -Wall -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE -g -DDEBUG -DVERBOSE -DVERSION=\"0.0.1-19-g3f7bbbe\" -o bin/ltsmc_testsuite   obj/src/test/test_dsstruct64_off_t.o  obj/src/test/test_qarray.o  obj/src/test/ltsmc_testsuite.o  obj/src/test/CuTest.o   obj/src/qarray.o  obj/src/tsmapi.o   -lApiTSM64 -lm
```

### Executable and Testing
After successful compiling you will find two binaries: `bin/ltsmc_testsuite` and `bin/ltsmc`. The former is a simple test suite, the latter the console client:
```
syntax: bin/ltsmc
	-a, --archive
	-r, --retrieve
	-q, --query
	-d, --delete
	-f, --fsname <STRING>
	-h, --hl <STRING>
	-l, --ll <STRING>
	-c, --description <STRING>
	-n, --node <STRING>
	-u, --username <STRING>
	-p, --password <STRING>
version: 0.0.1-19-g3f7bbbe, written by Thomas Stibor <t.stibor@gsi.de>
```

## Examples
Please follow first the installation and setup steps in [Section Getting Started](#getting.started)

## Core TSM Operations
### Archiving data
For demonstrating of how to archive files and directories we first create an *archive* directories and place there some data.
```
mkdir -p /tmp/archives && wget --directory-prefix /tmp/archives https://www.kernel.org/pub/linux/kernel/Historic/linux-0.01.tar.gz && tar xvfz /tmp/archives/linux-0.01.tar.gz -C /tmp/archives && rm -rf /tmp/archives/linux-0.01.tar.gz
```

First we archive the single file `/tmp/archives/linux/Makefile`
```
bin/ltsmc --archive --fsname '/' -c 'Historic Linux Kernel Makefile' --node lxdv81 --password lxdv81 /tmp/archives/linux/Makefile
```

For archiving the complete `/tmp/archives/linux` directory (with all sub-directories) perform following command:
```
bin/ltsmc --archive --fsname '/' -c 'Complete Historic Linux Kernel' --node lxdv81 --password lxdv81 /tmp/archives/linux
```
### Querying Data
For querying all data stored in directory `/tmp/archives/linux`
```
bin/ltsmc --query --fsname '/' --node lxdv81 --password lxdv81 --hl '/tmp/archives/linux/*' --ll '/*'
```
Querying the single file `/tmp/archives/linux/Makefile`
```
bin/ltsmc --query --fsname '/' --node lxdv81 --password lxdv81 --hl '/tmp/archives/linux' --ll '/Makefile'
```
### Retrieving Data
We first delete all data in `/tmp/archives/linux` and the restore the data to that directory:
```
rm -rf /tmp/archives/linux && bin/ltsmc --retrieve --fsname '/' --node lxdv81 --password lxdv81 --hl '/tmp/archives/linux/*' --ll '/*'
```
### Deleting Data
To delete data on the TSM perform the following command:
```
bin/ltsmc --delete --fsname '/' --node lxdv81 --password lxdv81 --hl '/tmp/archives/linux/*' --ll '/*'
```
A subsequent *delete* or *query* command shows that there is no data left on the TSM side.
```
bin/ltsmc --delete --fsname '/' --node lxdv81 --password lxdv81 --hl '/tmp/archives/linux/*' --ll '/*'
```

### Screenshot and Screencast
Klick on the screenshot to see the full screencast
<a href="http://web-docs.gsi.de/~tstibor/tsm/ltsm-screencast-1.mp4" rel="screencast">![screencast](http://web-docs.gsi.de/~tstibor/tsm/ltsm-screenshot.png)</a>

## TODOs
* Integrate LTSMAPI into the Lustre HSM framework.
* Implement multiple objects per transaction, currently a single object is processed per transaction.
* Extend LTSMAPI with new compression stream algorithm such as LZ4 (perform a comparison to TSM built-in compression).
 
## Issues

## Tweaks and Programming Tricks

## References
A thorough description and code examples of IBM's low-level TSM API/library can be found in the open document [Using the Application Programming Interface](http://web-docs.gsi.de/~tstibor/tsm/doc/using_the_programming_application_interface.pdf), 2007.

## License
LTSMAPI/LTSMC itself is licensed under the [GPL-2 license](http://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html). The code however, depends on IBM's low-level TSM API/library which are distributed 
under different licenses. See [IBM Tivoli Storage Manager Server](ftp://ftp.software.ibm.com/storage/tivoli-storage-management/maintenance/server/v7r1/Linux/7.1.5.000/README.htm).
