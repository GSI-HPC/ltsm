LTSM - Lightweight TSM API, Lustre TSM Copytool for Archiving and Retrieving Data.
==============

[![Tag Version](https://img.shields.io/github/tag/tstibor/ltsm.svg)](https://github.com/tstibor/ltsm/tags)
[![License](http://img.shields.io/:license-gpl2-blue.svg)](http://www.gnu.org/licenses/gpl-2.0.html)
[![Linux](https://github.com/tstibor/ltsm/actions/workflows/linux.yml/badge.svg)](https://github.com/tstibor/ltsm/actions)

This project consists of *four* parts:
1. Lightweight TSM API/library (called *tsmapi*) supporting operations (*archiving*, *retrieving*, *deleting*, *querying*).
2. Lustre TSM Copytool.
3. TSM console client.
4. Benchmark and test suite.

# Introduction into Lustre HSM
Lustre has since version 2.5 hierarchical storage management (HSM) capabilities, that is, data can be automatically
*archived* to low-cost storage media such as tape storage systems and seamlessly *retrieved* when accessing the data on Lustre clients. The Lustre HSM capabilities can be illustrated by means of state diagrams and the related Lustre commands
*lfs hsm_archive*, *lfs hsm_release*, *lfs hsm_restore* as depicted below.
![Statediagram](https://raw.githubusercontent.com/tstibor/ltsm.github.io/master/doc/images/state.diagram.png)
The initial state is a *new file*. Once the file is archived, it
changes into state *archived* and a identical copy exists on the HSM storage. If the archived file is modified, then the state
changes to *dirty*. As a consequence, one has to re-archive the file to reobtain an identical copy on the Lustre side as well
as the HSM storage. If the state changes to *released*, then the bulk data of the file is moved to the HSM storage and only the file skeleton
is kept. If the file is accessed or the *lfs hsm_restore* command is executed, the file is restored and the state is changes back to *archived*.

# Lustre HSM Framework
The aim of the Lustre HSM framework is to provide an interface for seamlessly *archiving* data which is no
longer used, and place them into a long term retrievable archive storage (such as tape drives).
The meta information of the *archived* data such as the file name, user id, etc. is however still
present in the Lustre file system, whereas the bulk data is placed on the retrievable storage.
When the *released* data is accessed, the bulk data is seamlessly copied from the retrievable storage back to the Lustre
file system. In summary, the data is still available regardless of where it is actually stored.
For providing such a functionality, the HSM framework is embedded in the main Lustre components and explained
for the case of *retrieving* data in the Figure below.
![HSMArchitecture](https://raw.githubusercontent.com/tstibor/ltsm.github.io/master/doc/images/hsm.architecture.png)
Triggered actions and data flows for retrieving data inside the Lustre HSM framework.
The participating nodes (depicted as dashed frames) are: Client, MDS, OSS, Copytool, Storage. The Lustre client opens a file which is already archived, thus the bulk data is not available on any OST. The MDT node receives the open call, allocate objects on the OST and notifies the HSM coordinator to copy in data for file indentifier (short FID) of the corresponding file. The copytool receives from the HSM coordinator the event to copyin data of FID and notifies the internal data mover for retrieving the data stored on the storage node and copy it over to a OST. Once the copy process is completed, the data mover notifies the HSM agent with a copydone message and the HSM agent finalizes the process with a HSM copydone message back to the MDT node.

# TSM Overview
Tivoli Storage Manager (now renamed to IBM Spectrum Protect) (TSM) is a client/server product from IBM
employed in heterogeneous distributed environments to *backup* and *archive* data.
The difference between a backup and an archive can be summarized as follows:
* Backup: A copy of the data is stored in the event the original becomes lost or damaged. Typically an incremental (forever) backup strategy is performed.
* Archive: Remove from an on-line system those data no longer in day to day use, and place them into a long term retrievable storage (such as tape drives).

The core capabilities and features of a TSM server are:
* Compression: Compress data stream seamless either on client or server side.
* Deduplication: Eliminating duplicate copies of repeating data.
* Collocation: Store and pack data of a client in few number of tapes as much as possible to reduce the number of media mounts and for minimizing tape drive movements.
* Storage hierarchies: Automatically move data from faster devices to slower devices based on characteristics such as file size or storage capacity.

The last issue is a crucial feature for the Lustre HSM framework, as it enables effectively archiving and retrieving data to fast devices, rather than being tied up to slow but large and cheap devices (see Fig. below).
![fig:storage.hierarchy](https://raw.githubusercontent.com/tstibor/ltsm.github.io/master/doc/images/storage.hierarchy.png)
Storage hierarchy of a TSM server. Fast but limited space storage devices (high cost per GByte) are located at the
top of the pyramid, whereas slower but large space storage devices (low cost per GByte) are located at the bottom.
Fast storage devices act as a *cache* layer, where data in automatically migrated by the TSM server based on access pattern and
space occupation level. More details can be found in [IBM Tivoli Storage Manager Implementation Guide (pp. 257)](https://github.com/tstibor/ltsm.github.io/raw/master/doc/tsm/ibm_tivoli_storage_management_implementation_guide.pdf#page=287).

Denote r<sub>1</sub> the transfer rate to (fast) storage devices s<sub>1</sub> and r<sub>2</sub> the transfer rate to (slow) storage devices s<sub>2</sub>.
Make sure to set the migration threshold t from s<sub>1</sub> to s<sub>2</sub> to a proper value which suits your use case.
Example: Let r<sub>1</sub> = 800MB/sec, r<sub>2</sub> = 200MB/sec, s<sub>1</sub> = 150TB and migration threshold is set to t = 0.6.
If r<sub>1</sub> &#62; r<sub>2</sub>, then after t &#183; s<sub>1</sub> / (r<sub>1</sub> - r<sub>2</sub>) seconds, s<sub>1</sub> is 100% full. For that example it yields
41.66667 hours, when continuously writing into s<sub>1</sub> with rate r<sub>1</sub>.

# Data Organized on TSM Server
The TSM server is an object storage server and is developed for storing and retrieving named objects. Similar to Lustre, the TSM
server has two main storage areas, database and data storage. The database contains the metadata of the objects such as unique
identifiers, names and attributes, whereas the data storage contains the object data. Each object stored on the TSM server has a
name associated with it that is determined by the client. The object name is composed of:
* **fs**: File space name (mount point),
* **hl**: High level name (directory name),
* **ll**: Low level name (file name),

and must be specified to identify the object for operations query, archive, retrieve and delete. In addition to determined TSM
metadata information, user defined metadata can be stored on the TSM server of size up to 255 bytes. The user defined space is
employed to store Lustre information as well as a CRC32 check-sum and a magic id (see Fig. below).
![](https://raw.githubusercontent.com/tstibor/ltsm.github.io/master/doc/images/object.meta.data.png)

# Copytool
As described above, a *copytool* is composed of a HSM proxy and data mover that runs on the same node. Loosely speaking, the
copytool receives archive, retrieve and delete actions from MDT node and triggers data moving operations. In more detail it is a
daemon process that requires a Lustre mount point for accessing Lustre files and processes actions *HSMA_ARCHIVE*, *HSMA_RESTORE*,
*HSMA_REMOVE* and *HSMA_CANCEL* as depicted below.

![](https://raw.githubusercontent.com/tstibor/ltsm.github.io/master/doc/images/source.core.overview.png)

The functions *ct_archive(session)*, *ct_restore(session)* and *ct_remove(session)* implement the core actions for accessing named objects and for transferring data between the Lustre mount point and the TSM storage.

# Implementation Details
For achieving high data throughput by means of parallelism the copytool employs the producer-consumer model. The data
structure of the model is a concurrent queue. The producer is a single thread receiving HSM action items from the MDS’s and
enqueues the items in the queue. There are multiple consumer threads, each having a session opened to the TSM server, which are
dequeueing items and executing the HSM actions (see Fig. below).
![](https://raw.githubusercontent.com/tstibor/ltsm.github.io/master/doc/images/produce.consumer.model.png)

Overview of a single-producer multiple-consumer model for a produce thread (P-Thread) and four consumer threads
(C-Threads). Three C-Threads dequeued HSM action items and are executing the actions. The P-Thread received from the MDS
a new HSM action item and enqueues the item into the concurrent queue. Once the enqueue operation is successful, C-Thread_4
will be notified to dequeue and execute new HSM action.

# Getting Started <a id="getting.started"></a>
Before using LTSM a working access to a TSM server is required. One can install for testing purposes a fully working
TSM server (e.g. inside a KVM) for a period of 30 days before the license expires. A complete installation and setup guide is provided
at [TSM Server Installation Guide](https://github.com/tstibor/ltsm.github.io/tree/master/doc/tutorial).
Download and install the TSM API client provided at [7.1.X.Y-TIV-TSMBAC-LinuxX86_DEB.tar](http://ftp.software.ibm.com/storage/tivoli-storage-management/maintenance/client/v7r1/Linux/LinuxX86_DEB/BA/).
Make sure to install `tivsm-api64.amd64.deb` where the header files `dapitype.h, dsmapips.h, ...` and the low-level library `libApiTSM64.so` are provided.

### Compile *lhsmtool_tsm* and *ltsmc*

Clone the git repository
```
git clone https://github.com/tstibor/ltsm
```
and execute
```
./autogen.sh && ./configure CFLAGS='-g -DDEBUG -O0' --enable-tests
```
If Lustre header files are not found you will get the message
```
...
configure: WARNING: cannot find Lustre header files, use --with-lustre-headers=PATH
configure: WARNING: cannot find Lustre API headers and/or Lustre API library
...
configured ltsm:

build tsmapi library and ltsmc : yes
build lhsmtool_tsm             : no
build test suite               : yes
```
and the console client *ltsmc* is built only. For building also the Lustre Copytool thus make sure the Lustre header files
and the Lustre library `liblustreapi.so` are available and the paths are correctly specified e.g.
```
./autogen.sh && ./configure CFLAGS='-g -DDEBUG -O0' --with-lustre-src=/usr/local/include/lustre LDFLAGS='-L/usr/local/lib' --with-tsm-headers=/opt/tivoli/tsm/client/api/bin64/sample --enable-tests
```

If *required* TSM and *optional* Lustre header files and libraries are found the following executable files are provided:
  * `src/lhsmtool_tsm` (Lustre TSM Copytool)
  * `src/ltsmc` (Console client)
  * `src/test/test_cds` (Test suite for data structures (linked-list, queue, hashtable, etc.))
  * `src/test/test_tsmapi` (Test suite for *tsmapi*)
  * `src/test/ltsmbench` (Benchmark suite for measuring threaded archive/retrieve performance)

### Install or Build DEB/RPM Package

Download and install already built Debian Stretch package [ltsm_0.8.0_amd64.deb](https://github.com/tstibor/ltsm.github.io/tree/master/packages/deb) or CentOS 7.7 package [ltsm-0.8.0-1.x86_64.rpm](https://github.com/tstibor/ltsm.github.io/tree/master/packages/rpm). In addition you can build the rpm package
yourself as follows
```
git clone https://github.com/tstibor/ltsm && cd ltsm && ./autogen.sh && ./configure && make rpms
```
or the deb package
```
git clone https://github.com/tstibor/ltsm && cd ltsm && ./autogen.sh && ./configure && make debs
```
Make sure your Linux distribution supports [systemd](https://freedesktop.org/wiki/Software/systemd/).
For customizing the options of systemd started `lhsmtool_tsm` daemon, edit file `/etc/default/lhsmtool_tsm`.

## Lustre Copytool
Make sure you have enabled the HSM functionality on the MGS/MDS, e.g. `lctl set_param 'mdt.<LNAME>-MDT0000.hsm_control=enabled`
and TSM server is running as well as `dsm.sys` contains the proper entries such as
```
SErvername              polaris-kvm-tsm-server
   Nodename             polaris
   TCPServeraddress     192.168.254.102
```
If the compilation process is successful, then you should see the following information `./src/lhsmtool_tsm --help`
```
usage: ./src/lhsmtool_tsm [options] <lustre_mount_point>
	-a, --archive-id <int> [default: 0]
		archive id number
	-t, --threads <int>
		number of processing threads [default: 1]
	-n, --node <string>
		node name registered on tsm server
	-p, --password <string>
		password of tsm node/owner
	-o, --owner <string>
		owner of tsm node
	-s, --servername <string>
		hostname of tsm server
	-c, --conf <file>
		option conf file
	-v, --verbose {error, warn, message, info, debug} [default: message]
		produce more verbose output
	--abort-on-error
		abort operation on major error
	--daemon
		daemon mode run in background
	--dry-run
		don't run, just show what would be done
	--restore-stripe
		restore stripe information
	--enable-maxmpc
		enable tsm mount point check to infer the maximum number of feasible threads
	-h, --help
		show this help

IBM API library version: 7.1.8.0, IBM API application client version: 7.1.8.0
version: 0.8.0 © 2017 by GSI Helmholtz Centre for Heavy Ion Research
```

Starting the *copytool* with 4 threads and *archive_id=1* works as follows `./src/lhsmtool_tsm --restore-stripe -a 1 -v debug -n polaris -p polaris -s 'polaris-kvm-tsm-server' -t 4 /lustre` where
parameters, *-n = nodename*, *-p = password* and *-s = servername* have to match with those setup on the TSM Server (see [TSM Server Installation Guide](https://github.com/tstibor/ltsm.github.io/tree/master/doc/tutorial)).
```
[D] 1509708481.724705 [15063] lhsmtool_tsm.c:262 using TSM filespace name '/lustre'
[D] 1509708481.743621 [15063] tsmapi.c:739 dsmSetUp: handle: 0 ANS0302I (RC0)    Successfully done.
[M] 1509708481.743642 [15063] lhsmtool_tsm.c:836 tsm_init: session: 1
[D] 1509708481.927847 [15063] tsmapi.c:785 dsmInitEx: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509708481.936992 [15063] tsmapi.c:806 dsmRegisterFS: handle: 1 ANS0242W (RC2062) On dsmRegisterFS the filespace is already registered
[D] 1509708482.241943 [15063] tsmapi.c:1262 [rc=0] extract_hl_ll
fpath: '/lustre/.mount/.test-maxnummp'
fs   : '/lustre'
hl   : '/.mount'
ll   : '/.test-maxnummp'

[I] 1509708482.241976 [15063] tsmapi.c:1020 query structure
fs   : '/lustre'
hl   : '/.mount'
ll   : '/.test-maxnummp'
owner: ''
descr: '*'
[D] 1509708482.242415 [15063] tsmapi.c:1023 dsmBeginQuery: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509708482.276913 [15063] tsmapi.c:1039 dsmGetNextQObj: handle: 1 ANS0258I (RC2200) On dsmGetNextQObj or dsmGetData there is more available data
[D] 1509708482.276953 [15063] tsmapi.c:1039 dsmGetNextQObj: handle: 1 ANS0272I (RC121)  The operation is finished
[D] 1509708482.276963 [15063] tsmapi.c:1233 [rc:0] get_qra: 0
[D] 1509708482.277211 [15063] tsmapi.c:1198 dsmBeginTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509708482.277226 [15063] tsmapi.c:1208 dsmDeleteObj: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509708482.321404 [15063] tsmapi.c:1217 dsmEndTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509708482.321415 [15063] tsmapi.c:1240 [rc:0] tsm_del_obj: 0
[I] 1509708482.321424 [15063] tsmapi.c:705 [delete] object # 0
fs: /lustre, hl: /.mount, ll: /.test-maxnummp
object id (hi,lo)                          : (0,34772)
object info length                         : 48
object info size (hi,lo)                   : (0,1) (1 bytes)
object type                                : DSM_OBJ_DIRECTORY
object magic id                            : 71147
crc32                                      : 0x00000000 (0000000000)
archive description                        : node mountpoint check
owner                                      :
insert date                                : 2017/11/03 12:28:01
expiration date                            : 2018/11/03 12:28:01
restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (2,0,11491,0,0)
estimated size (hi,lo)                     : (0,1) (1 bytes)
lustre fid                                 : [0:0x0:0x0]
lustre stripe size                         : 0
lustre stripe count                        : 0
...
...
...
[I] 1509708482.321437 [15063] tsmapi.c:2118 passed mount point check
[D] 1509708482.321440 [15063] lhsmtool_tsm.c:871 Abort on error 0
[D] 1509708482.322375 [15063] lhsmtool_tsm.c:645 waiting for message from kernel
```
Note, there is unfortunately no low-level TSM API call to query the [maximum number of mount points](https://www.ibm.com/support/knowledgecenter/en/SSS9C9_2.1.3/com.ibm.ia.doc_1.0/ic/t_coll_ssam_set_max_mount_points.html) (that is parallel sessions).
This number is an upper limit of the number of parallel threads of the copytool. To determine the maximum number of mount points and thus set the number of threads appropriately,
one can apply a trick and send *dummy* transactions to the TSM server until one receives a certain error code. That is the reason why at start you see (in debug mode) the output of *node mountpoint check* when starting the copytool with option *--enable-maxmpc*.

Once the copytool is started one can run the Lustre commands *lfs hsm_archive*, *lfs hsm_release*, *lfs hsm_restore* as depicted in the state diagram.

In the following example we archive the Lustre wiki website:
`wget -O - -o /dev/null http://wiki.lustre.org > /lustre/wiki.lustre.org && sudo lfs hsm_archive /lustre/wiki.lustre.org`.
If the command was successful you should see:
```
[M] 1509710509.205682 [16973] lhsmtool_tsm.c:680 copytool fs=ldomov archive#=1 item_count=1
[M] 1509710509.205705 [16973] lhsmtool_tsm.c:735 enqueue action 'ARCHIVE' cookie=0x59f9b6f9, FID=[0x200000401:0x17:0x0]
[D] 1509710509.205717 [16973] lhsmtool_tsm.c:645 waiting for message from kernel
[D] 1509710509.205803 [16976] lhsmtool_tsm.c:596 dequeue action 'ARCHIVE' cookie=0x59f9b6f9, FID=[0x200000401:0x17:0x0]
[M] 1509710509.205823 [16976] lhsmtool_tsm.c:532 '[0x200000401:0x17:0x0]' action ARCHIVE reclen 72, cookie=0x59f9b6f9
[D] 1509710509.207877 [16976] lhsmtool_tsm.c:361 [rc=0] ct_hsm_action_begin on '/lustre/wiki.lustre.org'
[M] 1509710509.207901 [16976] lhsmtool_tsm.c:366 archiving '/lustre/wiki.lustre.org' to TSM storage
[D] 1509710509.208148 [16976] lhsmtool_tsm.c:376 [fd=10] llapi_hsm_action_get_fd()
[D] 1509710509.208196 [16976] lhsmtool_tsm.c:389 [rc=0,fd=10] xattr_get_lov '/lustre/wiki.lustre.org'
[I] 1509710509.208201 [16976] tsmapi.c:1964 tsm_archive_fpath:
fs: /lustre, fpath: /lustre/wiki.lustre.org, desc: (null), fd: 10, *lustre_info: 0x7fd54c7bad70
[D] 1509710509.208534 [16976] tsmapi.c:1792 [rc:0] extract_hl_ll:
fpath: /lustre/wiki.lustre.org
fs   : /lustre
hl   : /
ll   : /wiki.lustre.org

[D] 1509710509.208938 [16976] tsmapi.c:1547 dsmBeginTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509710509.208958 [16976] tsmapi.c:1555 dsmBindMC: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509710509.208993 [16976] tsmapi.c:1576 dsmSendObj: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509710509.209499 [16976] tsmapi.c:1608 dsmSendData: handle: 1 ANS0302I (RC0)    Successfully done.
[I] 1509710509.209510 [16976] tsmapi.c:1615 cur_read: 20313, total_read: 20313, total_size: 20313
[D] 1509710509.209747 [16976] tsmapi.c:1663 dsmEndSendObj: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509710509.394623 [16976] tsmapi.c:1674 dsmEndTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[I] 1509710509.394646 [16976] tsmapi.c:1702
*** successfully archived: DSM_OBJ_FILE /lustre/wiki.lustre.org of size: 20313 bytes with settings ***
fs: /lustre
hl: /
ll: /wiki.lustre.org
desc:

[D] 1509710509.440357 [16976] tsmapi.c:1707 [rc:0] tsm_obj_update_crc32, crc32: 0x7d0db330 (2098049840)
[M] 1509710509.440370 [16976] lhsmtool_tsm.c:402 archiving '/lustre/wiki.lustre.org' to TSM storage done
[M] 1509710509.440681 [16976] lhsmtool_tsm.c:325 action completed, notifying coordinator cookie=0x59f9b6f9, FID=[0x200000401:0x17:0x0], err=0
[D] 1509710509.441332 [16976] lhsmtool_tsm.c:338 [rc=0] llapi_hsm_action_end on '/lustre/wiki.lustre.org' ok
```

Let us use our *ltsmc* to query (for fun) some TSM object information of */lustre/wiki.lustre.org*.
```
>src/ltsmc -f /lustre --query -v debug -n polaris -p polaris -s 'polaris-kvm-tsm-server' /lustre/wiki.lustre.org
...
...
[D] 1509712134.421465 [18010] tsmapi.c:1303 [rc:0] extract_hl_ll:
fpath: /lustre/wiki.lustre.org
fs   : /lustre
hl   : /
ll   : /wiki.lustre.org

[I] 1509712134.421487 [18010] tsmapi.c:1020 query structure
fs   : '/lustre'
hl   : '/'
ll   : '/wiki.lustre.org'
owner: ''
descr: '*'
[D] 1509712134.421833 [18010] tsmapi.c:1023 dsmBeginQuery: handle: 1 ANS0302I (RC0)    Successfully done.
[D] 1509712134.455811 [18010] tsmapi.c:1039 dsmGetNextQObj: handle: 1 ANS0258I (RC2200) On dsmGetNextQObj or dsmGetData there is more available data
[D] 1509712134.455866 [18010] tsmapi.c:1039 dsmGetNextQObj: handle: 1 ANS0258I (RC2200) On dsmGetNextQObj or dsmGetData there is more available data
[D] 1509712134.455927 [18010] tsmapi.c:1039 dsmGetNextQObj: handle: 1 ANS0272I (RC121)  The operation is finished
[I] 1509712134.455945 [18010] tsmapi.c:705 [query] object # 0
fs: /lustre, hl: /, ll: /wiki.lustre.org
object id (hi,lo)                          : (0,34778)
object info length                         : 48
object info size (hi,lo)                   : (0,20313) (20313 bytes)
object type                                : DSM_OBJ_FILE
object magic id                            : 71147
crc32                                      : 0x7d0db330 (2098049840)
archive description                        :
owner                                      :
insert date                                : 2017/11/03 13:01:49
expiration date                            : 2018/11/03 13:01:49
restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (2,0,11497,0,0)
estimated size (hi,lo)                     : (0,20313) (20313 bytes)
lustre fid                                 : [0x200000401:0x17:0x0]
lustre stripe size                         : 1048576
lustre stripe count                        : 1

```

# Tips for Tuning
There are basically 3 knobs for adjusting the archive/retrieve performance.
1. The [Txnbytelimit](http://www.ibm.com/support/knowledgecenter/en/SSGSG7_7.1.6/client/r_opt_txnbytelimit.html) option for adjusting the number of bytes the *tsmapi* buffers before it sends a transaction to the TSM server. The option depends on the workload, a value of *2GByte* resulted in good performance on most tested machines and setups.
2. The buffer length [TSM_BUF_LENGTH](github.com/tstibor/ltsm/blob/master/src/lib/tsmapi.h#L55) for sending and receiving TSM
bulk data. Empirical investigations showed that a buffer length of 2^15 - 4 = 32764 resulted in good performance on most tested machines and setups.
3. [Maximum number of TSM mount points](https://www.ibm.com/support/knowledgecenter/en/SSS9C9_2.1.3/com.ibm.ia.doc_1.0/ic/t_coll_ssam_set_max_mount_points.html) (that is parallel threaded sessions) and related [QUEUE_MAX_ITEMS](github.com/tstibor/ltsm/blob/master/src/lhsmtool_tsm.c#L84) setting. As described above, this parameter is crucial for achieving high throughput. By means of *QUEUE_MAX_ITEMS* the maximum number of HSM actions items in the queue is determined. That is, no new HSM action items
will be received until queue length drops below *QUEUE_MAX_ITEMS*. This value is set as *QUEUE_MAX_ITEMS = 2 * # threads*.

For checking the archive/retrieve performance the benchmark tool *ltsmbench* can be used
```
>./src/test/ltsmbench --help
usage: ./src/test/ltsmbench [options]
	-z, --size <long> [default: 16777216 bytes]
	-b, --number <int> [default: 16]
	-t, --threads <int> [default: 1]
	-f, --fsname <string> [default: '/']
	-n, --node <string>
	-p, --password <string>
	-s, --servername <string>
	-v, --verbose {error, warn, message, info, debug} [default: message]
	-h, --help

IBM API library version: 7.1.6.0, IBM API application client version: 7.1.6.0
version: 0.5.7-5 © 2017 by GSI Helmholtz Centre for Heavy Ion Research
```

In this example we benchmark the archive/retrieve performance of a TSM server running inside a KVM with 1 thread
```
>./src/test/ltsmbench -v warn -n polaris -p polaris -s polaris-kvm-tsm-server -t 1 -z 134217728 -b 4
[measurement]	'tsm_archive_fpath' processed 536870912 bytes in 9.307 secs (57.682 Mbytes / sec)
[measurement]	'tsm_retrieve_fpath' processed 536870912 bytes in 6.381 secs (84.137 Mbytes / sec)
```
vs 4 threads
```
>./src/test/ltsmbench -v warn -n polaris -p polaris -s polaris-kvm-tsm-server -t 4 -z 134217728 -b 4
[measurement]	'tsm_archive_fpath' processed 536870912 bytes in 8.444 secs (63.578 Mbytes / sec)
[measurement]	'tsm_retrieve_fpath' processed 536870912 bytes in 5.260 secs (102.070 Mbytes / sec)
```

For more TSM server/client tuning tips see [Tips for Tivoli Storage Manager Performance Tuning and Troubleshooting](https://github.com/tstibor/ltsm.github.io/raw/master/doc/tsm/tips.for.tivoli.storage.manager.performance.tuning.and.troubleshooting.pdf)

## Performace Measurement over 10GBit Network
By means of *ltsmbench* the archive and retrieve performance is measured from a ltsm machine to a TSM server. The TSM server is equipped with SSD's bundled into a hardware RAID-5 where the TSM database and transactions logs are located.
The TSM bulk data is placed on regular disks bundled into a hardware RAID-6 device. The archive and retrieve performance for increasing number of threads  {1,2,..,16} is visualized as a [Box-Whisker-Plot](https://en.wikipedia.org/wiki/Box_plot).
![Archive performace](https://raw.githubusercontent.com/tstibor/ltsm.github.io/master/doc/images/perf.meas.arch.png)
![Retrieve performace](https://raw.githubusercontent.com/tstibor/ltsm.github.io/master/doc/images/perf.meas.retr.png)

## Scaling
The Lustre file system can be bond to *multiple* HSM storage backends, where each bond is identified by
an unique number called the *archive id*. This concepts thus allows scaling within the Lustre HSM framework.
In the simplest case, only one copytool instance is started which handles
all archive id's in the range {1,2,...,32}. That is, all data is transferred through a single TSM server though
with multi-threaded access. In the full expanded parallel setup 32 TSM servers can be leveraged for data
transfer as depicted below.
![Archive performace](https://raw.githubusercontent.com/tstibor/ltsm.github.io/master/doc/images/scaling.png)

## Moving and Renaming Files already Archived and Released
Archiving and releasing a file F with the TSM copytool and subsequently moving
F to a different Lustre directory results in a mismatch between
the `readlink -f F` and the TSM object /fs/hl/ll stored on the TSM server.
As a consequence the file cannot be retrieved from the TSM server anymore.
To overcome this problem, each file archived with the TSM copytool stores in the extended attribute
`user.lustre.uuid` an UUID value, e.g. `user.lustre.uuid=0xc72e6182ab0b4e00b6798282dea857fe`.
The UUID value is also stored in the TSM object description field when F is archived.
Additional fields are `/fs/hl/ll` and some more as described above.
A released file F is queried e.g. as follows: `/fs/*/**` and `description=0xc72e6182ab0b4e00b6798282dea857fe`.
If the query is successful, then the data of the TSM object is written into file F.
Otherwise, the query is repeated however as follows: `/fs/hl/ll`, that is with `readlink -f F`.
Note, make sure the Lustre file system is mounted with extended attribute capability, that is `(/lustre type lustre (...,user_xattr,...)`.

## More Information
In the manual pages [lhsmtool_tsm.1](http://github.com/tstibor/ltsm/blob/master/man/lhsmtool_tsm.1) and [ltsmc.1](http://github.com/tstibor/ltsm/blob/master/man/ltsmc.1) usage details and options of *lhsmtool_tsm* and *ltsmc* are provided. In addition, a [screencast](https://github.com/tstibor/ltsm.github.io/raw/master/screencast/ltsm-screencast-2.mp4) of an older version of this project is provided.

## References
A thorough description and code examples of IBM's low-level TSM API/library can be found in the open document [Using the Application Programming Interface](https://github.com/tstibor/ltsm.github.io/raw/master/doc/tsm/using_the_programming_application_interface.pdf), Fourth edition (September 2015).

## Funding
This project is funded by Intel® through GSI's [Intel® Parallel Computing Centers](https://software.intel.com/en-us/ipcc/)

## License
This project itself is licensed under the [GPL-2 license](http://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html). The code however, depends on IBM's low-level TSM API/libraries which are distributed
under different licenses and thus are not provided in the repository. See [IBM Tivoli Storage Manager Server](http://ftp.software.ibm.com/storage/tivoli-storage-management/maintenance/server/v7r1/Linux/7.1.7.000/README.htm).

## Warranty
Note, this project is still under development and in a beta development state. It is however ready to be tested.
