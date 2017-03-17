LTSM - Lightweight TSM API, TSM Console Client and Lustre TSM Copytool for Archiving Data
==============

[![Build Status](https://travis-ci.org/tstibor/ltsm.svg?branch=master)](https://travis-ci.org/tstibor/ltsm)
[![Tag Version](https://img.shields.io/github/tag/tstibor/ltsm.svg)](https://github.com/tstibor/ltsm/tags)
[![License](http://img.shields.io/:license-gpl2-blue.svg)](http://www.gnu.org/licenses/gpl-2.0.html)

A lightweight high-level TSM API/library (called *tsmapi*) supporting operations
* *archiving*
* *retrieving*
* *deleting*
* and *querying*.

Moreover, a Lustre TSM Copytool (called *lhsmtool_tsm*) is provided which hooks into the Lustre framework and provides HSM functionality for Lustre and TSM storage backend.
In addition a simple console client (called *ltsmc*) is provided which demonstrates the use of *tsmapi* and 
can be used to retrieve data which was archived with the Lustre Copytool. This is especially useful when a Lustre storage deployment is decommissioned
and the archived data still needs to be retrieved afterwards.

## Getting Started <a id="getting.started"></a>
Before using LTSM a working access to a TSM server is required. One can install for testing purposes a fully working
TSM server (e.g. inside a KVM) for a period of 30 days before the license expires. A complete installation and setup guide is provided
at [TSM Server Installation Guide](http://web-docs.gsi.de/~tstibor/tsm/).
Download and install the TSM API client provided at [7.1.X.Y-TIV-TSMBAC-LinuxX86_DEB.tar](http://ftp.software.ibm.com/storage/tivoli-storage-management/maintenance/client/v7r1/Linux/LinuxX86_DEB/BA/).
Make sure to install `tivsm-api64.amd64.deb` where the header files `dapitype.h, dsmapips.h, ...` and the low-level library `libApiTSM64.so` are provided.

### Compile *ltsmc* and *lhsmtool_tsm*

Clone the git repository
```
git clone https://github.com/tstibor/ltsm
```
and execute
```
./autogen.sh && ./configure CFLAGS='-g -DDEBUG -O0' --with-lustre=<PATH-TO-LUSTRE-RELEASE> LDFLAGS='-L<PATH-TO-LUSTRE-RELEASE>/lustre/utils'
```
If Lustre in `<PATH-TO-LUSTRE-RELEASE>` is not found you will get the message
```
...
configure: WARNING: "cannot find Lustre source tree and Lustre library. Only TSM console client ltsmc will be build"
...

```
and the console client *ltsmc* is built only. For building also the Lustre Copytool thus make sure the Lustre sources (header files) are available
as well as the Lustre library `liblustreapi.so`. If *required* TSM and *optional* Lustre header files and libraries are found the following executable files are provided
  * `src/test/test_tsmapi` (Simple test suite)
  * `src/lhsmtool_tsm` (Lustre TSM Copytool)
  * `src/ltsmc` (Console client)

## Lustre Copytool
Make sure to have enabled the HSM functionality on the MGS/MDS, e.g. `lctl set_param 'mdt.<LNAME>-MDT0000.hsm_control=enabled`
and TSM server is running as well as `dsm.sys` contains the proper entries such as
```
SErvername              polaris-kvm-tsm-server
   Nodename             polaris
   TCPServeraddress     192.168.254.102
```
Start the Lustre TSM Copytool as follows: `# lhsmtool_tsm -n polaris -p polaris -s polaris-kvm-tsm-server -u polaris '/lustre'`
where the parameters, *-n = nodename*, *-p = password* have to match with those setup on the TSM Server (see [TSM Server Installation Guide](http://web-docs.gsi.de/~tstibor/tsm/))
and *-s = servername* with the entry in *dsm.sys*. If everything is correctly setup you should see the following information:
```
DSMI_CONFIG="/opt/tivoli/tsm/client/api/bin64/dsm.sys" ./src/lhsmtool_tsm -n polaris -p polaris -s 'polaris-kvm-tsm-server' -t 1 -v debug /lustre 
[DEBUG] 1488451355.713236 [26390] tsmapi.c:466 dsmSetUp: handle: 0 ANS0302I (RC0)    Successfully done.
[MESSAGE] 1488451355.713294 [26390] lhsmtool_tsm.c:741 tsm_init: session: 1
[DEBUG] 1488451355.817125 [26390] tsmapi.c:511 dsmInitEx: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488451355.820543 [26390] tsmapi.c:530 dsmRegisterFS: handle: 1 ANS0242W (RC2062) On dsmRegisterFS the filespace is already registered
[DEBUG] 1488451355.820591 [26390] tsmapi.c:585 dsmQuerySessOptions: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488451355.820607 [26390] tsmapi.c:604 
DSMI_DIR 	: /opt/tivoli/tsm/client/api/bin64
DSMI_CONFIG 	: /opt/tivoli/tsm/client/api/bin64/dsm.sys
serverName 	: POLARIS-KVM-TSM-SERVER
commMethod 	: 1
serverAddress 	: 192.168.254.101
nodeName 	: POLARIS
compress 	: 0
compressalways 	: 1
passwordAccess 	: 0
[DEBUG] 1488451355.820762 [26390] tsmapi.c:610 dsmQuerySessInfo: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488451355.820775 [26390] tsmapi.c:621 
Server's ver.rel.lev       : 7.1.3.0
ArchiveRetentionProtection : No

[INFO] 1488451355.820806 [26390] tsmapi.c:628 
Max number of multiple objects per transaction: 4096
Max number of Bytes per transaction: 26214400
dsmSessInfo.fsdelim: /
dsmSessInfo.hldelim: /

[INFO] 1488451355.820844 [26390] tsmapi.c:651 IBM API library version = 7.1.3.0

[MESSAGE] 1488451355.821993 [26390] lhsmtool_tsm.c:620 waiting for message from kernel
```
The Lustre TSM Copytool is now ready to process HSM action items in a single thread.
Processing HSM action items is implemented as a *single producer - multiple consumer* model.
A single producer inserts HSM action items into a queue, the multiple
consumers dequeue the items and execute the corresponding TSM actions such as *archive*, *retrieve*
and *delete*. The number of consumer threads is specified by the option `--threads <NUM>`, default is 1. Note,
it is necessary to set the same or even greater value on the TSM server with command:
`update node <NODENAME> maxnummp=<NUM>`, otherwise the TSM server will reply with
error message: `"This node has exceeded its maximum number of mount points"` and will fail
in processing the HSM action items.

## Console Client
In the following sections you find a quick overview of the functionality of *ltsmc*.
### Archiving data
For demonstrating of how to archive files and directories we first create an *archive* directories and place there some data.
```
mkdir -p /tmp/archives && wget --directory-prefix /tmp/archives https://www.kernel.org/pub/linux/kernel/Historic/linux-0.01.tar.gz && ta
r xvfz /tmp/archives/linux-0.01.tar.gz -C /tmp/archives && rm -rf /tmp/archives/linux-0.01.tar.gz
```

First we archive the single file `/tmp/archives/linux/Makefile`
```
./src/ltsmc -v debug --archive --fsname '/' -d 'Historic Linux Kernel Makefile' --node polaris --password polaris --servername polaris-kvm-tsm-server 
/tmp/archives/linux/Makefile
```
One should see the following result:
```
[DEBUG] 1488452577.504628 [26983] tsmapi.c:466 dsmSetUp: handle: 0 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452577.605622 [26983] tsmapi.c:511 dsmInitEx: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452577.609448 [26983] tsmapi.c:530 dsmRegisterFS: handle: 1 ANS0242W (RC2062) On dsmRegisterFS the filespace is already registered
[DEBUG] 1488452577.609482 [26983] tsmapi.c:585 dsmQuerySessOptions: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488452577.609488 [26983] tsmapi.c:604 
DSMI_DIR 	: /opt/tivoli/tsm/client/api/bin64
DSMI_CONFIG 	: /opt/tivoli/tsm/client/api/bin64/dsm.opt
serverName 	: POLARIS-KVM-TSM-SERVER
commMethod 	: 1
serverAddress 	: 192.168.254.101
nodeName 	: POLARIS
compress 	: 0
compressalways 	: 1
passwordAccess 	: 0
[DEBUG] 1488452577.609508 [26983] tsmapi.c:610 dsmQuerySessInfo: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488452577.609513 [26983] tsmapi.c:621 
Server's ver.rel.lev       : 7.1.3.0
ArchiveRetentionProtection : No

[INFO] 1488452577.609519 [26983] tsmapi.c:628 
Max number of multiple objects per transaction: 4096
Max number of Bytes per transaction: 26214400
dsmSessInfo.fsdelim: /
dsmSessInfo.hldelim: /

[INFO] 1488452577.609526 [26983] tsmapi.c:651 IBM API library version = 7.1.3.0

[INFO] 1488452577.609531 [26983] tsmapi.c:1508 tsm_archive_fpath:
fs: /, fpath: /tmp/archives/linux/Makefile, desc: Historic Linux Kernel Makefile, fd: -1, *lu_fid: (nil)
[DEBUG] 1488452577.609805 [26983] tsmapi.c:1137 dsmBeginTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452577.609829 [26983] tsmapi.c:1145 dsmBindMC: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452577.609863 [26983] tsmapi.c:1164 dsmSendObj: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452577.609900 [26983] tsmapi.c:1194 dsmSendData: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488452577.609906 [26983] tsmapi.c:1201 cur_read: 2186, total_read: 2186, total_size: 2186
[DEBUG] 1488452577.609917 [26983] tsmapi.c:1231 dsmEndSendObj: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452577.713329 [26983] tsmapi.c:1242 dsmEndTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488452577.713358 [26983] tsmapi.c:1259 
*** successfully archived: DSM_OBJ_FILE /tmp/archives/linux/Makefile of size: 2186 bytes with settings ***
fs: /
hl: /tmp/archives/linux
ll: /Makefile
desc: Historic Linux Kernel Makefile
```

For archiving the complete `/tmp/archives/linux` directory (with all sub-directories) perform following command:
```
./src/ltsmc -v debug --archive --recursive --fsname '/' -d 'Complete Historic Linux Kernel' --node polaris --password polaris --servername polaris-kvm-tsm-server /tmp/archives/linux
```
One should see the following result:
```
[DEBUG] 1488452711.376449 [27062] tsmapi.c:466 dsmSetUp: handle: 0 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.476296 [27062] tsmapi.c:511 dsmInitEx: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.479322 [27062] tsmapi.c:530 dsmRegisterFS: handle: 1 ANS0242W (RC2062) On dsmRegisterFS the filespace is already registered
[DEBUG] 1488452711.479359 [27062] tsmapi.c:585 dsmQuerySessOptions: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488452711.479370 [27062] tsmapi.c:604 
DSMI_DIR 	: /opt/tivoli/tsm/client/api/bin64
DSMI_CONFIG 	: /opt/tivoli/tsm/client/api/bin64/dsm.opt
serverName 	: POLARIS-KVM-TSM-SERVER
commMethod 	: 1
serverAddress 	: 192.168.254.101
nodeName 	: POLARIS
compress 	: 0
compressalways 	: 1
passwordAccess 	: 0
[DEBUG] 1488452711.479492 [27062] tsmapi.c:610 dsmQuerySessInfo: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488452711.479505 [27062] tsmapi.c:621 
Server's ver.rel.lev       : 7.1.3.0
ArchiveRetentionProtection : No

[INFO] 1488452711.479533 [27062] tsmapi.c:628 
Max number of multiple objects per transaction: 4096
Max number of Bytes per transaction: 26214400
dsmSessInfo.fsdelim: /
dsmSessInfo.hldelim: /

[INFO] 1488452711.479571 [27062] tsmapi.c:651 IBM API library version = 7.1.3.0

[INFO] 1488452711.479585 [27062] tsmapi.c:1508 tsm_archive_fpath:
fs: /, fpath: /tmp/archives/linux, desc: Complete Historic Linux Kernel, fd: -1, *lu_fid: (nil)
[DEBUG] 1488452711.479885 [27062] tsmapi.c:1137 dsmBeginTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.479919 [27062] tsmapi.c:1145 dsmBindMC: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.479966 [27062] tsmapi.c:1164 dsmSendObj: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.479988 [27062] tsmapi.c:1231 dsmEndSendObj: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.594322 [27062] tsmapi.c:1242 dsmEndTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488452711.594350 [27062] tsmapi.c:1259 
*** successfully archived: DSM_OBJ_DIRECTORY /tmp/archives/linux/init of size: 0 bytes with settings ***
fs: /
hl: /tmp/archives/linux
ll: /init
desc: Complete Historic Linux Kernel

[DEBUG] 1488452711.594672 [27062] tsmapi.c:1137 dsmBeginTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.594700 [27062] tsmapi.c:1145 dsmBindMC: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.594745 [27062] tsmapi.c:1164 dsmSendObj: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.594810 [27062] tsmapi.c:1194 dsmSendData: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488452711.594818 [27062] tsmapi.c:1201 cur_read: 3601, total_read: 3601, total_size: 3601
[DEBUG] 1488452711.594832 [27062] tsmapi.c:1231 dsmEndSendObj: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452711.626108 [27062] tsmapi.c:1242 dsmEndTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488452711.626136 [27062] tsmapi.c:1259 
*** successfully archived: DSM_OBJ_FILE /tmp/archives/linux/init/main.c of size: 3601 bytes with settings ***
fs: /
hl: /tmp/archives/linux/init
ll: /main.c
desc: Complete Historic Linux Kernel
...
```
### Querying Data
For querying all data stored in directory `/tmp/archives/linux`
```
./src/ltsmc -v debug --query --fsname '/' --node polaris --password polaris --servername polaris-kvm-tsm-server '/tmp/archives/linux*/*'
```
One should see the following result:
```
...
[INFO] 1488452910.950235 [27098] tsmapi.c:922 extract_hl_ll:
fpath: /tmp/archives/linux*/*
hl: /tmp/archives/linux*
ll: /*

[INFO] 1488452910.950241 [27098] tsmapi.c:689 query structure
fs: /
hl: /tmp/archives/linux*
ll: /*
owner: 
descr: *
[DEBUG] 1488452910.950517 [27098] tsmapi.c:692 dsmBeginQuery: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488452911.033557 [27098] tsmapi.c:709 dsmGetNextQObj: handle: 1 ANS0258I (RC2200) On dsmGetNextQObj or dsmGetData there is more available data
[INFO] 1488452911.033588 [27098] tsmapi.c:438 object # 1
fs: /, hl: /tmp/archives/linux, ll: /boot
object id (hi,lo)                          : (0,32605)
object info length                         : 32
object info size (hi,lo)                   : (0,4096)
object type                                : DSM_OBJ_DIRECTORY
object magic id                            : 71147
archive description                        : Complete Historic Linux Kernel
owner                                      : 
insert date                                : 2017/03/02 12:05:10
expiration date                            : 2018/03/02 12:05:10
restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (2,0,4831,0,0)
estimated size (hi,lo)                     : (0,4096)

[DEBUG] 1488452911.033691 [27098] tsmapi.c:709 dsmGetNextQObj: handle: 1 ANS0258I (RC2200) On dsmGetNextQObj or dsmGetData there is more available data
[INFO] 1488452911.033705 [27098] tsmapi.c:438 object # 2
fs: /, hl: /tmp/archives/linux, ll: /fs
object id (hi,lo)                          : (0,32681)
object info length                         : 32
object info size (hi,lo)                   : (0,4096)
object type                                : DSM_OBJ_DIRECTORY
object magic id                            : 71147
archive description                        : Complete Historic Linux Kernel
owner                                      : 
insert date                                : 2017/03/02 12:05:12
expiration date                            : 2018/03/02 12:05:12
restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (2,0,4907,0,0)
estimated size (hi,lo)                     : (0,4096)
```
Querying the single file `/tmp/archives/linux/Makefile`
```
./src/ltsmc -v debug --query --fsname '/' --node polaris --password polaris --servername polaris-kvm-tsm-server '/tmp/archives/linux/Makefile'
```
One should see the following result:
```
...
[INFO] 1488453027.105802 [27335] tsmapi.c:922 extract_hl_ll:
fpath: /tmp/archives/linux/Makefile
hl: /tmp/archives/linux
ll: /Makefile

[INFO] 1488453027.105836 [27335] tsmapi.c:689 query structure
fs: /
hl: /tmp/archives/linux
ll: /Makefile
owner: 
descr: *
[DEBUG] 1488453027.106126 [27335] tsmapi.c:692 dsmBeginQuery: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488453027.133116 [27335] tsmapi.c:709 dsmGetNextQObj: handle: 1 ANS0258I (RC2200) On dsmGetNextQObj or dsmGetData there is more available data
[INFO] 1488453027.133145 [27335] tsmapi.c:438 object # 1
fs: /, hl: /tmp/archives/linux, ll: /Makefile
object id (hi,lo)                          : (0,32601)
object info length                         : 32
object info size (hi,lo)                   : (0,2186)
object type                                : DSM_OBJ_FILE
object magic id                            : 71147
archive description                        : Historic Linux Kernel Makefile
owner                                      : 
insert date                                : 2017/03/02 12:02:56
expiration date                            : 2018/03/02 12:02:56
restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (2,0,4827,0,0)
estimated size (hi,lo)                     : (0,2186)

[DEBUG] 1488453027.133207 [27335] tsmapi.c:709 dsmGetNextQObj: handle: 1 ANS0258I (RC2200) On dsmGetNextQObj or dsmGetData there is more available data
[INFO] 1488453027.133217 [27335] tsmapi.c:438 object # 2
fs: /, hl: /tmp/archives/linux, ll: /Makefile
object id (hi,lo)                          : (0,32604)
object info length                         : 32
object info size (hi,lo)                   : (0,2186)
object type                                : DSM_OBJ_FILE
object magic id                            : 71147
archive description                        : Complete Historic Linux Kernel
owner                                      : 
insert date                                : 2017/03/02 12:05:10
expiration date                            : 2018/03/02 12:05:10
restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (2,0,4830,0,0)
estimated size (hi,lo)                     : (0,2186)

[DEBUG] 1488453027.133267 [27335] tsmapi.c:709 dsmGetNextQObj: handle: 1 ANS0272I (RC121)  The operation is finished
```
Note, when archiving the same file multiple times *t*, the file is stored on the TSM server *t* times, however, each file as a unique object identifier as well as a different timestamp.
### Retrieving Data
We first delete all data in `/tmp/archives/linux` and the restore the data to that directory:
```
rm -rf /tmp/archives/linux && ./src/ltsmc -v info --retrieve --fsname '/' --node polaris --password polaris --servername polaris-kvm-tsm-server '/tmp/archives/linux*/*'
```
One should see the following result:
```
...
[INFO] 1488454996.574687 [28773] tsmapi.c:689 query structure
fs: /
hl: /tmp/archives/linux*
ll: /*
owner: 
descr: *
[INFO] 1488454996.729380 [28773] tsmapi.c:986 get_query: 0, rc: 0
[INFO] 1488454996.729413 [28773] tsmapi.c:986 get_query: 1, rc: 0
...
...
[INFO] 1488454996.730529 [28773] tsmapi.c:438 object # 0
fs: /, hl: /tmp/archives/linux, ll: /Makefile
object id (hi,lo)                          : (0,33205)
object info length                         : 32
object info size (hi,lo)                   : (0,2186)
object type                                : DSM_OBJ_FILE
object magic id                            : 71147
archive description                        : Historic Linux Kernel Makefile
owner                                      : 
insert date                                : 2017/03/02 12:33:13
expiration date                            : 2018/03/02 12:33:13
restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (2,0,5431,0,0)
estimated size (hi,lo)                     : (0,2186)

[INFO] 1488454996.730642 [28773] tsmapi.c:284 mkdir_p(/tmp/archives/linux)
[INFO] 1488454996.730654 [28773] tsmapi.c:243 fs/hl/ll fpath: //tmp/archives/linux/Makefile

[INFO] 1488454996.806618 [28773] tsmapi.c:349 cur_written: 2186, total_written: 0, obj_size: 2186
[INFO] 1488454996.806663 [28773] tsmapi.c:1032 retrieve_obj, rc: 0

[INFO] 1488454996.806673 [28773] tsmapi.c:1009 get_query: 1, rc: 0
[INFO] 1488454996.806681 [28773] tsmapi.c:1020 retrieving object:
[INFO] 1488454996.806691 [28773] tsmapi.c:438 object # 1
fs: /, hl: /tmp/archives/linux, ll: /init
object id (hi,lo)                          : (0,33206)
object info length                         : 32
object info size (hi,lo)                   : (0,4096)
object type                                : DSM_OBJ_DIRECTORY
object magic id                            : 71147
archive description                        : Complete Historic Linux Kernel
owner                                      : 
insert date                                : 2017/03/02 12:33:56
expiration date                            : 2018/03/02 12:33:56
restore order (top,hi_hi,hi_lo,lo_hi,lo_lo): (2,0,5432,0,0)
estimated size (hi,lo)                     : (0,4096)
.....
```
### Deleting Data
To delete data on the TSM perform the following command:
```
./src/ltsmc -v debug --delete --fsname '/' --node polaris --password polaris --servername polaris-kvm-tsm-server '/tmp/archives/linux*/*'
```
One should see the following result:
```
...
[INFO] 1488455109.715133 [28860] tsmapi.c:857 get_query: 95, rc: 0
[DEBUG] 1488455109.715288 [28860] tsmapi.c:813 dsmBeginTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488455109.715308 [28860] tsmapi.c:823 dsmDeleteObj: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488455109.720409 [28860] tsmapi.c:831 dsmEndTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488455109.720425 [28860] tsmapi.c:881 
deleted obj fs: DSM_OBJ_FILE
		fs: /
		hl: /tmp/archives/linux/lib
		ll: /write.c
[INFO] 1488455109.720433 [28860] tsmapi.c:857 get_query: 96, rc: 0
[DEBUG] 1488455109.720587 [28860] tsmapi.c:813 dsmBeginTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488455109.720607 [28860] tsmapi.c:823 dsmDeleteObj: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488455109.725719 [28860] tsmapi.c:831 dsmEndTxn: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1488455109.725735 [28860] tsmapi.c:881 
deleted obj fs: DSM_OBJ_FILE
		fs: /
		hl: /tmp/archives/linux/mm
		ll: /Makefile
...		
```
A subsequent *delete* or *query* command shows that there are no TSM objects of the form '/tmp/archives/linux*/*' found on the TSM server.
```
./src/ltsmc -v debug --delete --fsname '/' --node polaris --password polaris --servername polaris-kvm-tsm-server '/tmp/archives/linux*/*'
```
One should see the following result:
```
...
[INFO] 1488455280.580033 [28900] tsmapi.c:689 query structure
fs: /
hl: /tmp/archives/linux*
ll: /*
owner: 
descr: *
[DEBUG] 1488455280.580325 [28900] tsmapi.c:692 dsmBeginQuery: handle: 1 ANS0302I (RC0)    Successfully done.
[DEBUG] 1488455280.603063 [28900] tsmapi.c:709 dsmGetNextQObj: handle: 1 ANS1302E (RC2)    No objects on server match query
[MESSAGE] 1488455280.603089 [28900] tsmapi.c:732 query has no match
```

More documentation and detail is provided in the manual pages [ltsmc.1](https://github.com/tstibor/ltsm/blob/master/docs/ltsmc.1) and [lhsmtool_tsm.1](https://github.com/tstibor/ltsm/blob/master/docs/lhsmtool_tsm.1).

## Screenshot and Screencast
Klick on the screenshot to see the full screencast which demonstrates the usage and functionality of this project.
<a href="http://web-docs.gsi.de/~tstibor/tsm/ltsm-screencast-2.mp4" rel="screencast">![screencast](http://web-docs.gsi.de/~tstibor/tsm/ltsm-screenshot.png)</a>

## Work in Progress
* Implement multiple objects per transaction, currently a single object is processed per transaction.
* Extend test suite for larger code coverage.
* ~~Thread handling and cleanup when archiving/retrieving/deleting is in progress, but shutdown is triggered.~~
 
## References
A thorough description and code examples of IBM's low-level TSM API/library can be found in the open document [Using the Application Programming Interface](http://web-docs.gsi.de/~tstibor/tsm/doc/using_the_programming_application_interface.pdf), Fourth edition (September 2015).

## License
This project itself is licensed under the [GPL-2 license](http://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html). The code however, depends on IBM's low-level TSM API/libraries which are distributed 
under different licenses and thus are not provided in the repository. See [IBM Tivoli Storage Manager Server](http://ftp.software.ibm.com/storage/tivoli-storage-management/maintenance/server/v7r1/Linux/7.1.5.000/README.htm).

## Warranty
Note, this project is still under development and in an early beta development state.
