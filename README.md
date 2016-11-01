# LTSM - Lightweight TSM API, TSM Console Client and Lustre Copytool for Archiving Data

A lightweight high-level TSM API/library (called *tsmmapi*) supporting operations
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
Download and install the TSM API client provided at [7.1.4.0-TIV-TSMBAC-LinuxX86_DEB.tar](http://ftp.software.ibm.com/storage/tivoli-storage-management/maintenance/client/v7r1/Linux/LinuxX86_DEB/BA/v714/).
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
  * `src/lhsmtool_tsm` (Lustre Copytool)
  * `src/ltsmc` (Console client)

## Lustre Copytool
Make sure to have enabled the HSM functionality on the MGS/MDS, e.g. `lctl set_param 'mdt.<LNAME>-MDT0000.hsm_control=enabled`
and TSM server is running as well as `dsm.sys` contains the proper entries such for example
```
SErvername              polaris-kvm-tsm-server
   Nodename             polaris
   TCPServeraddress     192.168.254.102
```
Start the Lustre Copytool as follows: `# lhsmtool_tsm -n polaris -p polaris -s polaris-kvm-tsm-server -u polaris '/lustre'`
where the parameters, *-n = nodename*, *-p = password* have to match with those setup on the TSM Server (see [TSM Server Installation Guide](http://web-docs.gsi.de/~tstibor/tsm/))
and *-s = servername* with the entry in *dsm.sys*. If everything is correctly setup you should see the following information:
```
[TRACE] 1478002437.508968 [21638] tsmapi.c:345 dsmInitEx: handle: 1 ANS0302I (RC0)    Successfully done.
[TRACE] 1478002437.512369 [21638] tsmapi.c:364 dsmRegisterFS: handle: 1 ANS0242W (RC2062) On dsmRegisterFS the filespace is already registered
[TRACE] 1478002437.512399 [21638] tsmapi.c:382 dsmQuerySessOptions: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1478002437.512405 [21638] tsmapi.c:401 
DSMI_DIR 	: /opt/tivoli/tsm/client/api/bin64
DSMI_CONFIG 	: /opt/tivoli/tsm/client/api/bin64/dsm.opt
serverName 	: POLARIS-KVM-TSM-SERVER
commMethod 	: 1
serverAddress 	: 192.168.254.101
nodeName 	: POLARIS
compress 	: 0
compressalways 	: 1
passwordAccess 	: 0
[TRACE] 1478002437.512433 [21638] tsmapi.c:407 dsmQuerySessInfo: handle: 1 ANS0302I (RC0)    Successfully done.
[INFO] 1478002437.512438 [21638] tsmapi.c:421 
Server's ver.rel.lev       : 7.1.3.0
ArchiveRetentionProtection : No

[INFO] 1478002437.512443 [21638] tsmapi.c:428 
Max number of multiple objects per transaction: 4096
Max number of Bytes per transaction: 26214400
dsmSessInfo.fsdelim: /
dsmSessInfo.hldelim: /

[TRACE] 1478002437.512450 [21638] tsmapi.c:451 API Library Version = 7.1.3.0

[TRACE] 1478002437.514016 [21638] lhsmtool_tsm.c:586 waiting for message from kernel
```

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
ltsmc --archive --fsname '/' -c 'Historic Linux Kernel Makefile' --node polaris --password polaris --servername=polaris-kvm-tsm-server 
/tmp/archives/linux/Makefile
```

For archiving the complete `/tmp/archives/linux` directory (with all sub-directories) perform following command:
```
ltsmc --archive --fsname '/' -c 'Complete Historic Linux Kernel' --node polaris --password polaris --servername=polaris-kvm-tsm-server /tmp/archives/linux
```
### Querying Data
For querying all data stored in directory `/tmp/archives/linux`
```
ltsmc --query --fsname '/' --node polaris --password polaris --servername=polaris-kvm-tsm-server --hl '/tmp/archives/linux/*' --ll '/*'
```
Querying the single file `/tmp/archives/linux/Makefile`
```
ltsmc --query --fsname '/' --node polaris --password polaris --servername=polaris-kvm-tsm-server --hl '/tmp/archives/linux' --ll '/Makefile'
```
### Retrieving Data
We first delete all data in `/tmp/archives/linux` and the restore the data to that directory:
```
rm -rf /tmp/archives/linux && ltsmc --retrieve --fsname '/' --node polaris --password polaris --servername=polaris-kvm-tsm-server --hl '/tmp/archives/linux/*' --ll '/*'
```

### Deleting Data
To delete data on the TSM perform the following command:
```
ltsmc --delete --fsname '/' --node polaris --password polaris --servername=polaris-kvm-tsm-server --hl '/tmp/archives/linux/*' --ll '/*'
```
A subsequent *delete* or *query* command shows that there is no data left on the TSM side.
```
ltsmc --delete --fsname '/' --node polaris --password polaris --servername=polaris-kvm-tsm-server --hl '/tmp/archives/linux/*' --ll '/*'
```

## Screenshot and Screencast
Klick on the screenshot to see the full screencast which demonstrates the usage and functionality of this project.
<a href="http://web-docs.gsi.de/~tstibor/tsm/ltsm-screencast-2.mp4" rel="screencast">![screencast](http://web-docs.gsi.de/~tstibor/tsm/ltsm-screenshot.png)</a>

## Work in Progress
* Implement multiple objects per transaction, currently a single object is processed per transaction.
* Extend test suite for larger code coverage.
* Provide Debian package
 
## References
A thorough description and code examples of IBM's low-level TSM API/library can be found in the open document [Using the Application Programming Interface](http://web-docs.gsi.de/~tstibor/tsm/doc/using_the_programming_application_interface.pdf), 2007.

## License
This project itself is licensed under the [GPL-2 license](http://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html). The code however, depends on IBM's low-level TSM API/library which are distributed 
under different licenses. See [IBM Tivoli Storage Manager Server](http://ftp.software.ibm.com/storage/tivoli-storage-management/maintenance/server/v7r1/Linux/7.1.5.000/README.htm).

## Warranty
Note, this project is still under development and in a beta development state.
