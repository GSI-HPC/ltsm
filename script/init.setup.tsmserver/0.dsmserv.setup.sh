#!/bin/bash

cd ${HOME}
mkdir -p tsminst1 && cd tsminst1
sync && sleep 1

sudo /opt/tivoli/tsm/db2/instance/db2icrt -a SERVER -s ese -u tsminst1 tsminst1
sync && sleep 1

/opt/tivoli/tsm/db2/bin/db2 update dbm cfg using dftdbpath ${HOME}/tsminst1
sync && sleep 1

echo "DEVCONFIG /home/tsminst1/tsminst1/devconf.dat" >> ${HOME}/tsminst1/dsmserv.opt           
echo "VOLUMEHISTORY /home/tsminst1/tsminst1/volhist.dat" >> ${HOME}/tsminst1/dsmserv.opt

cat >${HOME}/.profile <<EOF
if [ -f /home/tsminst1/sqllib/db2profile ]; then
    . /home/tsminst1/sqllib/db2profile
fi
EOF
source ${HOME}/.profile

${HOME}/sqllib/adm/db2set -i tsminst1 DB2CODEPAGE=1208

TSM_META=${HOME}/tsm/meta
TSM_DATA=${HOME}/tsm/data
TSM_META_DB=${TSM_META}/db
TSM_META_LOG_ACTIVE=${TSM_META}/log/active_log
TSM_META_LOG_ARCHIVE=${TSM_META}/log/archive_log
mkdir -p ${TSM_META} ${TSM_DATA} ${TSM_META_DB} ${TSM_META_LOG_ACTIVE} ${TSM_META_LOG_ARCHIVE}
sync && sleep 1

dsmserv format dbdir=${TSM_META_DB} activelogsize=8196 activelogdir=${TSM_META_LOG_ACTIVE} archlogdir=${TSM_META_LOG_ARCHIVE}

cat >${HOME}/sqllib/userprofile <<EOF
export DSMI_CONFIG=${HOME}/tsminst1/tsmdbmgr.opt
export DSMI_DIR=/opt/tivoli/tsm/client/api/bin64
export DSMI_LOG=${HOME}/tsminst1
EOF


