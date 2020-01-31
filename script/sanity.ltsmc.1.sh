#!/bin/bash

source $(dirname $(readlink -f $0))/include.sh
VERBOSE=1

__check_results()
{
    if [ ${1} != 0 ];
    then
	echo "${2}"
	rm -f ${3}
	exit 1
    fi
}

##########################################################
# settings
##########################################################
LTSM_BIN="src/ltsmc"
LTSM_NODE=${1-polaris}
LTSM_PASSWORD=${2-polaris1234}
LTSM_SERVERNAME=${3-tsmserver-8}
LTSM_VERBOSE=${4-message}
LTSM_FS=${5-/}
LTSM_OWNER=${6-testowner}

[ ${PWD##*/} == "script" ] && { LTSM_BIN="../${LTSM_BIN}"; }
__check_bin "${LTSM_BIN}"

##########################################################
# main
##########################################################
MAX_REPEAT=10
MAX_KiB=4096

for r in $(seq 1 ${MAX_REPEAT});
do
    RND_STR="/tmp/$(__rnd_string 8)"

    dd if=/dev/urandom of=${RND_STR} bs=1KiB count=$(( (RANDOM % ${MAX_KiB}) + 1 )) 2>/dev/null
    CRC32_ORIG=`${LTSM_BIN} --checksum ${RND_STR} | awk '{print $2}'`

    cat ${RND_STR} | ${LTSM_BIN} --pipe --node ${LTSM_NODE} --password ${LTSM_PASSWORD} \
				 --servername ${LTSM_SERVERNAME} --owner ${LTSM_OWNER} --verbose ${LTSM_VERBOSE} ${RND_STR}

    __check_results $? "archive via pipe failed" ${RND_STR}

    CRC32_ARCHIVED=`${LTSM_BIN} --delete --node ${LTSM_NODE} --password ${LTSM_PASSWORD} --servername ${LTSM_SERVERNAME} --owner ${LTSM_OWNER} --verbose info ${RND_STR} 2>&1 | awk '/crc32/{print $3}'`

    __check_results $? "query and delete failed" ${RND_STR}

    rm -f ${RND_STR}
    
    if [ ${CRC32_ORIG} != ${CRC32_ARCHIVED} ];
    then
    	echo "CRC32 checksum did not match for ${RND_STR}, CRC32_ORIG: ${CRC32_ORIG} != CRC32_ARCHIVED: ${CRC32_ARCHIVED}"
    	exit 1
    else
    	__log "CRC32 checksum matched for ${RND_STR}, CRC32_ORIG: ${CRC32_ORIG} == CRC32_ARCHIVED: ${CRC32_ARCHIVED}"
    fi
done
