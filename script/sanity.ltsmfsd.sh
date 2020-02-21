#!/bin/bash

source $(dirname $(readlink -f $0))/include.sh

# Exit on error.
set -e
VERBOSE=0

##########################################################
# settings
##########################################################
LTSM_BIN="src/ltsmc"
LTSM_NODE=${1-polaris}
LTSM_PASSWORD=${2-polaris1234}
LTSM_SERVERNAME=${3-tsmserver-8}
LTSM_VERBOSE=${4-message}
LTSM_FS=${5-/tmp}
LTSM_OWNER=${6-testowner}

FSDBENCH_BIN="src/test/fsdbench"
FSDBENCH_SIZE=4194304
FSDBENCH_NUMBER=100
FSDBENCH_THREADS=4
FSDBENCH_RESULT="/tmp/fsdbench.txt"
FSD_FSNAME="/lustre"
FSD_FPATH="${FSD_FSNAME}/fsdbench"
FSD_SERVER="localhost"

##########################################################
# main
##########################################################
__check_bin "${LTSM_BIN}"
__check_bin "${FSDBENCH_BIN}"

echo "sending ${FSDBENCH_NUMBER} files of size ${FSDBENCH_SIZE} with ${FSDBENCH_THREADS} threads to FSD server"
${FSDBENCH_BIN} -v info -z ${FSDBENCH_SIZE} -b ${FSDBENCH_NUMBER} -t ${FSDBENCH_THREADS} \
		-f ${FSD_FSNAME} -a ${FSD_FPATH} -n ${LTSM_NODE} -p ${LTSM_PASSWORD} \
		-s ${FSD_SERVER} > ${FSDBENCH_RESULT} 2>&1
FILE_CRC32_LIST=`cat ${FSDBENCH_RESULT} | awk '/fsd_fclose/{print $8"@"$10}' | tr -d "\'"`
FILE_LIST=`cat ${FSDBENCH_RESULT} | awk '/fsd_fclose/{print $8}' | tr -d "\'"`

# Wait until all files are archived.
N_FILES=0
FSDBENCH_NUMBER=$((FSDBENCH_NUMBER + 0))
while [[ ${FSDBENCH_NUMBER} > ${N_FILES} ]]
do
    for f in ${FILE_LIST}
    do
	if ${LTSM_BIN} --query --node ${LTSM_NODE} --password ${LTSM_PASSWORD} \
    		       --servername ${LTSM_SERVERNAME} --fsname ${FSD_FSNAME} \
		       --verbose message "${f}" 2>&1 | grep -q "DSM_OBJ_FILE"; then
	    N_FILES=$((N_FILES + 1))
	    echo -ne "waiting until ${N_FILES}/${FSDBENCH_NUMBER} files are archived\033[0K\r"
	fi
    done
done

# Verify files created with fsdbench, copied to Lustre and TSM archived have mutually identical CRC32 values.
for f in ${FILE_CRC32_LIST}
do
    FILE_AND_CRC32=(${f//@/ })
    CRC32_FSDBENCH=${FILE_AND_CRC32[1]}
    CRC32_LUSTRE=`${LTSM_BIN} --checksum ${FILE_AND_CRC32[0]} | awk '{print $2}'`
    CRC32_ARCHIVED=`${LTSM_BIN} --query --node ${LTSM_NODE} --password ${LTSM_PASSWORD} \
    				--servername ${LTSM_SERVERNAME} --fsname ${FSD_FSNAME} \
				--verbose message "${FILE_AND_CRC32[0]}" \
				| awk '{gsub(/^crc32:/, "", $9); print $9}'`

    if [[ ${CRC32_FSDBENCH} == ${CRC32_LUSTRE} && ${CRC32_FSDBENCH} == ${CRC32_ARCHIVED} ]];
    then
	echo "CRC32 ${FILE_AND_CRC32[0]} (fsdbench ${CRC32_FSDBENCH},lustre ${CRC32_LUSTRE},tsm ${CRC32_ARCHIVED}) OK"
    else
    	echo "CRC32 checksums did not match for ${FILE_AND_CRC32[0]} (fsdbench ${CRC32_FSDBENCH},lustre ${CRC32_LUSTRE},tsm ${CRC32_ARCHIVED})"
    	exit 1
    fi
done
