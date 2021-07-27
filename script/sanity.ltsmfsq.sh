#!/bin/bash

# Prerequisite: lhsmtool_tsm and ltsmfsq service are running.

source $(dirname $(readlink -f $0))/include.sh

# Exit on error.
set -e

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

FSQBENCH_BIN="src/test/fsqbench"
FSQBENCH_SIZE=$((((8 * RANDOM) % 4194304) + 1))
FSQBENCH_NUMBER=100
FSQBENCH_THREADS=4
FSQBENCH_RESULT=$(mktemp /tmp/fsqbench.XXXXXX)
FSQ_FSNAME="/lustre"
FSQ_FPATH="${FSQ_FSNAME}/fsqbench"
FSQ_SERVER="localhost"

##########################################################
# main
##########################################################
__check_bin "${LTSM_BIN}"
__check_bin "${FSQBENCH_BIN}"

echo "sending ${FSQBENCH_NUMBER} files of size ${FSQBENCH_SIZE} with ${FSQBENCH_THREADS} threads to FSQ server with result ${FSQBENCH_RESULT}"
${FSQBENCH_BIN} -v info -z ${FSQBENCH_SIZE} -b ${FSQBENCH_NUMBER} -t ${FSQBENCH_THREADS} \
		-f ${FSQ_FSNAME} -a ${FSQ_FPATH} -n ${LTSM_NODE} -p ${LTSM_PASSWORD} \
		-s ${FSQ_SERVER} > ${FSQBENCH_RESULT} 2>&1
FILE_CRC32_LIST=`cat ${FSQBENCH_RESULT} | awk '/fsq_fclose/{print $8"@"$10}' | tr -d "\'"`

# Wait until all files are archived.
N_FILES=0
FSQBENCH_NUMBER=$((FSQBENCH_NUMBER + 0))
while [[ ${FSQBENCH_NUMBER} > ${N_FILES} ]]
do
    for f in ${FILE_CRC32_LIST}
    do
	FILE_AND_CRC32=(${f//@/ })
	if ${LTSM_BIN} --query --node ${LTSM_NODE} --password ${LTSM_PASSWORD} \
		       --servername ${LTSM_SERVERNAME} --fsname ${FSQ_FSNAME} \
		       --verbose message "${FILE_AND_CRC32[0]}" 2>&1 | grep -q "DSM_OBJ_FILE"; then
	    N_FILES=$((N_FILES + 1))
	    echo -ne "waiting until ${N_FILES}/${FSQBENCH_NUMBER} files are archived\033[0K\r"
	fi
    done
done

# Verify files created with fsqbench, copied to Lustre and TSM archived have mutually identical CRC32 values.
for f in ${FILE_CRC32_LIST}
do
    FILE_AND_CRC32=(${f//@/ })
    CRC32_FSQBENCH=${FILE_AND_CRC32[1]}
    CRC32_LUSTRE=`${LTSM_BIN} --checksum ${FILE_AND_CRC32[0]} | awk '{print $2}'`
    CRC32_ARCHIVED=`${LTSM_BIN} --query --node ${LTSM_NODE} --password ${LTSM_PASSWORD} \
				--servername ${LTSM_SERVERNAME} --fsname ${FSQ_FSNAME} \
				--verbose message "${FILE_AND_CRC32[0]}" \
				| awk '{gsub(/^crc32:/, "", $9); print $9}'`

    if [[ ${CRC32_FSQBENCH} == ${CRC32_LUSTRE} && ${CRC32_FSQBENCH} == ${CRC32_ARCHIVED} ]];
    then
	echo "CRC32 ${FILE_AND_CRC32[0]} (fsqbench ${CRC32_FSQBENCH},lustre ${CRC32_LUSTRE},tsm ${CRC32_ARCHIVED}) OK"
    else
	echo "CRC32 checksums did not match for ${FILE_AND_CRC32[0]} (fsqbench ${CRC32_FSQBENCH},lustre ${CRC32_LUSTRE},tsm ${CRC32_ARCHIVED})"
    	exit 1
    fi
done
