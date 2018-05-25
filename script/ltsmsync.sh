#!/bin/bash
# Title       : ltsmsync.sh
# Date        : Fri 25 May 2018 03:30:44 PM CEST
# Version     : 0.0.6
# Author      : "Thomas Stibor" <t.stibor@gsi.de>
# Description : Query TSM server and create from the query result empty files
#               with appropriate Lustre HSM flags. Subsequent files access, transparently
#               retrieve the raw data of the files by means of the Lustre copytool.
#               At current this script also retrieve the raw data by reading with command 'head'
#               this first byte and thus triggers the copytool retrieve process. The retrieving
#               process is parallized by JOBS=4 concurrent retrieving processes.
# Note        : Lustre copytool must be running.

# Path to 3rd party executables.
LFS_BIN="/usr/bin/lfs"
LTSMC_BIN="/usr/bin/ltsmc"

# Default arguments.
FSPACE="/"
SERVERNAME=""
NODE=""
PASSWORD=""
OWNER=""
ARCHIVE_ID=0
JOBS=4
DRY_RUN=0

__usage() {
    echo -e "usage: ${0} <LUSTRE_DIRECTORY>\n" \
	 "\t-f, --filespace <string> [default: ${FSPACE}]\n" \
	 "\t-s, --servername <string> [default: ${SERVERNAME}]\n" \
	 "\t-n, --node <string>\n" \
	 "\t-p, --password <string>\n" \
	 "\t-o, --owner <string>\n" \
	 "\t-a, --archive-id <int> [default: ${ARCHIVE_ID}]\n" \
	 "\t-j, --jobs <int> [default: ${JOBS}]\n" \
	 "\t-d, --dry-run\n"
    exit 1
}

__check_bin() {
    [[ ! -f ${1} ]] && { echo "cannot find executable file ${1}"; exit 1; }
}

__job_limit() {
    # Test for single positive integer input.
    if (( $# == 1 )) && [[ $1 =~ ^[1-9][0-9]*$ ]]
    then
	# Check number of running jobs.
	joblist=($(jobs -rp))
	while (( ${#joblist[*]} >= $1 ))
	do
	    # Wait for any job to finish
	    command='wait '${joblist[0]}
	    for job in ${joblist[@]:1}
	    do
		command+=' || wait '$job
	    done
	    eval $command
	    joblist=($(jobs -rp))
	done
    fi
}

__retrieve_file() {

    [[ "$#" -ne 2 ]] && { echo "wrong number of arguments"; exit 1; }

    local f=${1}		# Filename.
    local i=${2}		# Archive ID.
    local rc=1

    DIR=`dirname ${f}`
    if [[ ! -d ${DIR} ]]; then
	mkdir -p ${DIR} || exit 1
    fi
    if [[ ! -f ${f} ]]; then
	touch ${f} || exit 1
	sudo ${LFS_BIN} hsm_set --exists --archived --archive-id ${i} ${f} || exit 1
	${LFS_BIN} hsm_release ${f} || exit 1
	# Comment out the line below for creating an empty HSM released file.
	head --bytes=1 ${f} > /dev/null
	rc=0
    else
	echo "file ${f} already exists"
    fi

    if [[ ${rc} -ne 0 ]]; then
	# (0x0000000d) released exists archived
	${LFS_BIN} hsm_state ${f} | awk '/(0x0000000d)/{f=1} /^Match/{exit} END{exit !f}'
	RC_RELEASE_EXISTS=$?

	# (0x00000009) exists archived
	${LFS_BIN} hsm_state ${f} | awk '/(0x00000009)/{f=1} /^Match/{exit} END{exit !f}'
	RC_ARCHIVE_EXISTS=$?

	if [[ ${RC_RELEASE_EXISTS} -ne 0 && ${RC_ARCHIVE_EXISTS} -ne 0 ]]; then
	    echo "file ${f} already exists, however with incorrect Lustre HSM flags"
	    rc=1
	fi
    else
	echo "successfully retrieved file ${f}"
	rc=0
    fi

    # File already exits.
    return ${rc}
}

##################################
# main
##################################
__check_bin ${LFS_BIN}
__check_bin ${LTSMC_BIN}

# Parse arguments.
while [[ $# -gt 1 ]]
do
arg="$1"

case $arg in
    -f|--filespace)
	FSPACE="$2"
	shift
	;;
    -s|--servername)
	SERVERNAME="$2"
	shift
	;;
    -n|--node)
	NODE="$2"
	shift
	;;
    -p|--password)
	PASSWORD="$2"
	shift
	;;
    -o|--owner)
	OWNER="$2"
	shift
	;;
    -a|--archive-id)
	ARCHIVE_ID="$2"
	shift
	;;
    -j|--jobs)
	JOBS="$2"
	shift
	;;
    -d|--dry-run)
	DRY_RUN=1
	;;
    *)
	echo "unknown argument $2"
	__usage
	;;
esac
shift
done

# Check for missing arguments.
[[ -z "$@" ]]            && { echo "missing argument <LUSTRE_DIRECTORY>"; __usage; }
[[ -z "${FSPACE}" ]]     && { echo "missing argument -f, --filespace"; __usage; }
[[ -z "${SERVERNAME}" ]] && { echo "missing argument -s, --servername"; __usage; }
[[ -z "${NODE}" ]]       && { echo "missing argument -n, --node"; __usage; }
[[ -z "${PASSWORD}" ]]   && { echo "missing argument -p, --password"; __usage; }
LUSTRE_DIR="$@"

# Make sure FSPACE is prefix of LUSTRE_DIRECTORY
[[ ! "${LUSTRE_DIR}" =~ ^${FSPACE}.* ]] && { echo "Filespace is not prefix of Lustre directory"; __usage; }

# Create list of files from query.
FILE_LIST=`${LTSMC_BIN} -f ${FSPACE} --query \
	     --servername ${SERVERNAME} -n ${NODE} \
	     --password ${PASSWORD} "${LUSTRE_DIR}" -v message \
	     | awk '{gsub(/^fs:/, "", $6); gsub(/^hl:/, "", $7); gsub(/^ll:/, "", $8); gsub(/^crc32:/, "@", $9); print $6 $7 $8 $9}'`

[[ -z ${FILE_LIST} ]] && { echo "no files found on TSM server"; exit 1; }

for f in ${FILE_LIST}
do
    # Split ${f} in filename ${FILE_AND_CRC[0]} and crc32 ${FILE_AND_CRC[1]}
    FILE_AND_CRC=(${f//@/ })

    # File does not exist, retrieve it.
    if [[ ! -f ${FILE_AND_CRC[0]} ]]; then
	if [[ ${DRY_RUN} -eq 1 ]]; then
	    echo "__retrieve_file '${FILE_AND_CRC[0]}' '${ARCHIVE_ID}'"
	else
	    ( __retrieve_file "${FILE_AND_CRC[0]}" "${ARCHIVE_ID}" ) &
	fi
    # File exists, check whether crc32 matches with those stored on TSM server.
    else
	file_crc32="`${LTSMC_BIN} --checksum ${FILE_AND_CRC[0]} | awk '{print $2}'`"
	if [[ "${file_crc32}"  != "${FILE_AND_CRC[1]}" ]]; then
	    echo "crc32 mismatch of file ${FILE_AND_CRC[0]} (${FILE_AND_CRC[1]},${file_crc32})"
	    if [[ ${DRY_RUN} -eq 1 ]]; then
		echo "__retrieve_file '${FILE_AND_CRC[0]}' '${ARCHIVE_ID}'"
	    else
		( __retrieve_file "${FILE_AND_CRC[0]}" "${ARCHIVE_ID}" ) &
	    fi
	else
	    echo "file already exists ${FILE_AND_CRC[0]} and has valid crc32 (${FILE_AND_CRC[1]},${file_crc32})"
	fi
    fi
    __job_limit ${JOBS}
done

