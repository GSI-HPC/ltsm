#!/bin/bash
# Title       : ltsmsync.sh
# Date        : Mon 19 Apr 2021 09:29:43 AM CEST
# Version     : 0.1.1
# Author      : "Thomas Stibor" <t.stibor@gsi.de>
# Description : Query TSM server by means of ltsmc and create from the query result empty files
#               with appropriate Lustre HSM flags. Subsequent files access, seamlessly
#               retrieve the raw data of the files by means of the Lustre copytool or ltsmc.
#               The copytool retrieve is triggered by means of command 'head' and
#               parallized by JOBS=4 concurrent retrieving processes.
# Note        : Lustre copytool must be running when argument {OMIT_COPYTOOL} equals 'no'.

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
DAYS_AGO=7
DRY_RUN=0
CRC32_VERIFY=0
OMIT_COPYTOOL="no"

__usage() {
    echo -e "usage: ${0} <LUSTRE_DIRECTORY>\n" \
	 "\t-f, --filespace <string> [default: ${FSPACE}]\n" \
	 "\t-s, --servername <string> [default: ${SERVERNAME}]\n" \
	 "\t-n, --node <string>\n" \
	 "\t-p, --password <string>\n" \
	 "\t-o, --owner <string>\n" \
	 "\t-a, --archive-id <int> [default: ${ARCHIVE_ID}]\n" \
	 "\t-j, --jobs <int> [default: ${JOBS}]\n" \
	 "\t-y, --days-ago <int> [default: ${DAYS_AGO}]\n" \
	 "\t-c, --crc32-verify\n" \
	 "\t-x, --omit-copytool <yes,no> [default: ${OMIT_COPYTOOL}]\n" \
	 "\t-d, --dry-run\n"
    exit 1
}

__log() {
    echo "[$(date "+%F %T")] $@"
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
	    # Wait for any job to finish.
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
	if [[ ${OMIT_COPYTOOL} -eq 1 ]]; then
	    ${LTSMC_BIN} --retrieve --latest --fsname ${FSPACE} \
			 --servername ${SERVERNAME} -n ${NODE} \
			 --password ${PASSWORD} -v message \
			 --datelow "${DATE_TIME_DAYS_AGO}" \
			 "${f}"
	    rc=$?
	    if [[ ${rc} -ne 0 ]]; then
		__log "retrieving file ${f} from TSM server failed"
		return ${rc}
	    fi
	    sudo ${LFS_BIN} hsm_set --exists --archived --archive-id ${i} ${f} || exit 1
	else
	    touch ${f} || exit 1
	    sudo ${LFS_BIN} hsm_set --exists --archived --archive-id ${i} ${f} || exit 1
	    ${LFS_BIN} hsm_release ${f} || exit 1
	    # Comment out the line below for creating an empty HSM released file.
	    head --bytes=1 ${f} > /dev/null
	    rc=0
	fi
    else
	__log "file ${f} already exists"
    fi

    if [[ ${rc} -ne 0 ]]; then
	# (0x0000000d) released exists archived
	${LFS_BIN} hsm_state ${f} | awk '/(0x0000000d)/{f=1} /^Match/{exit} END{exit !f}'
	RC_RELEASE_EXISTS=$?

	# (0x00000009) exists archived
	${LFS_BIN} hsm_state ${f} | awk '/(0x00000009)/{f=1} /^Match/{exit} END{exit !f}'
	RC_ARCHIVE_EXISTS=$?

	if [[ ${RC_RELEASE_EXISTS} -ne 0 && ${RC_ARCHIVE_EXISTS} -ne 0 ]]; then
	    __log "file ${f} already exists, however with incorrect Lustre HSM flags"
	    rc=1
	fi
    else
	__log "successfully retrieved file ${f}"
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
    -y|--days-ago)
	DAYS_AGO="$2"
	shift
	;;
    -c|--crc32-verify)
	CRC32_VERIFY=1
	;;
    -x|--omit-copytool)
	OMIT_COPYTOOL="$2"
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
[[ -z "$@" ]]                 && { echo "missing argument <LUSTRE_DIRECTORY>"; __usage; }
[[ -z "${FSPACE}" ]]          && { echo "missing argument -f, --filespace"; __usage; }
[[ -z "${SERVERNAME}" ]]      && { echo "missing argument -s, --servername"; __usage; }
[[ -z "${NODE}" ]]            && { echo "missing argument -n, --node"; __usage; }
[[ -z "${PASSWORD}" ]]        && { echo "missing argument -p, --password"; __usage; }
[[ -z "${OMIT_COPYTOOL}" ]]   && { echo "missing argument -x, --omit-copytool"; __usage; }
# Check for incorrect argument.
if [[ "${OMIT_COPYTOOL}" == [Yy][Ee][Ss] ]];
then
    OMIT_COPYTOOL=1
elif [[ "${OMIT_COPYTOOL}" == [Nn][Oo] ]];
then
    OMIT_COPYTOOL=0
else
    { echo "incorrect argument -x, --omit-copytool ${OMIT_COPYTOOL}"; __usage; }
fi

LUSTRE_DIR="$@"

# Make sure FSPACE is prefix of LUSTRE_DIRECTORY
[[ ! "${LUSTRE_DIR}" =~ ^${FSPACE}.* ]] && { echo "Filespace is not prefix of Lustre directory"; __usage; }

# Create list of files from query.
DATE_TIME_DAYS_AGO=`date --date="${DAYS_AGO} days ago" +"%Y:%m:%d:%H:%M:%S"`
FILE_LIST=`${LTSMC_BIN} -f ${FSPACE} --query --latest \
	     --servername ${SERVERNAME} -n ${NODE} \
	     --password ${PASSWORD} "${LUSTRE_DIR}" -v message --datelow "${DATE_TIME_DAYS_AGO}" \
	     | awk '{gsub(/^fs:/, "", $6); gsub(/^hl:/, "", $7); gsub(/^ll:/, "", $8); gsub(/^crc32:/, "@", $9); print $6 $7 $8 $9}'`

[[ -z ${FILE_LIST} ]] && { __log "no files found on TSM server"; exit 1; }

for f in ${FILE_LIST}
do
    # Split ${f} in filename ${FILE_AND_CRC[0]} and crc32 ${FILE_AND_CRC[1]}
    FILE_AND_CRC=(${f//@/ })

    # File does not exist, retrieve it.
    if [[ ! -f ${FILE_AND_CRC[0]} ]]; then
	if [[ ${DRY_RUN} -eq 1 ]]; then
	    __log "__retrieve_file '${FILE_AND_CRC[0]}' '${ARCHIVE_ID}'"
	else
	    ( __retrieve_file "${FILE_AND_CRC[0]}" "${ARCHIVE_ID}" ) &
	fi
    # File exists, check whether crc32 matches with those stored on TSM server.
    else
	if [[ ${CRC32_VERIFY} -eq 1 ]]; then
	    file_crc32="`${LTSMC_BIN} --checksum ${FILE_AND_CRC[0]} | awk '{print $2}'`"
	    if [[ "${file_crc32}"  != "${FILE_AND_CRC[1]}" ]]; then
		__log "crc32 mismatch of file ${FILE_AND_CRC[0]} (${FILE_AND_CRC[1]},${file_crc32})"
		if [[ ${DRY_RUN} -eq 1 ]]; then
		    __log "__retrieve_file crc32-verified '${FILE_AND_CRC[0]}' '${ARCHIVE_ID}'"
		else
		    ( __retrieve_file "${FILE_AND_CRC[0]}" "${ARCHIVE_ID}" ) &
		fi
	    else
		__log "file already exists ${FILE_AND_CRC[0]} and has valid crc32 (${FILE_AND_CRC[1]},${file_crc32})"
	    fi
	else # Do not check crc32, but only whether file size is > 0.
	    file_size=$(wc -c ${FILE_AND_CRC[0]} | awk '{print $1}')
	    if [[ ${file_size} -eq 0 ]]; then
		if [[ ${DRY_RUN} -eq 1 ]]; then
		    __log "__retrieve_file size-verified '${FILE_AND_CRC[0]}' '${ARCHIVE_ID}'"
		else
		    ( __retrieve_file "${FILE_AND_CRC[0]}" "${ARCHIVE_ID}" ) &
		fi
	    else
		__log "file already exists ${FILE_AND_CRC[0]} and has file size '${file_size}'"
	    fi
	fi
    fi
    __job_limit ${JOBS}
done
