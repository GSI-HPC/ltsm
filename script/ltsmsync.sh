#!/bin/bash
# Title       : ltsmsync.sh
# Date        : Fri 18 May 2018 12:20:41 PM CEST
# Version     : 0.0.4
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
FSPACE="/lustre/hebe"
SERVERNAME="myltsm01"
NODE=""
PASSWORD=""
OWNER=""
JOBS=4

__usage() {
    echo -e "usage: ${0} <LUSTRE_DIRECTORY>\n" \
	 "\t-f, --filespace <string> [default: ${FSPACE}]\n" \
	 "\t-s, --servername <string> [default: ${SERVERNAME}]\n" \
	 "\t-n, --node <string>\n" \
	 "\t-p, --password <string>\n" \
	 "\t-o, --owner <string>\n" \
	 "\t-j, --jobs <int> [default: 4]\n"
    exit 1
}

__check_bin() {
    [[ ! -f ${1} ]] && { echo "cannot find executable file ${1}"; exit 1; }
}

__job_limit () {
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

    [[ -z ${1} ]] && { echo "no argument provided"; exit 1; }

    local f=${1}
    local rc=1

    DIR=`dirname ${f}`
    if [[ ! -d ${DIR} ]]; then
	mkdir -p ${DIR} || exit 1
    fi
    if [[ ! -f ${f} ]]; then
	touch ${f} || exit 1
	sudo ${LFS_BIN} hsm_set --exists --archived ${f} || exit 1
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
    -j|--jobs)
	JOBS="$2"
	shift
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
    | awk '{gsub(/^fs:/, "", $6); gsub(/^hl:/, "", $7); gsub(/^ll:/, "", $8); print $6 $7 $8}'`

[[ -z ${FILE_LIST} ]] && { echo "no files found on TSM server"; exit 1; }

for f in ${FILE_LIST}
do
    # TODO: If file has incorrect CRC32, retrieve it.
    if [[ ! -f ${f} ]]; then
	( __retrieve_file "${f}" ) &
    fi
    __job_limit ${JOBS}
done

wait
