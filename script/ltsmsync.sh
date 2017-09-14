#!/bin/bash
# Title       : ltsmsync.sh
# Date        : Thu 14 Sep 2017 01:14:03 PM CEST
# Version     : 0.0.2
# Author      : "Thomas Stibor" <t.stibor@gsi.de>
# Description : Query TSM server and create from the query result empty files
#               with appropriate Lustre HSM flags. Subsequent files access, transparently
#               restore the raw data of the files by means of the Lustre copytool.
# Note        : Lustre copytool must be running.

# Path to 3rd party executables.
LFS_BIN="/usr/bin/lfs"
LTSMC_BIN="/home/tstibor/dev/tsm/github/ltsm/src/ltsmc"

# Default arguments.
FSPACE="/lustre"
SERVERNAME="lxltsm01-tsm-server"
NODE=""
PASSWORD=""
OWNER=""

__usage() {
    echo -e "usage: ${0} <LUSTRE_DIRECTORY>\n" \
	 "\t-f, --filespace <string> [default: /lustre]\n" \
	 "\t-s, --servername <string> [default: lxltsm01-tsm-server]\n" \
	 "\t-n, --node <string>\n" \
	 "\t-p, --password <string>\n" \
	 "\t-o, --owner <string>\n"
    exit 1
}

__check_bin() {
    [[ ! -f ${1} ]] && { echo "cannot find executable file ${1}"; exit 1; }
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
# [[ -z "${OWNER}" ]]      && { echo "missing argument -o, --owner"; __usage; }
LUSTRE_DIR="$@"

# Make sure FSPACE is prefix of LUSTRE_DIR
[[ ! "${LUSTRE_DIR}" =~ ^${FSPACE}.* ]] && { echo "Filespace is not prefix of Lustre directory"; __usage; }

# Create list of files from query.
FILE_LIST=`${LTSMC_BIN} -f ${FSPACE} --query \
	     --servername ${SERVERNAME} -n ${NODE} \
	     --password ${PASSWORD} "${LUSTRE_DIR}/**" -v message \
    | awk '{gsub(/^fs:/, "", $6); gsub(/^hl:/, "", $7); gsub(/^ll:/, "", $8); print $6 $7 $8}'`

[[ -z ${FILE_LIST} ]] && { echo "no files found on TSM server"; exit 1; }
FILES_NUM_TOTAL=`echo "${FILE_LIST}" | wc -l`
FILE_COUNTER=0

for f in ${FILE_LIST}
do
    DIR=`dirname ${f}`
    if [[ ! -d ${DIR} ]]; then
	mkdir -p ${DIR} ||  exit 1
    fi
    if [[ ! -f ${f} ]]; then
	touch ${f} ||  exit 1
	sudo ${LFS_BIN} hsm_set --exists --archived ${f} || exit 1
	${LFS_BIN} hsm_release ${f} || exit 1
    else
	# (0x0000000d) released exists archived
	${LFS_BIN} hsm_state ${f} | awk '/(0x0000000d)/{f=1} /^Match/{exit} END{exit !f}'
	if [[ $? != 0 ]]; then
	    echo "file ${f} already exists, however with incorrect Lustre HSM flags"
	else
	    (( FILE_COUNTER++ ))
	fi
    fi
done

if [[ ${FILE_COUNTER} == ${FILES_NUM_TOTAL} ]]; then
    echo "successfully setup ${FILE_COUNTER} files from TSM server" && exit 0
else
    echo "warning, queried ${FILES_NUM_TOTAL}, but setup only ${FILE_COUNTER}" &&  exit 1
fi
