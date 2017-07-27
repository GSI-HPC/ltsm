#!/bin/bash

source $(dirname $(readlink -f $0))/include.sh

# Exit on error.
set -e
VERBOSE=0

##########################################################
# main
##########################################################
TSM_NAME=${1-polaris}
LTSM_BIN="src/ltsmc"
LTSM_NODE=${TSM_NAME}
LTSM_PASSWORD=${TSM_NAME}
LTSM_SERVERNAME=${2-polaris-kvm-tsm-server}
LTSM_VERBOSE=${3-message}
LTSM_FS=${4-/tmp}
LTSM_OWNER=${5-testowner}

[ ${PWD##*/} == "script" ] && { LTSM_BIN="../${LTSM_BIN}"; }
__check_bin "${LTSM_BIN}"

PATH_PREFIX=${LTSM_FS}${PATH_PREFIX}
echo "Creating sanity data in ${PATH_PREFIX} please wait ..."

##########################################################
# Create directories and files
##########################################################
MAX_NUM_FILES=35
MAX_NESTED_DIRS=16
MAX_DIR_LEN=6
for r in $(seq 1 5); do
    __rnd_files ${MAX_NUM_FILES} ${MAX_NESTED_DIRS} ${MAX_DIR_LEN}
done

##########################################################
# Create directories with empty files
##########################################################
MAX_NUM_FILES=10
MAX_NESTED_DIRS=5
MAX_DIR_LEN=7
MAX_DIRS=4
for r in $(seq 1 ${MAX_DIRS}); do
    DIR=$(__rnd_dirs ${MAX_NESTED_DIRS} ${MAX_DIR_LEN})
    mkdir -p ${DIR}

    MAX_EMPTY_FILES=$(( (RANDOM % ${MAX_NUM_FILES}) + 1 ))
    __log "Create ${MAX_EMPTY_FILES} files"
    for i in $(seq 1 ${MAX_EMPTY_FILES}); do
	FILE=$(__rnd_string 4)
	touch ${DIR}/${FILE}
	__log "Create empty file: ${DIR}/${FILE}"
    done
done

##########################################################
# Create empty directories
##########################################################
for r in $(seq 1 ${MAX_DIRS}); do
    DIR=$(__rnd_dirs ${MAX_NESTED_DIRS} ${MAX_DIR_LEN})
    mkdir -p ${DIR}
done

echo "Total number of directories      : `find ${PATH_PREFIX} -type d | wc -l`"
echo "Total number of empty directories: `find ${PATH_PREFIX} -type d -empty | wc -l`"
echo "Total number of files            : `find ${PATH_PREFIX} -type f | wc -l`"
echo "Total number of empty files      : `find ${PATH_PREFIX} -type f -empty | wc -l`"

MD5_ORIG="/tmp/md5orig.txt"
MD5_RETR="/tmp/md5retr.txt"

echo "Creating MD5 sum file of original data: ${MD5_ORIG}"
find ${PATH_PREFIX} -exec md5sum -b '{}' \; &> ${MD5_ORIG}

##########################################################
# LTSM actions
##########################################################
# Archive data
echo "Archiving data please wait ..."
${LTSM_BIN} --verbose ${LTSM_VERBOSE} --archive -r -f ${LTSM_FS} -n ${LTSM_NODE} -p ${LTSM_PASSWORD} -o ${LTSM_OWNER} -s ${LTSM_SERVERNAME} "${PATH_PREFIX}"
[ $? -eq 0 ] && { echo -e "done\n"; }

# First remove data locally, second retrieve data from TSM storage.
echo "Deleting data locally in ${PATH_PREFIX} and retrieving data from TSM storage"
rm -rf ${PATH_PREFIX}
${LTSM_BIN} --verbose ${LTSM_VERBOSE} --retrieve -f ${LTSM_FS} -n ${LTSM_NODE} -p ${LTSM_PASSWORD} -o ${LTSM_OWNER} -s ${LTSM_SERVERNAME} "${PATH_PREFIX}*/*"
[ $? -eq 0 ] && { echo -e "done\n"; }

echo "Creating MD5 sum file of retrieved data: ${MD5_RETR}"
find ${PATH_PREFIX} -exec md5sum -b '{}' \; &> ${MD5_RETR}

# Finally remove data locally and also from TSM storage.
rm -rf ${PATH_PREFIX}
${LTSM_BIN} --verbose ${LTSM_VERBOSE} --delete -f ${LTSM_FS} -n ${LTSM_NODE} -p ${LTSM_PASSWORD} -o ${LTSM_OWNER} -s ${LTSM_SERVERNAME} "${PATH_PREFIX}*/*"
[ $? -eq 0 ] && { echo -e "done\n"; }

# Check for equality
ARE_EQUAL=1 # FALSE
if diff -q ${MD5_ORIG} ${MD5_RETR}; then
    ARE_EQUAL=0
fi

# Final result and output
if [ ${ARE_EQUAL} -eq 0 ] ; then
    echo "Sanity successfully finished, archived and retrieved data match."
    rm -rf ${MD5_ORIG} ${MD5_RETR} ${PATH_PREFIX}
else
    echo "Sanity failed, archived and retrieved data does not match."
    rm -rf ${PATH_PREFIX} # Keep the md5sum files to figure out what is going on.
fi

exit ${ARE_EQUAL}
