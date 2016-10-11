#!/bin/bash

# This is an initial sanity check script.
# However, it is terribly written and needs to be
# rewritten once I have more time.

# Environment variable DSMI_DIR to locate the dsm.sys file and DSMI_CONFIG to locate the client user-options file (default name dsm.opt).

# Exit on error.
set -e

__check_bin() {
    [[ ! -f "${1}" ]] && { echo "[ERROR]: Cannot find '${1}' binary"; exit 1; }

    return 0
}

__create_filelist() {
    [[ ! -n "${1}" ]] && { echo "[ERROR]: parameter path/files is not set"; exit 1; }

    LISTED_FILES=`find \`pwd\`/${1} -type f`
    [[ ! -n "${LISTED_FILES}" ]] && { echo "[ERROR]: no path/files was listed with 'find'"; exit 1; }

    return 0
}

__calc_md5sum() {
    for f in ${LISTED_FILES}; do
	# echo "${f}: `md5sum ${f}`"
	MD5_LOCAL_FILES[${f}]=`md5sum ${f}`
    done

    [[ ${#MD5_LOCAL_FILES[@]} -eq 0 ]] && { echo "[ERROR]: bash associative array MD5_LOCAL_FILES is empty"; exit 1; }

    return 0
}

__ltsm_archive() {
    echo "##### Archiving files #####"
    set -x
    ${LTSM_BIN} ${LTSM_VERBOSE} -a -f / -n ${LTSM_NODE} -p ${LTSM_PASSWORD} -s ${LTSM_SERVERNAME} ${1}
    set +x    
    echo "##### Archiving done #####"

    return 0
}

__ltsm_hl_ll_star() {
    local cmd=${1}
    local hl_param=${2}
    set -x
    ${LTSM_BIN} ${LTSM_VERBOSE} --${cmd} -f / --node ${LTSM_NODE} --password ${LTSM_PASSWORD} -s ${LTSM_SERVERNAME} --hl ${hl_param} --l "/*"
    set +x

    return 0
}

__ltsm_query_hl_ll() {
    echo "##### Querying hl/ll files #####"
    HL_PATH="`pwd`/${1}/*"
    HL_PATH="`find ${HL_PATH} -type d`"
    for d in ${HL_PATH}; do
	__ltsm_hl_ll_star "query" ${d}
    done
    echo "##### Querying hl/ll done #####"

    return 0
}    

__ltsm_retrieve_hl_ll() {
    echo "##### Retrieving hl/ll files #####"
    __ltsm_hl_ll_star "retrieve" ${1}
    echo "##### Retrieving hl/ll done #####"

    return 0
}

__ltsm_delete_hl_ll() {
    echo "##### Deleting hl/ll files #####"
    HL_PATH="`pwd`/${1}/*"
    HL_PATH="`find ${HL_PATH} -type d`"
    for d in ${HL_PATH}; do
	__ltsm_hl_ll_star "delete" ${d}
    done
    echo "##### Deleting hl/ll done #####"

    return 0
}

__ltsm_file() {
    local cmd=${1}
    local file=${2}
    set -x    
    ${LTSM_BIN} ${LTSM_VERBOSE} --${cmd} -f / --node ${LTSM_NODE} --password ${LTSM_PASSWORD} -s ${LTSM_SERVERNAME} ${file}
    set +x

    return 0
}

__ltsm_query_file() {
    echo "##### Querying files #####"
    for f in ${LISTED_FILES}; do
	__ltsm_file "query" ${f}
    done
    echo "##### Querying done #####"

    return 0
}

__ltsm_archive_file() {
    echo "##### Archiving files #####"
    for f in ${LISTED_FILES}; do
	__ltsm_file "archive" ${f}
    done
    echo "##### Archiving done #####"

    return 0
}

__ltsm_retrieve_file() {
    echo "##### Retrieving files #####"
    for f in ${LISTED_FILES}; do
	echo "$cmd $f"
	__ltsm_file "retrieve" ${f}
    done
    echo "##### Retrieving done #####"

    return 0
}

__ltsm_delete_file() {
    echo "##### Deleting files #####"
    for f in ${LISTED_FILES}; do
	__ltsm_file "delete" ${f}
    done
    echo "##### Deleting done #####"

    return 0
}

__verify_md5sum() {
    local has_mismatch=0
    for i in "${!MD5_LOCAL_FILES[@]}"; do
	MD5_FILE_RETRIEVED=`md5sum $i | awk '{print $1}'`
	MD5_FILE_ARCHIVE=`echo ${MD5_LOCAL_FILES[$i]} | awk '{print $1}'`
	[[ ${MD5_FILE_RETRIEVED} != ${MD5_FILE_ARCHIVE} ]] && { echo "MD5SUM mismatch on ${i}"; has_mismatch=1; }
	# echo "MD5_FILE_RETRIEVED: ${MD5_FILE_RETRIEVED} MD5_FILE_ARCHIVE: ${MD5_FILE_ARCHIVE}"
	# echo "key (filename):" $i
	# echo "value (filename + md5sum):" ${MD5_LOCAL_FILES[$i]}
    done

    if [ ${has_mismatch} == 0 ]; then
	echo "Successfully archived and retrieved data verified with MD5SUM"
    else
	{ echo "MD5SUM mismatch"; exit 1; }
    fi

    return 0
}

__test_ltsm_file() {
    local ARCHIVE_DIR=${1}

    __ltsm_archive_file
    __ltsm_query_file

    rm -rf ${ARCHIVE_DIR}

    __ltsm_retrieve_file
    __ltsm_delete_file

    return 0
}

__test_ltsm_hl_ll() {
    local ARCHIVE_DIR=${1}

    __ltsm_archive "${LISTED_FILES}"
    __ltsm_query_hl_ll ${ARCHIVE_DIR}

    rm -rf ${ARCHIVE_DIR}

    # Don't do that, that is uncool and ugly.
    RESTORE_DIR=`realpath .`/${ARCHIVE_DIR}/letters
    __ltsm_retrieve_hl_ll ${RESTORE_DIR}
    __ltsm_delete_hl_ll ${ARCHIVE_DIR}

    return 0
}

__create_letters() {
    local DIR_TEST_IMGS="${1}/letters/"
    local SIZE=${2}
    local IMG_FORMAT=${3} # Change format to e.g. ppm to create very large files.
    local WHICH_LETTERS=${4}

    echo "creating ${IMG_FORMAT} image letters ${WHICH_LETTERS}, please wait..."
    mkdir -p ${DIR_TEST_IMGS}
    for letter in {a..z}; do
	convert \
	    -size ${SIZE} \
	    xc:lightgray \
	    -font Bookman-DemiItalic \
	    -pointsize 1000 \
	    -fill blue \
	    -gravity center \
	    -draw "text 0,0 '${letter}'" \
	    ${DIR_TEST_IMGS}${letter}.big.${IMG_FORMAT}; \
    done
    echo "done"

    return 0
}

##############################################################
# main
##############################################################
TSM_NAME=${1-lxdv81}
LTSM_BIN="bin/ltsmc"
LTSM_NODE=${TSM_NAME}
LTSM_PASSWORD=${TSM_NAME}
LTSM_SERVERNAME=${2-lxdv81-kvm-tsm-server}
export DSMI_CONFIG=`pwd`/dsmopt/dsm.sys

make clean
DEBUG=1 VERBOSE=1 make

__check_bin "${LTSM_BIN}"
ARCHIVE_DIR="archives"

##############################################################
# Test suite for parametrize filenames, data Linux kernel sources.
##############################################################
LISTED_FILES=""
declare -A MD5_LOCAL_FILES

KERNEL_SRC_PACKAGE="${ARCHIVE_DIR}/linux-0.01.tar.gz"
if [ ! -f ${KERNEL_SRC_PACKAGE} ]; then
    wget --directory-prefix archives https://www.kernel.org/pub/linux/kernel/Historic/linux-0.01.tar.gz
fi
tar xvfz ${KERNEL_SRC_PACKAGE} -C ${ARCHIVE_DIR}
rm -rf ${KERNEL_SRC_PACKAGE}

__create_filelist ${ARCHIVE_DIR}
__calc_md5sum
__test_ltsm_file ${ARCHIVE_DIR}
__verify_md5sum
rm -rf ${ARCHIVE_DIR}

##############################################################
# Test suite for parametrize hl/ll, data generated with imagemagick.
##############################################################
unset LISTED_FILES
LISTED_FILES=""
unset MD5_LOCAL_FILES
declare -A MD5_LOCAL_FILES
__create_letters ${ARCHIVE_DIR} "256x256" "png" "{a..z}"
__create_filelist ${ARCHIVE_DIR}
__calc_md5sum
__test_ltsm_hl_ll ${ARCHIVE_DIR}
__verify_md5sum
rm -rf ${ARCHIVE_DIR}
