#!/bin/bash

source $(dirname $(readlink -f $0))/include.sh

# Exit on error.
set -e
VERBOSE=1

__rnd_files()
{
    # Create randomly at most ${1} files in directory ${2}
    local NUM_FILES=${1}
    local DIR=${2}
    local MIN_KB_SIZE=${3}
    local MAX_KB_SIZE=${4}
    
    mkdir -p ${DIR}
    __log "Create ${NUM_FILES} file(s) in directory ${DIR}"

    for f in $(seq 1 ${NUM_FILES}); do
	RND_FN=$(__rnd_string 4) # Create a random file name of length 4
	RND_FILE_SIZE=`shuf -i ${MIN_KB_SIZE}-${MAX_KB_SIZE} -n 1`

	__log "Create random file name: ${RND_FN} of size: ${RND_FILE_SIZE} KB in directory ${DIR}"
	dd if=/dev/urandom of=${DIR}/${RND_FN} bs=1K count=${RND_FILE_SIZE} > /dev/null 2>&1
    done
}

__hsm_match_state()
{
    for f in `find ${1} -type f`
    do
	if lfs hsm_state ${f} | grep -vq "${2}$"
	then
	    echo "[ERROR]: file ${f} is not in correct HSM state ${2}"
	    return 1;
	fi
    done
    return 0;
}

__hsm_match_and_wait()
{
    declare -A L_FILES
    for f in `find ${1} -type f`
    do
	L_FILES[${f}]=0
    done

    N_FILES=`find ${1} -type f | wc -l`
    N_FILES=$((N_FILES + 0))  # Convert to integer
    N_FINISHED=0

    echo -n "waiting to finish operation"
    while [[ ${N_FINISHED} -lt ${N_FILES} ]]
    do
	for f in `find ${1} -type f`
	do
	    if lfs hsm_action ${f} | grep -q "NOOP$" && lfs hsm_state ${f} | grep -q "${2}$"
	    then
		if [[ ${L_FILES[${f}]} -eq 0 ]]
		then
		    L_FILES[${f}]=1		    
		    N_FINISHED=$((N_FINISHED+1))
		fi
	    fi
	done
	echo -n "."
    done
    echo -e "\ndone\n"
}

##########################################################
# main
##########################################################
TSM_NAME=${1-polaris}
LHSMTOOL_TSM_BIN="src/lhsmtool_tsm"

PATH_LUSTRE_MOUNTPOINT='/lustre'
PATH_DIR=${PATH_LUSTRE_MOUNTPOINT}`mktemp -d`
NUM_FILES=30
MIN_KB_SIZE=1
MAX_KB_SIZE=8196

[ ${PWD##*/} == "script" ] && { LHSMTOOL_TSM_BIN="../${LHSMTOOL_TSM_BIN}"; }
__check_bin "${LHSMTOOL_TSM_BIN}"

echo "Creating sanity data in ${PATH_DIR} please wait ..."

##########################################################
# Create randomly generated file names and file sizes.
##########################################################
__rnd_files ${NUM_FILES} ${PATH_DIR} ${MIN_KB_SIZE} ${MAX_KB_SIZE}

echo "Total number of files            : `find ${PATH_DIR} -type f | wc -l`"
echo "Total number of empty files      : `find ${PATH_DIR} -type f -empty | wc -l`"
echo "Total number of bytes            : `ls -FaGl ${PATH_DIR} | awk '{ total += $4; }; END { print total }'`"

MD5_SUFFIX_NAME=`mktemp -u --tmpdir=. | awk -F "." '{print $3}'`
MD5_ORIG="/tmp/md5orig.${MD5_SUFFIX_NAME}"
MD5_RETR="/tmp/md5retr.${MD5_SUFFIX_NAME}"
rm -rf ${MD5_ORIG} ${MD5_RETR}

echo "Creating MD5 sum file of original data: ${MD5_ORIG}"
find ${PATH_DIR} -exec md5sum -b '{}' \; &> ${MD5_ORIG}

# Clean state
STRDEF_EMPTY="(0x00000000)"
__hsm_match_state ${PATH_DIR} ${STRDEF_EMPTY}

# Archived state
echo "archiving files in ${PATH_DIR}/*"
STRDEF_ARCHIVE_EXISTS="(0x00000009) exists archived, archive_id:1"
lfs hsm_archive ${PATH_DIR}/*
__hsm_match_and_wait ${PATH_DIR} "${STRDEF_ARCHIVE_EXISTS}"
__hsm_match_state ${PATH_DIR} "${STRDEF_ARCHIVE_EXISTS}"


# Released state
echo "releasing files in ${PATH_DIR}/*"
STRDEF_RELEASED_EXISTS="(0x0000000d) released exists archived, archive_id:1"
lfs hsm_release ${PATH_DIR}/*
__hsm_match_and_wait ${PATH_DIR} "${STRDEF_RELEASED_EXISTS}"
__hsm_match_state ${PATH_DIR} "${STRDEF_RELEASED_EXISTS}"

# Restored state
echo "restoring files in ${PATH_DIR}/*"
lfs hsm_restore ${PATH_DIR}/*
__hsm_match_and_wait ${PATH_DIR} "${STRDEF_ARCHIVE_EXISTS}"
__hsm_match_state ${PATH_DIR} "${STRDEF_ARCHIVE_EXISTS}"

echo "Creating MD5 sum file of retrieved data: ${MD5_RETR}"
find ${PATH_DIR} -exec md5sum -b '{}' \; &> ${MD5_RETR}

# Delete state
echo "deleting files in ${PATH_DIR}/*"
STRDEF_EMPTY_ARCHIVE="(0x00000000), archive_id:1"
lfs hsm_remove ${PATH_DIR}/*
__hsm_match_and_wait ${PATH_DIR} "${STRDEF_EMPTY_ARCHIVE}"
__hsm_match_state ${PATH_DIR} "${STRDEF_EMPTY_ARCHIVE}"

# Check for equality
ARE_EQUAL=1 # FALSE
if diff -q ${MD5_ORIG} ${MD5_RETR}; then
    ARE_EQUAL=0
fi

# Final result and output
if [[ ${ARE_EQUAL} -eq 0 ]] ; then
    echo "Sanity successfully finished, archived and retrieved data match."
    rm -rf ${MD5_ORIG} ${MD5_RETR} ${PATH_DIR}
else
    echo "Sanity failed, archived and retrieved data does not match."
    echo "See MD5 checksum files '${MD5_ORIG}' and '${MD5_RETR}'"
    rm -rf ${PATH_DIR} # Keep the md5sum files to figure out what is going on.
fi

exit ${ARE_EQUAL}
