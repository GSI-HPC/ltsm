PATH_PREFIX=`mktemp -d`
VERBOSE=0

__check_bin() {
    [[ ! -f "${1}" ]] && { echo "[ERROR]: Cannot find '${1}' binary"; exit 1; }

    return 0
}

__rnd_string()
{
    local L=${1}
    cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w ${1:-${L}} | head -n 1
}

__log() {
    [[ ${VERBOSE} -eq 1 ]] && echo "$@"

    return 0
}

__rnd_dirs()
{
    local MAX_DIR_DEPTH=${1}
    local DIR_LENGTH=${2}

    local D=$(( (RANDOM % ${MAX_DIR_DEPTH}) + 1 ))
    local P="${PATH_PREFIX}/"

    for d in $(seq 1 ${D}); do
	P+=$(__rnd_string ${DIR_LENGTH})
	P+="/"
    done

    echo ${P}
}

__rnd_files()
{
    # Create randomly at most ${1} files within the last nested directory (can also be 0).
    local MAX_NUM_FILES=${1}

    # Create randomly at most ${2} nested directories each have random file name of length ${3}.
    local RND_DIR=$(__rnd_dirs ${2} ${3})

    mkdir -p ${RND_DIR}
    __log "Create random nested directories: ${RND_DIR}"
    __log "Create ${F} file(s) with the last nested directory"

    local F=$(( (RANDOM % ${MAX_NUM_FILES}) ))
    local MAX_FILE_SIZE_KB=1024
    for f in $(seq 1 ${F}); do
	RND_FN=$(__rnd_string 4) # Create a random file name of length 4
	RND_FILE_SIZE=$(( (RANDOM % ${MAX_FILE_SIZE_KB}) + 1 ))

	__log "Create random file name: ${RND_FN} of size: ${RND_FILE_SIZE} KB in last directory"
	dd if=/dev/urandom of=${RND_DIR}/${RND_FN} bs=1K count=${RND_FILE_SIZE} > /dev/null 2>&1
    done
}
