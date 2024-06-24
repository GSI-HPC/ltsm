#!/bin/bash

# Title       : ltsm.sh
# Date        : Mon 24 Jun 2024 09:57:18 AM CEST
# Version     : 0.1
# Author      : Thomas Stibor <t.stibor@gsi.de>
# Description : Wrapper script for lfs {hsm_archive, hsm_state} $@ where
#               archive ID is properly picked by the script. Note,
#               the archive ID is mapping to the TSM Node account, where
#               each TSM Account is representing an experiment (Linux) group.

# Required Lustre client utility executable.
LFS_BIN="/usr/bin/lfs"

ARCHIVE_ID=-1
GROUP_KEY=""
PERFORM_ARCHIVE=0
PERFORM_VERIFY=0

declare -rA GROUP_ID_MAP=(
    ["hades"]=1
    ["mbs"]=2
    ["tasca"]=3
    ["r3b"]=4
    # ["him"]=5
    ["nustar"]=6
    ["frs"]=7
    ["lascool"]=8
    ["litv_exp"]=9
    ["cry_exp"]=10
    ["stoe_exp"]=11
    ["hagm_exp"]=12
    ["despec"]=13
    ["cbm"]=14
    ["hadestest"]=15
    ["ship"]=16
    ["sfr"]=17
    ["hpc"]=18
)

__usage() {
    echo -e "usage: ${0}\n" \
	 "\t-a <Lustre directory>\n" \
	 "\t\t archive recursively all files in <Lustre directory>\n" \
	 "\t-v <Lustre directory>\n" \
	 "\t\t verify recursively whether all files in <Lustre directory> are archived\n" \
	 "\t-g {hades, mbs, tasca, ..., sfr, hpc}\n" \
	 "\t\t archive files using this group\n" \
	 "\t-l\n" \
	 "\t\t list all supported groups and associate archive id\n" \
	 "\t-h\n" \
	 "\t\t show this help" \
	 "\nexamples:\n" \
	 "\tarchive recursively all files in directory /lustre/hades/archive with group hades\n" \
	 "\t\$ ${0} -g hades -a /lustre/hades/archive\n" \
	 "\tnote, this equivalent to: find /lustre/hades/archive -type f -exec lfs hsm_archive --archive 1 {} \;\n\n" \
	 "\tverify recursively that all files in directory /lustre/hades/archive are archived\n" \
	 "\t\$ ${0} -v /lustre/hades/archive\n" \
	 "\tnote, this equivalent to: find /lustre/hades/archive -type f -exec lfs hsm_state {} \; | grep -v \"(0x00000009) exists archived\" \n\n" \
	 "\tcheck HSM operation progress of file /lustre/hades/archive_progress\n" \
	 "\t\$ lfs hsm_action /lustre/hades/archive_progress\n\n" \
	 "\tadditional details and listed in manual pages: lfs-hsm, lfs-hsm_state, lfs-hsm_action, lfs-hsm_set and lfs-hsm_clear\n"
	 exit 1
}

__check_bin() {
    [[ ! -f ${1} ]] && { echo "Error: cannot find executable file ${1}"; exit 1; }
}

__list_groups() {
    local SORTED_GROUP_ID_MAP

    for key in "${!GROUP_ID_MAP[@]}"; do
	SORTED_GROUP_ID_MAP+=("$key:${GROUP_ID_MAP[$key]}")
    done

    IFS=$'\n' SORTED_GROUP_ID_MAP=($(sort -n -t ':' -k 2,2 <<<"${SORTED_GROUP_ID_MAP[*]}"))
    unset IFS

    echo "supported groups and associated archive id's"
    for item in "${SORTED_GROUP_ID_MAP[@]}"; do
	key="${item%%:*}"
	value="${item##*:}"
	printf "group: %-10s -> archive id: %3d\n" ${key} ${value}
    done
}

#---------- main ----------#
__check_bin ${LFS_BIN}

# No options provided, show usage help.
[[ $# -eq 0 ]] && { __usage; }

while getopts "avlg:h" opt; do
    case ${opt} in
	a)
	    PERFORM_ARCHIVE=1
	    ;;
	v)
	    PERFORM_VERIFY=1
	    ;;
        l)
	    __list_groups
	    exit 0
            ;;
        g)
            GROUP_KEY=${OPTARG}
            ;;
        h)
            __usage
            ;;
	\?)
	    echo "Error: invalid option"
	    __usage
	    ;;
    esac
done

shift $((OPTIND - 1))
LUSTRE_PATH=("$@")

# Make sure archive and verify option are both not set.
[[ ${PERFORM_ARCHIVE} -eq 1 && ${PERFORM_VERIFY} -eq 1 ]] && { echo -e "error: archive -a and verify -v options cannot be set together"; exit 1; }

# Make sure GROUP_KEY is set together with archive option.
[[ ${PERFORM_ARCHIVE} -eq 1 && ! -n ${GROUP_KEY} ]] && { echo -e "error: missing group -g option for archive operation"; exit 1; }

# Make sure option group key exists in associative array GROUP_ID_MAP.
[[ ! ${GROUP_ID_MAP[${GROUP_KEY}]} ]] && { echo -e "error: group '${GROUP_KEY}' does not exist, list supported groups with option -l"; exit 1; }

# Make sure Lustre directory path provided in option ${LUSTRE_PATH} is valid.
# TODO: Verfiy via mount command that ${LUSTRE_PATH} is prefix if Lustre mount point.
[[ ! -d ${LUSTRE_PATH} ]] && { echo -e "error: ${LUSTRE_PATH} is not a valid directory"; exit 1; }

if [[ ${PERFORM_ARCHIVE} -eq 1 ]]
then
    find ${LUSTRE_PATH} -type f -exec lfs hsm_archive --archive ${GROUP} {} \;
elif [[ ${PERFORM_VERIFY} -eq 1 ]]
then
    find ${LUSTRE_PATH} -type f -exec lfs hsm_state {} \; | grep -v "(0x00000009) exists archived"
fi
