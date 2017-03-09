#!/bin/bash

DSMADMC_BIN=`which dsmadmc` || { echo "cannot find administrative command line tool: dsmadmc"; exit 1; }
TSM_ID=${1-polaris}
TSM_NODE=${TSM_ID}
TSM_PASSWORD=${TSM_ID}
TSM_SERVERNAME=${2-polaris-kvm-tsm-server}

${DSMADMC_BIN} -displ=list -se=${TSM_SERVERNAME} -id=${TSM_ID} -pass=${TSM_PASSWORD} -outfile "quit"
[[ $? -ne 0 ]] && { exit 1; }

MAX_THREADS=`${DSMADMC_BIN} -displ=list -se=${TSM_SERVERNAME} -id=${TSM_ID} -pass=${TSM_PASSWORD} -outfile "q node ${TSM_NODE} f=d" | grep -oP '(?<=Maximum Mount Points Allowed: ).*'`

[[ -n ${MAX_THREADS+x} ]] && { echo -e "maximum mount points allowed is: ${MAX_THREADS}\nstart copytool lhsmtool_tsm with option --threads=${MAX_THREADS}"; exit 0; }
