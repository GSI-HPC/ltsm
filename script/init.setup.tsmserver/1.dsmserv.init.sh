#!/bin/bash

OPT_SE="polaris-kvm-tsm-server"
OPT_ID="admin"
OPT_PW="admin"

OPT_MAC_FILE_1="1.dsmserv.lic.mac"
OPT_MAC_FILE_2="2.devc.mac"
OPT_MAC_FILE_3="3.stg.mac"
OPT_MAC_FILE_4="4.domain.mac"
OPT_MAC_FILE_5="5.policyset.mac"
OPT_MAC_FILE_6="6.mgmtclass.mac"
OPT_MAC_FILE_7="7.copygroup.mac"
OPT_MAC_FILE_8="8.assign.mac"
OPT_MAC_FILE_9="9.activate.mac"

DSMADMC_CMD="dsmadmc -se=${OPT_SE} -id=${OPT_ID} -password=${OPT_PW} -itemcommit macro"

# 1. Register licenses.
${DSMADMC_CMD} ${OPT_MAC_FILE_1}

# 2. Create device class.
${DSMADMC_CMD} ${OPT_MAC_FILE_2}

# 3. Create storage group.
# Make sure to define here the high/low migration threshold, as well,
# as the next storage pool where data is migrated.
${DSMADMC_CMD} ${OPT_MAC_FILE_3}

# 4. Create domain.
${DSMADMC_CMD} ${OPT_MAC_FILE_4}

# 5. Policy set.
${DSMADMC_CMD} ${OPT_MAC_FILE_5}

# 6. Management class.
${DSMADMC_CMD} ${OPT_MAC_FILE_6}

# 7. Copygroup.
# Make sure to define here the proper RET* settings.
${DSMADMC_CMD} ${OPT_MAC_FILE_7}

# 8. Assign management class and policyset.
${DSMADMC_CMD} ${OPT_MAC_FILE_8}

# 9. Activate management class and policyset.
${DSMADMC_CMD} ${OPT_MAC_FILE_9}
