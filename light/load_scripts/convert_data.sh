#!/bin/bash

#
## convert each date of format yyyy-mm-dd to a number of format yyyymmddd
sed -i "s#|\([0-9][0-9][0-9][0-9]\)-\([0-9][0-9]\)-\([0-9][0-9]\)|#|\1\2\3|#g" "${LIGHT_DATA_DIR}/person${POSTFIX}"
#
## convert each datetime of format yyyy-mm-ddThh:mm:ss.mmm+0000
## to a number of format yyyymmddhhmmssmmm
sed -i "s#|\([0-9][0-9][0-9][0-9]\)-\([0-9][0-9]\)-\([0-9][0-9]\)T\([0-9][0-9]\):\([0-9][0-9]\):\([0-9][0-9]\)\.\([0-9][0-9][0-9]\)+0000#|\1\2\3\4\5\6\7#g" ${LIGHT_DATA_DIR}/*${POSTFIX}

sed -i '1d' ${LIGHT_DATA_DIR}/*${POSTFIX}
sed -i "s/,/+/g" ${LIGHT_DATA_DIR}/*${POSTFIX}
sed -i "s/|/,/g" ${LIGHT_DATA_DIR}/*${POSTFIX}
