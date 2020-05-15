#!/usr/bin/env bash

SCRIPT_DIR=$(cd $(dirname $0); pwd)

INPUT=$1
if [[ ${INPUT} == "" ]]; then
    echo "No application log file specified"
    exit 1
fi


if [[ -f ${INPUT} ]]; then
    LOG_FILE_NAME=${INPUT}

elif [[ $(echo ${INPUT} | grep -q "application_") -eq 0 ]]; then
    APP_ID=${INPUT}
    mkdir -p logs
    cd logs
    LOG_FILE_NAME=${APP_ID}.log
    yarn logs -size_limit_mb=-1 -applicationId=${APP_ID} > ${LOG_FILE_NAME}

else
    echo "Input should either be log file or yarn application id"
    exit
fi


cat ${LOG_FILE_NAME} | grep "ConsoleOutputReporter - Stacktrace:" | awk '{print substr($0,37)}' > ${LOG_FILE_NAME}.json
python ${SCRIPT_DIR}/flame/stackcollapse.py  -i ${LOG_FILE_NAME}.json > ${LOG_FILE_NAME}.folded
${SCRIPT_DIR}/flame/flamegraph.pl  ${LOG_FILE_NAME}.folded > ${LOG_FILE_NAME}.svg
rm ${LOG_FILE_NAME}.folded ${LOG_FILE_NAME}.json
echo "Output flamegraph to $(pwd)/${LOG_FILE_NAME}.svg"

