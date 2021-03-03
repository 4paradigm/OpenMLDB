#!/usr/bin/env bash

# java/bin/drawflame.sh
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

