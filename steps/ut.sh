#!/bin/bash

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

#
# ut.sh
#
WORK_DIR=$(pwd)

# on hybridsql 0.4.1 or later, 'THIRD_PARTY_SRC_DIR' is defined and is '/deps/src'
THIRDSRC=${THIRD_PARTY_SRC_DIR:-thirdsrc}

# shellcheck disable=SC2039
ulimit -c unlimited
mkdir -p reports
cp steps/zoo.cfg "$THIRDSRC/zookeeper-3.4.14/conf"
cd "$THIRDSRC/zookeeper-3.4.14" && ./bin/zkServer.sh start && cd "$WORK_DIR" || exit
sleep 5
TMPFILE="code.tmp"
echo 0 > $TMPFILE
if [ $# -eq 0 ];
then
    pushd build
    make test
    popd
else
    CASE_NAME=$1
    CASE_LEVEL=$2
    ROOT_DIR=$(pwd)
    echo "WORK_DIR: ${ROOT_DIR}"
    echo "sql c++ sdk test : case_level ${CASE_LEVEL}, case_file ${CASE_NAME}"
    HYBRIDSE_LEVEL=${CASE_LEVEL} YAML_CASE_BASE_DIR=${ROOT_DIR} "./build/bin/${CASE_NAME}" --gtest_output=xml:./reports/"${CASE_NAME}.xml" --minloglevel=2
    RET=$?
    echo "${CASE_NAME} result code is: $RET"
    if [ $RET -ne 0 ];then
        echo $RET > $TMPFILE
    fi
fi
code=$(cat $TMPFILE)
echo "code result: $code"
rm ${TMPFILE}
cd "$THIRDSRC/zookeeper-3.4.14" && ./bin/zkServer.sh stop
cd - || exit
exit "${code}"
