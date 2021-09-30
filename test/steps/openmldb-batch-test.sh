#!/usr/bin/env bash

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

while getopts ":b:c:d:l:" opt
do
   case $opt in
        b)
        echo "参数b的值:$OPTARG"
        BUILD_MODE=$OPTARG
        ;;
        ?) echo "未知参数"
           exit 1
        ;;
   esac
done
if [[ "${BUILD_MODE}" == "" ]]; then
    BUILD_MODE="PKG"
fi
echo "BUILD_MODE:${BUILD_MODE}"
ROOT_DIR=$(pwd)
source test/steps/read_properties.sh
# 从源码编译
if [[ "${BUILD_MODE}" == "SRC" ]]; then
    cd java/openmldb-batch || exit
    mvn clean install -Dmaven.test.skip=true -Dgpg.skip
    cd "${ROOT_DIR}" || exit
fi
# install command tool
cd test/test-tool/command-tool || exit
mvn clean install -Dmaven.test.skip=true
cd "${ROOT_DIR}" || exit

# modify config
sh test/steps/modify_batch_config.sh "${BATCH_VERSION}" "${BUILD_MODE}"

cd "${ROOT_DIR}"/test/integration-test/openmldb-test-java || exit
mvn clean install -Dmaven.test.skip=true

cd "${ROOT_DIR}"/test/batch-test/openmldb-batch-test/ || exit
mvn clean test
