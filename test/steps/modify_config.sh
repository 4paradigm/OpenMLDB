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


CASE_XML=$1
if [[ "${CASE_XML}" == "" ]]; then
    CASE_XML="test_all.xml"
fi
DEPLOY_MODE=$2
if [[ "${DEPLOY_MODE}" == "" ]]; then
    DEPLOY_MODE="cluster"
fi
echo "deploy_mode:${DEPLOY_MODE}"
ROOT_DIR=`pwd`
source steps/read_properties.sh
echo "test_version:$test_version"
cd java/hybridsql-test/fedb-sdk-test
system_type=`uname`
if [ ${system_type} == "Linux" ] ;then
    sed -i "s#<parameter name=\"version\" value=\".*\"/>#<parameter name=\"version\" value=\"${test_version}\"/>#"  test_suite/${CASE_XML}
else
    sed -i "" "s#<parameter name=\"version\" value=\".*\"/>#<parameter name=\"version\" value=\"${test_version}\"/>#" test_suite/${CASE_XML}
fi
if [ ${system_type} == "Linux" ] ;then
    sed -i "s#<parameter name=\"env\" value=\".*\"/>#<parameter name=\"env\" value=\"${DEPLOY_MODE}\"/>#"  test_suite/${CASE_XML}
else
    sed -i "" "s#<parameter name=\"env\" value=\".*\"/>#<parameter name=\"env\" value=\"${DEPLOY_MODE}\"/>#" test_suite/${CASE_XML}
fi
cd ${ROOT_DIR}