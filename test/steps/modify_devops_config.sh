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
PRE_UPGRADE_VERSION=$2
OPENMLDB_SDK_VERSION=$3
TEST_CASE_VERSION=$4
OPENMLDB_SERVER_VERSION=$5
JAVA_NATIVE_VERSION=$6
TABLE_STORAGE_MODE=$7
echo "deploy_mode:${DEPLOY_MODE}"
ROOT_DIR=$(pwd)
echo "test_sdk_version:$OPENMLDB_SDK_VERSION"
cd test/integration-test/openmldb-test-java/openmldb-devops-test || exit
# modify suite_xml
if [[ "${PRE_UPGRADE_VERSION}" == "" ]]; then
  sed -i "s#<parameter name=\"version\" value=\".*\"/>#<parameter name=\"version\" value=\"${OPENMLDB_SERVER_VERSION}\"/>#"  test_suite/"${CASE_XML}"
else
  sed -i "s#<parameter name=\"version\" value=\".*\"/>#<parameter name=\"version\" value=\"${PRE_UPGRADE_VERSION}\"/>#"  test_suite/"${CASE_XML}"
  sed -i "s#<parameter name=\"upgradeVersion\" value=\".*\"/>#<parameter name=\"upgradeVersion\" value=\"${OPENMLDB_SERVER_VERSION}\"/>#"  test_suite/"${CASE_XML}"
fi

echo "devops test suite xml:"
cat test_suite/"${CASE_XML}"
cd "${ROOT_DIR}" || exit
cd test/integration-test/openmldb-test-java/openmldb-sdk-test || exit
# modify suite_xml
sed -i "s#<parameter name=\"version\" value=\".*\"/>#<parameter name=\"version\" value=\"${OPENMLDB_SERVER_VERSION}\"/>#"  test_suite/test_cluster.xml
sed -i "s#<parameter name=\"env\" value=\".*\"/>#<parameter name=\"env\" value=\"deploy\"/>#"  test_suite/test_cluster.xml

echo "test suite xml:"
cat test_suite/test_cluster.xml

if [ -n "${TEST_CASE_VERSION}" ]; then
  echo -e "\nversion=${TEST_CASE_VERSION}" >> src/main/resources/run_case.properties
fi
if [ -n "${TABLE_STORAGE_MODE}" ]; then
  sed -i "s#table_storage_mode=.*#table_storage_mode=${TABLE_STORAGE_MODE}#" src/main/resources/run_case.properties
fi
echo "run_case config:"
cat src/main/resources/run_case.properties
# modify pom
cd "${ROOT_DIR}" || exit
cd test/integration-test/openmldb-test-java/openmldb-test-common || exit
sed -i "s#<openmldb.jdbc.version>.*</oopenmldb.jdbc.version>#<openmldb.jdbc.version>${OPENMLDB_SDK_VERSION}</openmldb.jdbc.version>#" pom.xml
sed -i "s#<openmldb.navtive.version>.*</openmldb.navtive.version>#<openmldb.navtive.version>${JAVA_NATIVE_VERSION}</openmldb.navtive.version>#" pom.xml
echo "pom xml:"
cat pom.xml
cd "${ROOT_DIR}" || exit
