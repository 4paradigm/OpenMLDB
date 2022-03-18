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
DEPLOY_MODE=$2
FEDB_SDK_VERSION=$3
BUILD_MODE=$4
FEDB_SERVER_VERSION=$4
JAVA_NATIVE_VERSION=$5
echo "deploy_mode:${DEPLOY_MODE}"
ROOT_DIR=$(pwd)
echo "test_version:$FEDB_SDK_VERSION"
cd test/integration-test/openmldb-test-java/openmldb-sdk-test || exit
# modify suite_xml
sed -i "s#<parameter name=\"version\" value=\".*\"/>#<parameter name=\"version\" value=\"${FEDB_SERVER_VERSION}\"/>#"  test_suite/"${CASE_XML}"
sed -i "s#<parameter name=\"env\" value=\".*\"/>#<parameter name=\"env\" value=\"${DEPLOY_MODE}\"/>#"  test_suite/"${CASE_XML}"
if [[ "${BUILD_MODE}" == "SRC" ]]; then
    sed -i "s#<parameter name=\"fedbPath\" value=\".*\"/>#<parameter name=\"fedbPath\" value=\"${ROOT_DIR}/build/bin/openmldb\"/>#" test_suite/"${CASE_XML}"
fi
# modify pom
sed -i "s#<openmldb.jdbc.version>.*</oopenmldb.jdbc.version>#<openmldb.jdbc.version>${FEDB_SDK_VERSION}</openmldb.jdbc.version>#" pom.xml
sed -i "s#<openmldb.navtive.version>.*</openmldb.navtive.version>#<openmldb.navtive.version>${JAVA_NATIVE_VERSION}</openmldb.navtive.version>#" pom.xml
cd "${ROOT_DIR}" || exit
