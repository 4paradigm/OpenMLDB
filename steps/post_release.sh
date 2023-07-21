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


VERSION=$1
if [[ ! ${VERSION} =~ ^[0-9]\.[0-9]\.[0-9]$ ]]
then
    echo "invalid version ${VERSION}"
    exit 0
fi

cd "$(dirname "$0")/.."
ROOT=$(pwd)
cmake_file="$ROOT/CMakeLists.txt"

ARR=(${VERSION//./ })
echo "splited version components: ${ARR[*]}"
if [[ "${#ARR[*]}" -lt 3 ]]; then
    echo -e "${RED}inputed version should have at least three number${NC}"
    exit 1
fi

MAJOR=${ARR[0]}
MINOR=${ARR[1]}
BUG=${ARR[2]}

sed -i"" -e "s/OPENMLDB_VERSION=[0-9]\.[0-9]\.[0-9]/OPENMLDB_VERSION=${VERSION}/g" demo/Dockerfile
sed -i"" -e "s/SPARK_VERSION=[0-9]\.[0-9]\.[0-9]/SPARK_VERSION=${VERSION}/g" test/integration-test/openmldb-test-python/install.sh

# version in server
echo -e "${GREEN}setting native cpp version to $MAJOR.$MINOR.$BUG${NC}"
sed -i"" -e "s/OPENMLDB_VERSION_MAJOR .*/OPENMLDB_VERSION_MAJOR ${MAJOR})/g" "${cmake_file}"
sed -i"" -e "s/OPENMLDB_VERSION_MINOR .*/OPENMLDB_VERSION_MINOR ${MINOR})/g" "${cmake_file}"
sed -i"" -e "s/OPENMLDB_VERSION_BUG .*/OPENMLDB_VERSION_BUG ${BUG})/g" "${cmake_file}"

PY_VERSION=$VERSION
if [[ ${#ARR[@]} -gt 3 ]]; then
    # has {pre-prelease-identifier}
    # refer: https://www.python.org/dev/peps/pep-0440/#pre-releases
    PY_VERSION="${ARR[0]}.${ARR[1]}.${ARR[2]}${ARR[3]}"
fi

# version in python sdk
echo -e "${GREEN}setting py version to $PY_VERSION${NC}"
sed -i"" -e "s/version=.*/version='${PY_VERSION}a0',/g" python/openmldb_sdk/setup.py
sed -i"" -e "s/version=.*/version='${PY_VERSION}a0',/g" python/openmldb_tool/setup.py

sh java/prepare_release.sh "${VERSION}-SNAPSHOT"
