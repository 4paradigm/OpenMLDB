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
# shellcheck disable=SC2206
ARR=(${VERSION//./ })
echo "splited version components: ${ARR[*]}"
if [[ "${#ARR[*]}" -lt 3 ]]; then
    echo -e "${RED}inputed version should have at least three number${NC}"
    exit 1
fi
MAJOR=${ARR[0]}
MINOR=${ARR[1]}
BUG=${ARR[2]}
NEW_BUG=$(($BUG+1))
NEW_VERSION="${MAJOR}.${MINOR}.${NEW_BUG}"

sed -i"" -e "s/OPENMLDB_VERSION=[0-9]\.[0-9]\.[0-9]/OPENMLDB_VERSION=${VERSION}/g" demo/Dockerfile
sed -i"" -e "s/SPARK_VERSION=[0-9]\.[0-9]\.[0-9]/SPARK_VERSION=${VERSION}/g" test/integration-test/openmldb-test-python/install.sh

sh steps/prepare_release.sh "${NEW_VERSION}.a0"
