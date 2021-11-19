#! /bin/bash

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
# prepare_release.sh
#

set -ex

cd "$(dirname "$0")/.."
ROOT=$(pwd)
cmake_file="$ROOT/CMakeLists.txt"

VERSION=$1
# shellcheck disable=SC2206
ARR=(${VERSION//./ })
echo "${ARR[*]}"
if [ "${#ARR[*]}" != 3 ]; then
    echo "invalid version"
    exit 1
fi

MAJOR=${ARR[0]}
MINOR=${ARR[1]}
BUG=${ARR[2]}

# version in server
sed -i"" -e "s/OPENMLDB_VERSION_MAJOR .*/OPENMLDB_VERSION_MAJOR ${MAJOR})/g" "${cmake_file}"
sed -i"" -e "s/OPENMLDB_VERSION_MINOR .*/OPENMLDB_VERSION_MINOR ${MINOR})/g" "${cmake_file}"
sed -i"" -e "s/OPENMLDB_VERSION_BUG .*/OPENMLDB_VERSION_BUG ${BUG})/g" "${cmake_file}"

# version in python sdk
sed -i"" -e "s/version=.*/version='$VERSION',/g" python/sqlalchemy-openmldb/setup.py
