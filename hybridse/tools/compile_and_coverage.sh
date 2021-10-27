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

set -eE

# goto toplevel directory
# goto toplevel directory
pushd "$(dirname "$0")/../.."
OPENMLDB_DIR=$(pwd)
popd
# goto hybridse directory
pushd "$(dirname "$0")/.."

./tools/third-party.sh

if uname -a | grep -q Darwin; then
    # in case coreutils not install on mac
    alias nproc='sysctl -n hw.logicalcpu'
fi

rm -rf build
mkdir -p build
pushd build

cmake .. -DCMAKE_BUILD_TYPE=Debug -DCOVERAGE_ENABLE=ON -DTESTING_ENABLE=ON -DEXAMPLES_ENABLE=ON -DEXAMPLES_TESTING_ENABLE=ON
make -j"$(nproc)"
make -j"$(nproc)" coverage SQL_CASE_BASE_DIR="${OPENMLDB_DIR}" YAML_CASE_BASE_DIR="${OPENMLDB_DIR}"

popd

pushd java
# run java coverage via `jacoco:report`
if [[ "$OSTYPE" = "darwin"* ]]; then
    mvn prepare-package -P macos
elif [[ "$OSTYPE" = "linux-gnu" ]]; then
    mvn prepare-package
fi
popd

popd
