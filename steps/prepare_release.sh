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

# Usage:
#  ./prepare_release.sh ${version-number}
#
# Requirements:
# - sed
# - java & maven
#
# supported {version-number} syntax is X.Y.Z[.{pre-prelease-identifier}] where it could be:
#  - semVer: X.Y.Z    # final release
#  - X.Y.Z.(a|alpha)N # alpha release
#  - X.Y.Z.(b|beta)N  # beta release
#  - X.Y.Z.(r|rc)N    # release candidate
#  NOTE: other version like 'X.Y.Z-rc1' may not work, don't use

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

set -eE

cd "$(dirname "$0")/.."
ROOT=$(pwd)
cmake_file="$ROOT/CMakeLists.txt"

VERSION=$1
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

# version in server
echo -e "${GREEN}setting native cpp version to $MAJOR.$MINOR.$BUG${NC}"
sed -i"" -e "s/OPENMLDB_VERSION_MAJOR .*/OPENMLDB_VERSION_MAJOR ${MAJOR})/g" "${cmake_file}"
sed -i"" -e "s/OPENMLDB_VERSION_MINOR .*/OPENMLDB_VERSION_MINOR ${MINOR})/g" "${cmake_file}"
sed -i"" -e "s/OPENMLDB_VERSION_BUG .*/OPENMLDB_VERSION_BUG ${BUG})/g" "${cmake_file}"

# tweak java sdk version
pushd java/
./prepare_release.sh "$VERSION"
popd

# tweak python sdk version
PY_VERSION=$VERSION
if [[ ${#ARR[@]} -gt 3 ]]; then
    # has {pre-prelease-identifier}
    # refer: https://www.python.org/dev/peps/pep-0440/#pre-releases
    PY_VERSION="${ARR[0]}.${ARR[1]}.${ARR[2]}${ARR[3]}"
fi

# version in python sdk
echo -e "${GREEN}setting py version to $PY_VERSION${NC}"
sed -i"" -e "s/version=.*/version='$PY_VERSION',/g" python/openmldb_sdk/setup.py
sed -i"" -e "s/version=.*/version='$PY_VERSION',/g" python/openmldb_tool/setup.py
