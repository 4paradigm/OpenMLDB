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

set -e

pushd "$(dirname "$0")/.."

VERSION=$1
make configure SQL_JAVASDK_ENABLE=ON
# the cp_native_so will do the copy so job
cmake --build build --target cp_native_so -- -j4

pushd java/
BASE_PATTERN="^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*"

if [[ ! $VERSION =~ $BASE_PATTERN ]]; then
  VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
  echo "get version in pom: $VERSION"
fi

# tweak VERSION based on rules:
#  - 0.2.2     -> 0.2.2
#  - 0.2.2(.*) -> 0.2.2-SNAPSHOT

# shellcheck disable=SC2001
SUFFIX_VERSION=$(echo "$VERSION" | sed -e "s/${BASE_PATTERN}//")
BASE_VERSION=${VERSION%"$SUFFIX_VERSION"}
VERSION=${BASE_VERSION}
if [ -f openmldb-native/src/main/resources/libsql_jsdk.dylib ]; then
  VERSION="${VERSION}-macos"
fi

if [ -n "$SUFFIX_VERSION" ]; then
  VERSION="${VERSION}-SNAPSHOT"
fi

echo "set version: ${VERSION}"

mvn versions:set -DnewVersion="${VERSION}"
mvn deploy -Dmaven.test.skip=true
popd

popd
