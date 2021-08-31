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

#
# package.sh
#
set -e

pushd "$(dirname "$0")"

sdk_version=$1
cmake -H. -Bbuild -DSQL_JAVASDK_ENABLE=ON
cmake --build build --target sql_jsdk -- -j4

mkdir -p java/openmldb-native/src/main/resources/
test -f build/src/sdk/libsql_jsdk.dylib && cp build/src/sdk/libsql_jsdk.dylib  java/openmldb-native/src/main/resources/
test -f build/src/sdk/libsql_jsdk.so && cp build/src/sdk/libsql_jsdk.so  java/openmldb-native/src/main/resources/

pushd java/
if [ -n "$sdk_version" ] ; then
    mvn versions:set -DnewVersion="${sdk_version}"
fi
mvn deploy -Dmaven.test.skip=true
popd

popd
