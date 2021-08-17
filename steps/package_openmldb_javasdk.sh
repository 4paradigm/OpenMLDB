#! /bin/sh

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
WORKDIR=$(pwd)
set -e
if [ -z "$1" ]; then
    echo "Usage: $0 \$VERSION. error: version number required"
    exit 1
fi
sdk_version=$1
mkdir -p build && cd build &&  cmake -DSQL_JAVASDK_ENABLE=ON .. && make -j4 sql_jsdk

cd "${WORKDIR}"
mkdir -p java/sql-native/src/main/resources/
test -f build/src/sdk/libsql_jsdk.dylib && cp build/src/sdk/libsql_jsdk.dylib  java/sql-native/src/main/resources/
test -f build/src/sdk/libsql_jsdk.so && cp build/src/sdk/libsql_jsdk.so  java/sql-native/src/main/resources/
cd java/ &&  mvn versions:set -DnewVersion="${sdk_version}" && mvn deploy -Dmaven.test.skip=true
