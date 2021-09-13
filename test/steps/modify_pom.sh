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


ROOT_DIR=$(pwd)

javaSDKVersion=$(more fedb/src/sdk/java/pom.xml | grep "<version>.*</version>" | head -1 | sed 's#.*<version>\(.*\)</version>.*#\1#')
echo "javaSDKVersion:${javaSDKVersion}"
sed -i "s#<sql.version>.*</sql.version>#<sql.version>${javaSDKVersion}</sql.version>#" java/hybridsql-test/fedb-sdk-test/pom.xml

sed -i "s#<parameter name=\"fedbPath\" value=\".*\"/>#<parameter name=\"fedbPath\" value=\"${ROOT_DIR}/fedb/build/bin/fedb\"/>#" test_suite/"${CASE_XML}"
