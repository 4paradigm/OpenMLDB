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

JAVA_SDK_VERSION=$1
OPENMLDB_SERVER_VERSION=$2
PYTHON_SDK_VERSION=$3
BATCH_VERSION=$4
DIFF_VERSIONS=$5

sed -i "s#JAVA_SDK_VERSION=.*#JAVA_SDK_VERSION=${JAVA_SDK_VERSION}#" test/steps/openmldb_test.properties
sed -i "s#JAVA_NATIVE_VERSION=.*#JAVA_NATIVE_VERSION=${JAVA_SDK_VERSION}#" test/steps/openmldb_test.properties
sed -i "s#OPENMLDB_SERVER_VERSION=.*#OPENMLDB_SERVER_VERSION=${OPENMLDB_SERVER_VERSION}#" test/steps/openmldb_test.properties
sed -i "s#PYTHON_SDK_VERSION=.*#PYTHON_SDK_VERSION=${PYTHON_SDK_VERSION}#" test/steps/openmldb_test.properties
sed -i "s#BATCH_VERSION=.*#BATCH_VERSION=${BATCH_VERSION}#" test/steps/openmldb_test.properties
sed -i "s#DIFF_VERSIONS=.*#DIFF_VERSIONS=${DIFF_VERSIONS}#" test/steps/openmldb_test.properties
cat test/steps/openmldb_test.properties
