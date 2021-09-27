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


BATCH_VERSION=$1
BUILD_MODE=$2
ROOT_DIR=$(pwd)
# 从源码编译
if [[ "${BUILD_MODE}" == "SRC" ]]; then
    cd java/openmldb-batch || exit
    BATCH_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:3.1.0:evaluate -Dexpression=project.version -q -DforceStdout)
fi
echo "BATCH_VERSION:${BATCH_VERSION}"
cd test/batch-test/openmldb-batch-test || exit
# modify pom
sed -i "s#<openmldb.batch.version>.*</openmldb.batch.version>#<openmldb.batch.version>${BATCH_VERSION}</openmldb.batch.version>#" pom.xml

cd "${ROOT_DIR}" || exit
