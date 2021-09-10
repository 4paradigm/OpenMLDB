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
sh steps/clone-fedb.sh
cd OpenMLDB
ls -al
sh "${ROOT_DIR}"/steps/retry-command.sh "bash steps/init_env.sh"
mkdir -p build
source /root/.bashrc && cd build && cmake -DSQL_PYSDK_ENABLE=ON -DSQL_JAVASDK_ENABLE=ON -DTESTING_ENABLE=ON .. && make -j$(nproc)
make sqlalchemy_fedb && cd ../
cd src/sdk/java
mvn clean install -Dmaven.test.skip=true
