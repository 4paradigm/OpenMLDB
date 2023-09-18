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

set -ex

ROOT_DIR=$(pwd)

# on hybridsql 0.4.1 or later, 'THIRD_PARTY_SRC_DIR' is defined and is '/deps/src'
THIRDSRC=${THIRD_PARTY_SRC_DIR:-thirdsrc}

bash steps/ut_zookeeper.sh reset
sleep 5
bash onebox/start_onebox.sh
echo "onebox started, check"
sleep 5
pgrep -f openmldb
echo "ROOT_DIR:${ROOT_DIR}"

# debug
python3 -m pip --version

cd "${ROOT_DIR}"/python/openmldb_sdk/dist/
whl_name_sdk=$(ls openmldb*.whl)
echo "whl_name_sdk:${whl_name_sdk}"
python3 -m pip install "${whl_name_sdk}[test]"

cd "${ROOT_DIR}"/python/openmldb_tool/dist/
whl_name_tool=$(ls openmldb*.whl)
echo "whl_name_tool:${whl_name_tool}"
# pip 23.1.2 just needs to install test(rpc is required by test)
python3 -m pip install "${whl_name_tool}[rpc,test]"

python3 -m pip install pytest-cov

cd "${ROOT_DIR}"/python/openmldb_sdk/tests
python3 -m pytest -vv --junit-xml=pytest.xml --cov=./ --cov-report=xml
cd "${ROOT_DIR}"/python/openmldb_tool/tests
python3 -m pytest -vv --junit-xml=pytest.xml --cov=./ --cov-report=xml

cd "${ROOT_DIR}"/onebox && ./stop_all.sh && cd "$ROOT_DIR"
cd "$THIRDSRC/zookeeper-3.4.14" && ./bin/zkServer.sh stop && cd "$ROOT_DIR"
