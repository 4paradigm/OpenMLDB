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

test -d /rambuild/ut_zookeeper && rm -rf /rambuild/ut_zookeeper/*
cp steps/zoo.cfg "$THIRDSRC/zookeeper-3.4.14/conf"
cd "$THIRDSRC/zookeeper-3.4.14"
# TODO(hw): macos no -p
if [[ "$OSTYPE" =~ ^darwin ]]; then
  lsof -ni | grep 6181 | awk '{print $2}'| xargs -I{} kill -9 {}
elif [[ "$OSTYPE" =~ ^linux ]]; then
  netstat -anp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs -I{} kill -9 {}
fi
./bin/zkServer.sh start && cd "$ROOT_DIR"
echo "zk started"
sleep 5
cd onebox && sh start_onebox.sh && cd "$ROOT_DIR"
echo "onebox started, check"
sleep 5
pgrep -f openmldb
echo "ROOT_DIR:${ROOT_DIR}"

cd "${ROOT_DIR}"/python/openmldb_sdk/dist/
whl_name_sdk=$(ls openmldb*.whl)
echo "whl_name_sdk:${whl_name_sdk}"
python3 -m pip install "${whl_name_sdk}[test]"

cd "${ROOT_DIR}"/python/openmldb_tool/dist/
whl_name_tool=$(ls openmldb*.whl)
echo "whl_name_tool:${whl_name_tool}"
python3 -m pip install "${whl_name_tool}[test]"

python3 -m pip install pytest-cov

cd "${ROOT_DIR}"/python/openmldb_sdk/tests
python3 -m pytest -vv --junit-xml=pytest.xml --cov=./ --cov-report=xml
cd "${ROOT_DIR}"/python/openmldb_tool/tests
python3 -m pytest -vv --junit-xml=pytest.xml --cov=./ --cov-report=xml

cd "${ROOT_DIR}"/onebox && ./stop_all.sh && cd "$ROOT_DIR"
cd "$THIRDSRC/zookeeper-3.4.14" && ./bin/zkServer.sh stop && cd "$ROOT_DIR"
