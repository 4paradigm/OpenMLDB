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
test -d /rambuild/ut_zookeeper && rm -rf /rambuild/ut_zookeeper/*
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.14/conf
cd thirdsrc/zookeeper-3.4.14 || exit
# TODO(hw): macos -p
netstat -anp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs kill -9
./bin/zkServer.sh start && cd "$ROOT_DIR" || exit
echo "zk started"
sleep 5
cd onebox && sh start_onebox.sh && cd "$ROOT_DIR" || exit
echo "onebox started, check"
sleep 5
ps axu | grep openmldb
echo "ROOT_DIR:${ROOT_DIR}"

cd "${ROOT_DIR}"/build/python/dist/ || exit
whl_name=$(ls openmldb*.whl)
echo "whl_name:${whl_name}"
python3 -m pip install "${whl_name}" -i https://pypi.tuna.tsinghua.edu.cn/simple

# needs: easy_install nose sqlalchemy
cd "${ROOT_DIR}"/python/sqlalchemy-test && nosetests --with-xunit

cd "${ROOT_DIR}"/onebox && sh stop_all.sh && cd "$ROOT_DIR" || exit
cd thirdsrc/zookeeper-3.4.14 || ./bin/zkServer.sh stop && cd "$ROOT_DIR" || exit
