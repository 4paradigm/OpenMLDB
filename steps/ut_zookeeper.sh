#!/bin/sh

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

WORK_DIR=$(pwd)

if [ $# -ne 1 ]; then
    echo "./ut_zookeeper.sh [start|stop|reset]"
    exit 1
fi

OP=$1
# on hybridsql 0.4.1 or later, 'THIRD_PARTY_SRC_DIR' is defined and is '/deps/src'
THIRDSRC=${THIRD_PARTY_SRC_DIR:-thirdsrc}

case $OP in
    start)
        echo "Starting zk ... "
        cp steps/zoo.cfg "$THIRDSRC/zookeeper-3.4.14/conf/"
        cd "$THIRDSRC/zookeeper-3.4.14" && ./bin/zkServer.sh start && cd "$WORK_DIR" || exit
        sleep 5
        echo "start zk succeed"
        ;;
    stop)
        echo "Stopping zk ... "
        cd "$THIRDSRC/zookeeper-3.4.14" && ./bin/zkServer.sh stop
        ;;
    reset)
        echo "remove zk data and then start zk"
        rm -rf "$THIRDSRC/zookeeper-3.4.14/data"
        sh steps/ut_zookeeper.sh start
        ;;
    *)
        echo "Only support {start|stop}" >&2
esac
