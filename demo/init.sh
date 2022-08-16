#!/bin/bash
#
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

# init.sh
set -ex

MODE="cluster"
if [ $# -gt 0 ]; then
    MODE=$1
fi
# predict server may be running
set +e
pkill python3
set -e

rm -rf /tmp/openmldb_offline_storage/*
rm -rf /work/openmldb/logs*
rm -rf /work/openmldb/db*
rm -rf /work/openmldb/taskmanager/bin/logs
sleep 2
echo "Starting openmldb in $MODE mode..."
if [[ "$MODE" = "standalone" ]]; then
    python3 /work/taxi-trip/convert_data.py < /work/taxi-trip/data/taxi_tour_table_train_simple.csv  > /work/taxi-trip/data/taxi_tour.csv
    cd /work/openmldb && ./bin/stop-standalone.sh && ./bin/start-standalone.sh
    sleep 1
else
    cd /work/zookeeper-3.4.14 && ./bin/zkServer.sh stop && rm -rf /tmp/zookeeper && ./bin/zkServer.sh start
    sleep 1
    cd /work/openmldb && ./bin/stop-all.sh && ./bin/start-all.sh
fi

echo "Started"
