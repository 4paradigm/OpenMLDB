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

MODE="cluster"
if [ $# -gt 0 ]; then
    MODE=$1
fi
pkill mon
pkill python3
pkill java
rm -rf /tmp/*
sleep 2
if [[ "$MODE" = "standalone" ]]; then
    sed -i "s/.*zk_cluster=.*/#--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/nameserver.flags
    sed -i "s/.*zk_root_path=.*/#--zk_root_path=\/openmldb/g" /work/openmldb/conf/nameserver.flags
    sed -i "s/.*zk_cluster=.*/#--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/tablet.flags
    sed -i "s/.*zk_root_path=.*/#--zk_root_path=\/openmldb/g" /work/openmldb/conf/tablet.flags
    sed -i "s/.*zk_cluster=.*/#--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/apiserver.flags
    sed -i "s/.*zk_root_path=.*/#--zk_root_path=\/openmldb/g" /work/openmldb/conf/apiserver.flags
    python3 convert_data.py < data/taxi_tour_table_train_simple.csv  > ./data/taxi_tour.csv
    cd /work/openmldb && sh bin/start-all.sh
    sleep 1
else
    sed -i "s/.*zk_cluster=.*/--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/nameserver.flags
    sed -i "s/.*zk_root_path=.*/--zk_root_path=\/openmldb/g" /work/openmldb/conf/nameserver.flags
    sed -i "s/.*system_table_replica_num=.*/--system_table_replica_num=1/g" /work/openmldb/conf/nameserver.flags
    sed -i "s/.*zk_cluster=.*/--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/tablet.flags
    sed -i "s/.*zk_root_path=.*/--zk_root_path=\/openmldb/g" /work/openmldb/conf/tablet.flags
    sed -i "s/.*zk_cluster=.*/--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/apiserver.flags
    sed -i "s/.*zk_root_path=.*/--zk_root_path=\/openmldb/g" /work/openmldb/conf/apiserver.flags
    cd /work/zookeeper-3.4.14 && ./bin/zkServer.sh restart
    sleep 1
    cd /work/openmldb && ./bin/start.sh start tablet
    sleep 1
    cd /work/openmldb && ./bin/start.sh start nameserver
    sleep 1
    cd /work/openmldb && ./bin/start.sh start apiserver
    sleep 1
    cd /work/openmldb/taskmanager/bin && sh taskmanager.sh &
    sleep 1
fi
