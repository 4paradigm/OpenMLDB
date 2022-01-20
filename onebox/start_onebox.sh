#!/bin/bash

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

set -x -e

cd "$(dirname "$0")/../"

ulimit -c unlimited

# first start zookeeper
IP=127.0.0.1

ZK_CLUSTER=$IP:6181
NS1=$IP:9622
NS2=$IP:9623
NS3=$IP:9624
TABLET1=$IP:9520
TABLET2=$IP:9521
TABLET3=$IP:9522

# start tablet0
test -d tablet0-binlogs && rm -rf tablet0-binlogs
test -d recycle_bin0 && rm -rf recycle_bin0
./build/bin/openmldb --db_root_path=tablet0-binlogs \
                   --recycle_bin_root_path=recycle_bin0 \
                   --endpoint=${TABLET1} --role=tablet \
                   --binlog_notify_on_put=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet0.log 2>&1 &
test -d tablet1-binlogs && rm -rf tablet1-binlogs
test -d recycle_bin1 && rm -rf recycle_bin1


# start tablet1
./build/bin/openmldb --db_root_path=tablet1-binlogs \
                   --recycle_bin_root_path=recycle_bin1 \
                   --endpoint=${TABLET2} --role=tablet \
                   --zk_cluster=${ZK_CLUSTER}\
                   --binlog_notify_on_put=true\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet1.log 2>&1 &
test -d tablet2-binlogs && rm -rf tablet2-binlogs
test -d recycle_bin2 && rm -rf recycle_bin2


# start tablet2
./build/bin/openmldb --db_root_path=tablet2-binlogs \
                   --recycle_bin_root_path=recycle_bin2 \
                   --endpoint=${TABLET3} --role=tablet \
                   --binlog_notify_on_put=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet2.log 2>&1 &

test -d recycle_bin3 && rm -rf recycle_bin3

# start ns1 
./build/bin/openmldb --endpoint=${NS1} --role=nameserver \
                   --zk_cluster=${ZK_CLUSTER}\
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --zk_root_path=/onebox > ns1.log 2>&1 &

# start ns2 
./build/bin/openmldb --endpoint=${NS2} --role=nameserver \
                   --zk_cluster=${ZK_CLUSTER}\
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --zk_root_path=/onebox > ns2.log 2>&1 &

# start ns3 
./build/bin/openmldb --endpoint=${NS3} --role=nameserver \
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_root_path=/onebox > ns3.log 2>&1 &

sleep 3

echo "start all ok"

