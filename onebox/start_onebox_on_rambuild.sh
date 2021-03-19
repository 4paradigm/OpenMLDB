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

#! /bin/sh
#
ulimit -c unlimited
# start_onebox.sh

# first start zookeeper
IP=127.0.0.1

ZK_CLUSTER=$IP:6181
NS1=$IP:9622
NS2=$IP:9623
NS3=$IP:9624
TABLET1=$IP:9520
TABLET2=$IP:9521
TABLET3=$IP:9522
BLOB1=$IP:9720

../build/bin/fedb --db_root_path=/rambuild/tablet0-binlogs \
                   --recycle_bin_root_path=/rambuild/recycle_bin0 \
                   --endpoint=${TABLET1} --role=tablet \
                   --binlog_notify_on_put=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet0.log 2>&1 &

# start tablet1
../build/bin/fedb --db_root_path=/rambuild/tablet1-binlogs \
                   --recycle_bin_root_path=/rambuild/recycle_bin1 \
                   --endpoint=${TABLET2} --role=tablet \
                   --zk_cluster=${ZK_CLUSTER}\
                   --binlog_notify_on_put=true\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet1.log 2>&1 &

# start tablet2
../build/bin/fedb --db_root_path=/rambuild/tablet2-binlogs \
                   --recycle_bin_root_path=/rambuild/recycle_bin2 \
                   --endpoint=${TABLET3} --role=tablet \
                   --binlog_notify_on_put=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet2.log 2>&1 &

# start ns1
../build/bin/fedb --endpoint=${NS1} --role=nameserver \
                   --zk_cluster=${ZK_CLUSTER}\
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --request_timeout_ms=100000\
                   --zk_root_path=/onebox > ns1.log 2>&1 &
sleep 2

# start ns2
../build/bin/fedb --endpoint=${NS2} --role=nameserver \
                   --zk_cluster=${ZK_CLUSTER}\
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --request_timeout_ms=100000\
                   --zk_root_path=/onebox > ns2.log 2>&1 &
sleep 2

# start ns3
../build/bin/fedb --endpoint=${NS3} --role=nameserver \
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --request_timeout_ms=100000\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_root_path=/onebox > ns3.log 2>&1 &

sleep 5

echo "start all ok"

