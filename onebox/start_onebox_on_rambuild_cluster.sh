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

RAMBUILD_PREFIX=/tmp/rambuild
../build/bin/rtidb --db_root_path=${RAMBUILD_PREFIX}/tablet0-binlogs \
                   --hdd_root_path=${RAMBUILD_PREFIX}/tablet0-hdd-binlogs \
                   --ssd_root_path=${RAMBUILD_PREFIX}/tablet0-ssd-binlogs \
                   --recycle_bin_root_path=${RAMBUILD_PREFIX}/recycle_bin0 \
                   --recycle_ssd_bin_root_path=${RAMBUILD_PREFIX}/recycle_ssd_bin0 \
                   --recycle_hdd_bin_root_path=${RAMBUILD_PREFIX}/recycle_hdd_bin0 \
                   --endpoint=${TABLET1} --role=tablet \
                   --binlog_notify_on_put=true\
                   --enable_distsql=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/cluster> tablet0.log 2>&1 &

# start tablet1
../build/bin/rtidb --db_root_path=${RAMBUILD_PREFIX}/tablet1-binlogs \
                   --hdd_root_path=${RAMBUILD_PREFIX}/tablet1-hdd-binlogs \
                   --ssd_root_path=${RAMBUILD_PREFIX}/tablet1-ssd-binlogs \
                   --recycle_bin_root_path=${RAMBUILD_PREFIX}/recycle_bin1 \
                   --recycle_ssd_bin_root_path=${RAMBUILD_PREFIX}/recycle_ssd-bin1 \
                   --recycle_hdd_bin_root_path=${RAMBUILD_PREFIX}/recycle_hdd-bin1 \
                   --endpoint=${TABLET2} --role=tablet \
                   --zk_cluster=${ZK_CLUSTER}\
                   --binlog_notify_on_put=true\
                   --enable_distsql=true\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/cluster > tablet1.log 2>&1 &

# start tablet2
../build/bin/rtidb --db_root_path=${RAMBUILD_PREFIX}/tablet2-binlogs \
                   --hdd_root_path=${RAMBUILD_PREFIX}/tablet2-hdd-binlogs \
                   --ssd_root_path=${RAMBUILD_PREFIX}/tablet2-ssd-binlogs \
                   --recycle_bin_root_path=${RAMBUILD_PREFIX}/recycle_bin2 \
                   --recycle_ssd_bin_root_path=${RAMBUILD_PREFIX}/recycle_ssd_bin2 \
                   --recycle_hdd_bin_root_path=${RAMBUILD_PREFIX}/recycle_hdd_bin2 \
                   --endpoint=${TABLET3} --role=tablet \
                   --binlog_notify_on_put=true\
                   --enable_distsql=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/cluster > tablet2.log 2>&1 &

# start ns1
../build/bin/rtidb --endpoint=${NS1} --role=nameserver \
                   --zk_cluster=${ZK_CLUSTER}\
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --request_timeout_ms=100000\
                   --zk_root_path=/cluster > ns1.log 2>&1 &
sleep 2

# start ns2
../build/bin/rtidb --endpoint=${NS2} --role=nameserver \
                   --zk_cluster=${ZK_CLUSTER}\
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --request_timeout_ms=100000\
                   --zk_root_path=/cluster > ns2.log 2>&1 &
sleep 2

# start ns3
../build/bin/rtidb --endpoint=${NS3} --role=nameserver \
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --request_timeout_ms=100000\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_root_path=/cluster > ns3.log 2>&1 &

sleep 5
# start blob1
../build/bin/rtidb --hdd_root_path=${RAMBUILD_PREFIX}/blob1-hdd-binlogs \
                   --ssd_root_path=${RAMBUILD_PREFIX}/blob1-ssd-binlogs \
                   --recycle_bin_root_path=${RAMBUILD_PREFIX}/recycle_bin3 \
                   --recycle_ssd_bin_root_path=${RAMBUILD_PREFIX}/recycle_ssd_bin3 \
                   --recycle_hdd_bin_root_path=${RAMBUILD_PREFIX}/recycle_hdd_bin3 \
                   --endpoint=${BLOB1} --role=blob \
                   --binlog_notify_on_put=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/cluster > blob1.log 2>&1 &

sleep 5

echo "start all ok"

