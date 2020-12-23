#! /bin/sh
#
ulimit -c unlimited
# start_onebox.sh

# first start zookeeper
IP=127.0.0.1

ZK_CLUSTER=$IP:6181
NS1=$IP:9622
TABLET1=$IP:9520
BLOB1=$IP:9720

RAMBUILD_PREFIX=/Users/chenjing/tmp/rambuild
../build/bin/rtidb --db_root_path=${RAMBUILD_PREFIX}/tablet0-binlogs \
                   --hdd_root_path=${RAMBUILD_PREFIX}/tablet0-hdd-binlogs \
                   --ssd_root_path=${RAMBUILD_PREFIX}/tablet0-ssd-binlogs \
                   --recycle_bin_root_path=${RAMBUILD_PREFIX}/recycle_bin0 \
                   --recycle_ssd_bin_root_path=${RAMBUILD_PREFIX}/recycle_ssd_bin0 \
                   --recycle_hdd_bin_root_path=${RAMBUILD_PREFIX}/recycle_hdd_bin0 \
                   --endpoint=${TABLET1} --role=tablet \
                   --binlog_notify_on_put=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet0.log 2>&1 &
sleep 2
# start ns1
../build/bin/rtidb --endpoint=${NS1} --role=nameserver \
                   --zk_cluster=${ZK_CLUSTER}\
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --request_timeout_ms=100000\
                   --zk_root_path=/onebox > ns1.log 2>&1 &
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
                   --zk_root_path=/onebox > blob1.log 2>&1 &

sleep 5
echo "start all ok"

