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

# start tablet0
test -d tablet0-binlogs && rm -rf tablet0-binlogs
test -d tablet0-hdd-binlogs && rm -rf tablet0-hdd-binlogs
test -d tablet0-ssd-binlogs && rm -rf tablet0-ssd-binlogs
test -d recycle_bin0 && rm -rf recycle_bin0
test -d recycle_hdd_bin0 && rm -rf recycle_hdd_bin0
test -d recycle_ssd_bin0 && rm -rf recycle_ssd_bin0
../build/bin/rtidb --db_root_path=tablet0-binlogs \
                   --hdd_root_path=tablet0-hdd-binlogs \
                   --ssd_root_path=tablet0-ssd-binlogs \
                   --recycle_bin_root_path=recycle_bin0 \
                   --recycle_ssd_bin_root_path=recycle_ssd_bin0 \
                   --recycle_hdd_bin_root_path=recycle_hdd_bin0 \
                   --endpoint=${TABLET1} --role=tablet \
                   --binlog_notify_on_put=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet0.log 2>&1 &
test -d tablet1-binlogs && rm -rf tablet1-binlogs
test -d tablet1-hdd-binlogs && rm -rf tablet1-hdd-binlogs
test -d tablet1-ssd-binlogs && rm -rf tablet1-ssd-binlogs
test -d recycle_bin1 && rm -rf recycle_bin1
test -d recycle_hdd_bin1 && rm -rf recycle_hdd_bin1
test -d recycle_ssd_bin1 && rm -rf recycle_ssd_bin1


# start tablet1
../build/bin/rtidb --db_root_path=tablet1-binlogs \
                   --hdd_root_path=tablet1-hdd-binlogs \
                   --ssd_root_path=tablet1-ssd-binlogs \
                   --recycle_bin_root_path=recycle_bin1 \
                   --recycle_ssd_bin_root_path=recycle_ssd-bin1 \
                   --recycle_hdd_bin_root_path=recycle_hdd-bin1 \
                   --endpoint=${TABLET2} --role=tablet \
                   --zk_cluster=${ZK_CLUSTER}\
                   --binlog_notify_on_put=true\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet1.log 2>&1 &
test -d tablet2-binlogs && rm -rf tablet2-binlogs
test -d tablet2-hdd-binlogs && rm -rf tablet2-hdd-binlogs
test -d tablet2-ssd-binlogs && rm -rf tablet2-ssd-binlogs
test -d recycle_bin2 && rm -rf recycle_bin2
test -d recycle_hdd_bin2 && rm -rf recycle_hdd_bin2
test -d recycle_ssd_bin2 && rm -rf recycle_ssd_bin2


# start tablet2
../build/bin/rtidb --db_root_path=tablet2-binlogs \
                   --hdd_root_path=tablet2-hdd-binlogs \
                   --ssd_root_path=tablet2-ssd-binlogs \
                   --recycle_bin_root_path=recycle_bin2 \
                   --recycle_ssd_bin_root_path=recycle_ssd_bin2 \
                   --recycle_hdd_bin_root_path=recycle_hdd_bin2 \
                   --endpoint=${TABLET3} --role=tablet \
                   --binlog_notify_on_put=true\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet2.log 2>&1 &


# start ns1 
../build/bin/rtidb --endpoint=${NS1} --role=nameserver \
                   --zk_cluster=${ZK_CLUSTER}\
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --zk_root_path=/onebox > ns1.log 2>&1 &

# start ns2 
../build/bin/rtidb --endpoint=${NS2} --role=nameserver \
                   --zk_cluster=${ZK_CLUSTER}\
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --zk_root_path=/onebox > ns2.log 2>&1 &

# start ns3 
../build/bin/rtidb --endpoint=${NS3} --role=nameserver \
                   --tablet_offline_check_interval=1\
                   --tablet_heartbeat_timeout=1\
                   --zk_cluster=${ZK_CLUSTER}\
                   --zk_root_path=/onebox > ns3.log 2>&1 &

sleep 3

echo "start all ok"

