#! /bin/sh
#
# start_onebox.sh

# first start zookeeper

# start tablet0
test -d tablet0-binlogs && rm -rf tablet0-binlogs
test -d recycle_bin0 && rm -rf recycle_bin0
../build/bin/rtidb --db_root_path=tablet0-binlogs \
                   --recycle_bin_root_path=recycle_bin0 \
                   --endpoint=127.0.0.1:9520 --role=tablet \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet0.log 2>&1 &
test -d tablet1-binlogs && rm -rf tablet1-binlogs
test -d recycle_bin1 && rm -rf recycle_bin1


# start tablet1
../build/bin/rtidb --db_root_path=tablet1-binlogs \
                   --recycle_bin_root_path=recycle_bin1 \
                   --endpoint=127.0.0.1:9521 --role=tablet \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet1.log 2>&1 &
test -d tablet2-binlogs && rm -rf tablet2-binlogs
test -d recycle_bin2 && rm -rf recycle_bin2


# start tablet2
../build/bin/rtidb --db_root_path=tablet2-binlogs \
                   --recycle_bin_root_path=recycle_bin2 \
                   --endpoint=127.0.0.1:9522 --role=tablet \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_keep_alive_check_interval=100000000\
                   --zk_root_path=/onebox > tablet2.log 2>&1 &


# start ns1 
../build/bin/rtidb --endpoint=127.0.0.1:9622 --role=nameserver \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_root_path=/onebox > ns1.log 2>&1 &

# start ns2 
../build/bin/rtidb --endpoint=127.0.0.1:9623 --role=nameserver \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_root_path=/onebox > ns2.log 2>&1 &

# start ns3 
../build/bin/rtidb --endpoint=127.0.0.1:9624 --role=nameserver \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_root_path=/onebox > ns3.log 2>&1 &

sleep 3

echo "start all ok"

