#!/bin/bash
#
# init.sh

MODE="cluster"
if [ $# -gt 0 ]; then
    MODE=$1
fi
if [ $MODE = "standalone" ]; then
    sed -i "s/.*zk_cluster=.*/#--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/nameserver.flags
    sed -i "s/.*zk_root_path=.*/#--zk_root_path=\/openmldb/g" /work/openmldb/conf/nameserver.flags
    sed -i "s/.*zk_cluster=.*/#--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/tablet.flags
    sed -i "s/.*zk_root_path=.*/#--zk_root_path=\/openmldb/g" /work/openmldb/conf/tablet.flags
    sed -i "s/.*zk_cluster=.*/#--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/apiserver.flags
    sed -i "s/.*zk_root_path=.*/#--zk_root_path=\/openmldb/g" /work/openmldb/conf/apiserver.flags
    cat data/taxi_tour_table_train_simple.csv | python3 convert_data.py > ./data/taxi_tour.csv
    cd /work/openmldb && sh bin/start-all.sh
    sleep 1
else
    sed -i "s/.*zk_cluster=.*/--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/nameserver.flags
    sed -i "s/.*zk_root_path=.*/--zk_root_path=\/openmldb/g" /work/openmldb/conf/nameserver.flags
    sed -i "s/.*zk_cluster=.*/--zk_cluster=127.0.0.1:2181/g" /work/openmldb/conf/tablet.flags
    sed -i "s/.*zk_root_path=.*/--zk_root_path=\/openmldb/g" /work/openmldb/conf/tablet.flags
    cd /work/zookeeper-3.4.14 && ./bin/zkServer.sh start
    sleep 1
    cd /work/openmldb && ./bin/start.sh start tablet
    sleep 1
    cd /work/openmldb && ./bin/start.sh start nameserver
    sleep 1
fi
