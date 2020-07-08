#!/bin/bash

PATH=$PATH:/work/jdk1.8.0_121/bin
cd /work/

IP=127.0.0.1

cd ./zookeeper-3.4.14
sh ./bin/zkServer.sh start
cd -

cd fedb
sed -i "s/--endpoint=.*/--endpoint=${IP}:6527/g" conf/nameserver.flags
sed -i "s/--zk_cluster=.*/--zk_cluster=${IP}:2181/g" conf/nameserver.flags
sed -i "s/--zk_root_path=.*/--zk_root_path=\/fedb/g" conf/nameserver.flags

sed -i "s/--endpoint=.*/--endpoint=${IP}:9527/g" conf/tablet.flags
sed -i "s/#--zk_cluster=.*/--zk_cluster=${IP}:2181/g" conf/tablet.flags
sed -i "s/#--zk_root_path=.*/--zk_root_path=\/fedb/g" conf/tablet.flags

sh bin/boot_ns.sh &
sleep 2

sh bin/boot.sh
