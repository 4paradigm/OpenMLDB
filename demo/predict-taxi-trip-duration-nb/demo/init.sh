#! /bin/sh
#
# init.sh

cd /work/zookeeper-3.4.14 && ./bin/zkServer.sh start
sleep 1
cd /work/openmldb && ./bin/start.sh start tablet
sleep 1
cd /work/openmldb && ./bin/start.sh start nameserver
sleep 1
cd /work/openmldb && ./bin/openmldb --interactive=false --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --cmd="show databases;"
