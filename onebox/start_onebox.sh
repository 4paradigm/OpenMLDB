#! /bin/sh
#
# start_onebox.sh

# first start zookeeper

# start tablet0
../build/bin/rtidb --db_root_path=/tmp/tablet3-binlogs \
                   --endpoint=127.0.0.1:9520 --role=tablet \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_root_path=/onebox > tablet0.log 2>&1 &

# start tablet1
../build/bin/rtidb --db_root_path=/tmp/tablet1-binlogs \
                   --endpoint=127.0.0.1:9521 --role=tablet \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_root_path=/onebox > tablet1.log 2>&1 &

# start tablet2
../build/bin/rtidb --db_root_path=/tmp/tablet2-binlogs \
                   --endpoint=127.0.0.1:9522 --role=tablet \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_root_path=/onebox > tablet2.log 2>&1 &


# start ns1 
../build/bin/rtidb --endpoint=127.0.0.1:9622 --role=nameserver \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_root_path=/onebox > ns1.log 2>&1 &

# start ns2 
../build/bin/rtidb --endpoint=127.0.0.1:9623 --role=nameserver \
                   --zk_cluster=127.0.0.1:6181\
                   --zk_root_path=/onebox > ns2.log 2>&1 &

sleep 3

echo "start all ok"

