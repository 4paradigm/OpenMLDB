#! /bin/sh
#
# boot_ns.sh
export zk_cluster
export zk_root_path
export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH
export nameserver_port
Endpoint=$MY_NODE_NAME:$nameserver_port
./bin/rtidb --flagfile=./conf/nameserver.flags --endpoint=$Endpoint --zk_cluster=$zk_cluster --zk_root_path=$zk_root_path
