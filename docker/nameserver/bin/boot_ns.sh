#! /bin/sh
#
# boot_ns.sh
export zk_cluster
export zk_root_path
#export port
port="6527"
Endpoint=$MY_NODE_NAME:$port
./bin/rtidb --flagfile=./conf/nameserver.flags --endpoint=$Endpoint --zk_cluster=$zk_cluster --zk_root_path=$zk_root_path
