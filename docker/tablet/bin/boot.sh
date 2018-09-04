#! /bin/sh
#
# boot.sh
export zk_cluster
export zk_root_path
#export port
port="9527"
Endpoint=$MY_NODE_NAME:$port
./bin/rtidb --flagfile=./conf/tablet.flags --endpoint=$Endpoint --zk_cluster=$zk_cluster --zk_root_path=$zk_root_path
