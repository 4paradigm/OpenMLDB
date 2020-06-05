#! /bin/sh
#
# boot.sh
export zk_cluster
export zk_root_path
export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH
export blobproxy_port
Endpoint=$MY_NODE_NAME:$blobproxy_port
./bin/rtidb --flagfile=./conf/blob_proxy.flags --endpoint=$Endpoint --zk_cluster=$zk_cluster --zk_root_path=$zk_root_path
