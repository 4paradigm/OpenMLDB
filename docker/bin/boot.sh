#! /bin/sh
#
# boot.sh
[ -z "${zkservers}" ] && { bash ./bin/getzkconf.sh || exit 1; }
export zk_cluster=${zkservers}
export zk_root_path=${zkpath}
export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH
export blobproxy_port
Endpoint=$MY_NODE_NAME:$blobproxy_port
./bin/rtidb --flagfile=./conf/blob_proxy.flags --endpoint=$Endpoint --zk_cluster=$zk_cluster --zk_root_path=$zk_root_path
