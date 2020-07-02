#! /bin/sh
#
# boot_blob_proxy.sh
[ -z "${zkservers}" ] && { bash ./bin/getzkconf.sh || exit 1; }
export zk_cluster=${zkservers}
export zk_root_path=${zkpath}
export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH
export blobproxy_port
Endpoint=$MY_NODE_NAME:$blobproxy_port
SELFDIR=$(dirname $0)
WORKDIR=$(dirname ${SELFDIR})
cd ${WORKDIR}
./bin/rtidb --flagfile=./conf/blob_proxy.flags --endpoint=$Endpoint --zk_cluster=$zk_cluster --zk_root_path=$zk_root_path
