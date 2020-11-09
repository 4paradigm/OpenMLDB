#! /bin/sh
#
# boot_blob_proxy.sh
source ./bin/getzkconf.sh
[ -z "${zkservers}" ] && { getzk; }
export zk_cluster=${zkservers}
export zk_root_path=${zkpath}
export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH
export blobproxy_port

function detect_zk_change() {
  while :
  do
    sleep 30
    getzk
    if [[ ${zk_cluster} != ${zkservers} ]] || [[ ${zk_root_path} != ${zkpath} ]]; then
      ps -ef | grep ${Endpoint} | grep -v grep | awk '{print "kill "$2}' | bash
      break
    fi
  done
}

Endpoint="$(hostname -i):${blobproxy_port}"
SELFDIR=$(dirname $(readlink -f $0))
WORKDIR=$(dirname ${SELFDIR})
cd ${WORKDIR}
tmpfile=$(mktemp)
[ -n "${mimedb}" ] && { echo ${mimedb} | base64 -d >> ${tmpfile} && cat ${tmpfile} >> conf/mime.conf; }
detect_zk_change &
./bin/rtidb --flagfile=./conf/blob_proxy.flags --endpoint=$Endpoint --zk_cluster=$zk_cluster --zk_root_path=$zk_root_path --enable_status_service=true
