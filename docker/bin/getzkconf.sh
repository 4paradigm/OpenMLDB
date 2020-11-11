function getzk() {
  zkservers=""
  zkpath=""
  SELFDIR=$(dirname $0)
  cd ${SELFDIR}
  for ((int = 0; i < 10; i++)) {
      zkservers=$(curl -sH "Access-Key: e5f1fc70-7d1d-4f60-9bd6-fd949078adad" http://config-center/config-center/v1/configs/config-center/ZOOKEEPER_INFO | python getdata.py)
      zkpath=$(curl -sH "Access-Key: e5f1fc70-7d1d-4f60-9bd6-fd949078adad" http://config-center/config-center/v1/configs/config-center/RTIDB_ZK_ROOT_PATH | python getdata.py)
      [ -z "${zkservers}" ] && sleep 30 && continue
      [ -z "${zkpath}" ] && sleep 30 && continue
      break
  }
  [ -z "${zkservers}" ] && echo "zk servers is empty" && exit 1
  [ -z "${zkpath}" ] && echo "zk path is empty" && exit 1
  export zkservers
  export zkpath
  cd -
}
