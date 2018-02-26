#! /bin/sh
#
# java_ut.sh
#
ROOT_DIR=`pwd`

clear_debug() {
    ps -ef | grep rtidb | grep 9501 | awk '{print $2}' | while read line; do kill -9 $line; done
}
PROTO_BIN=$ROOT_DIR/thirdparty/bin/protoc

sed -i "/protocExecutable/c\<protocExecutable>${PROTO_BIN}<\/protocExecutable>" java/pom.xml
mkdir -p java/src/main/proto/rtidb/api
mkdir -p java/src/main/proto/rtidb/nameserver
cp -rf src/proto/tablet.proto java/src/main/proto/rtidb/api/
cp -rf src/proto/name_server.proto java/src/main/proto/rtidb/nameserver/

clear_debug
test -d /tmp/ut_zookeeper && rm -rf /tmp/ut_zookeeper
cd thirdsrc/zookeeper-3.4.10/bin && ./zkServer.sh start && cd $ROOT_DIR

./build/bin/rtidb --db_root_path=/tmp/$RANDOM --log_level=debug --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:9501 --role=tablet &

sleep 2

cd onebox && sh start_onebox.sh && cd $ROOT_DIR
sleep 3
cd $ROOT_DIR/java
mvn clean test -Dtest=com._4paradigm.rtidb.client.ut.*Test,com._4paradigm.rtidb.client.ut.ha.*Test

clear_debug
cd $ROOT_DIR
cd onebox && sh stop_all.sh && cd $ROOT_DIR
cd thirdsrc/zookeeper-3.4.10/bin && ./zkServer.sh stop
