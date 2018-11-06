#! /bin/sh
#
# java_ut.sh
#
ROOT_DIR=`pwd`

clear_debug() {
    ps -ef | grep rtidb | grep 9501 | awk '{print $2}' | while read line; do kill -9 $line; done
}
PROTO_BIN=$ROOT_DIR/thirdparty/bin/protoc
ulimit -c unlimited
sed -i "/protocExecutable/c\<protocExecutable>${PROTO_BIN}<\/protocExecutable>" java/pom.xml
mkdir -p java/src/main/proto/rtidb/api
mkdir -p java/src/main/proto/rtidb/nameserver
cp -rf src/proto/tablet.proto java/src/main/proto/rtidb/api/
cp -rf src/proto/name_server.proto java/src/main/proto/rtidb/nameserver/

clear_debug
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.10/conf
cd thirdsrc/zookeeper-3.4.10
test -d ut_zookeeper && rm -rf ut_zookeeper
netstat -anp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs kill -9
./bin/zkServer.sh start && cd $ROOT_DIR
./build/bin/rtidb --db_root_path=/tmp/$RANDOM --log_level=debug --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:9501 --role=tablet &

sleep 2

cd onebox && sh start_onebox.sh && cd $ROOT_DIR
sleep 3
cd $ROOT_DIR/java
#mvn clean test -Dproject.build.sourceEncoding=UTF-8 -Dproject.reporting.outputEncoding=UTF-8 -Dtest=com._4paradigm.rtidb.client.ut.*Test,com._4paradigm.rtidb.client.ut.ha.*Test
mvn clean test  -Dtest=com._4paradigm.rtidb.client.ut.ha.TableSyncClientTest
clear_debug
cd $ROOT_DIR
cd onebox && sh stop_all.sh
cd $ROOT_DIR
cd thirdsrc/zookeeper-3.4.10 && ./bin/zkServer.sh stop
