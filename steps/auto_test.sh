#! /bin/sh

ROOT_DIR=`pwd`

PROTO_BIN=$ROOT_DIR/thirdparty/bin/protoc
ulimit -c unlimited
sed -i "/protocExecutable/c\<protocExecutable>${PROTO_BIN}<\/protocExecutable>" java/pom.xml
mkdir -p java/src/main/proto/
cp -rf src/proto/tablet.proto java/src/main/proto/
cp -rf src/proto/name_server.proto java/src/main/proto/
cp -rf src/proto/common.proto java/src/main/proto/

cp steps/zoo.cfg thirdsrc/zookeeper-3.4.10/conf
cd thirdsrc/zookeeper-3.4.10
test -d ut_zookeeper && rm -rf ut_zookeeper
netstat -anp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs kill -9
./bin/zkServer.sh start && cd $ROOT_DIR

sleep 5


cd onebox && sh start_onebox.sh && cd $ROOT_DIR
sleep 3

cd $ROOT_DIR/java
rtidb_version=`cat pom.xml| grep version | head -n 1 | sed -n 's/<version>\(.*\)<\/version>/\1/p'`
mvn clean install -Dmaven.test.skip=true


cd $ROOT_DIR
source /root/.bashrc && rm -rf auto-test-rtidb; git checkout .; git submodule init; git submodule update
cd auto-test-rtidb
git checkout release/1.4.2
echo "rtidb_version:$rtidb_version"
git pull
sh run.sh cicd.xml $rtidb_version

code=$?
cd $ROOT_DIR
cd onebox && sh stop_all.sh
cd $ROOT_DIR
cd thirdsrc/zookeeper-3.4.10 && ./bin/zkServer.sh stop
exit $code
