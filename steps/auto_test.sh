#! /bin/sh

ROOT_DIR=`pwd`

PROTO_BIN=$ROOT_DIR/thirdparty/bin/protoc
ulimit -c unlimited
sed -i "/protocExecutable/c\<protocExecutable>${PROTO_BIN}<\/protocExecutable>" java/pom.xml
mkdir -p java/src/main/proto/
cp -rf src/proto/tablet.proto java/src/main/proto/
cp -rf src/proto/name_server.proto java/src/main/proto/
cp -rf src/proto/common.proto java/src/main/proto/

cd $ROOT_DIR/java
rtidb_version=`cat pom.xml| grep version | head -n 1 | sed -n 's/<version>\(.*\)<\/version>/\1/p'`
mvn clean install -Dmaven.test.skip=true

cd $ROOT_DIR
source /root/.bashrc && rm -rf rtidb-auto-test; git checkout .; git submodule init; git submodule update
cd rtidb-auto-test
git pull
sh run.sh gl.xml $rtidb_version