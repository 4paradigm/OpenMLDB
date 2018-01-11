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
cp -rf src/proto/tablet.proto java/src/main/proto/rtidb/api

clear_debug

./build/bin/rtidb --db_root_path=/tmp/$RANDOM --log_level=debug --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:9501 --role=tablet &

sleep 2

cd $ROOT_DIR/java
mvn test -Dtest=com._4paradigm.rtidb.client.*Test
clear_debug

