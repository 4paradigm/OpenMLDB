#! /bin/sh

ROOT_DIR=`pwd`

clear_debug() {
    ps -ef | grep rtidb | grep 9526 | awk '{print $2}' | while read line; do kill -9 $line; done
}

cp -rf src/proto/tablet.proto java/src/main/proto/rtidb/api

clear_debug

nohup ./build/bin/rtidb --log_level=debug --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:9526 --role=tablet >log 2>&1 &

sleep 2

cd $ROOT_DIR/java
mvn package 

clear_debug




