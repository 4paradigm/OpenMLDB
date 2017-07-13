#! /bin/sh
#
# cluster_run.sh

ROOT=`pwd`

clear_debug() {
    ps -u $USER -ef | grep rtidb | grep -v rtidb | awk '{print $2}' | while read line; do kill -9 $line; done
}

clear_debug

test -d /tmp/binlog1 && rm -rf /tmp/binlog1
test -d /tmp/binlog2 && rm -rf /tmp/binlog2

./build/bin/rtidb --binlog_root_path=/tmp/binlog1 --log_level=debug --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:19527 --role=tablet >log0 2>&1 &
./build/bin/rtidb --binlog_root_path=/tmp/binlog2 --log_level=debug --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:19528 --role=tablet >log1 2>&1 &
sleep 1

./build/bin/rtidb --cmd="create t1 1 1 0 false 127.0.0.1:9528" --role=client --endpoint=127.0.0.1:19528 --interactive=false 
./build/bin/rtidb --cmd="create t1 1 1 0 true 127.0.0.1:9528" --role=client --endpoint=127.0.0.1:19527 --interactive=false 
./build/bin/rtidb --cmd="create t1 2 1 0 false 127.0.0.1:9528" --role=client --endpoint=127.0.0.1:19528 --interactive=false 
./build/bin/rtidb --cmd="create t1 2 1 0 true 127.0.0.1:9528" --role=client --endpoint=127.0.0.1:19527 --interactive=false 
sleep 1
./build/bin/rtidb --cmd="benchmark" --role=client --endpoint=127.0.0.1:19527 --interactive=false  | grep Percentile

