#! /bin/sh
#
# cluster_run.sh

ROOT=`pwd`

clear_debug() {
    ps xf | grep rtidb | grep -v grep | awk '{print $1}' | while read line; do kill -9 $line; done
}

clear_debug

test -d $ROOT/binlog1 && rm -rf $ROOT/binlog1
test -d $ROOT/binlog2 && rm -rf $ROOT/binlog2

./build/bin/rtidb --binlog_root_path=$ROOT/binlog1 --binlog_single_file_max_size=8 --log_level=debug --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:19527 --role=tablet >log0 2>&1 &
./build/bin/rtidb --binlog_root_path=$ROOT/binlog2 --binlog_single_file_max_size=8 --log_level=debug --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:19528 --role=tablet >log1 2>&1 &
sleep 1

./build/bin/rtidb --cmd="create t1 2 1 0 false 127.0.0.1:19528" --role=client --endpoint=127.0.0.1:19528 --interactive=false 
./build/bin/rtidb --cmd="create t1 2 1 0 true 127.0.0.1:19528" --role=client --endpoint=127.0.0.1:19527 --interactive=false 
./build/bin/rtidb --cmd="create t1 1 1 0 true" --role=client --endpoint=127.0.0.1:19527 --interactive=false 
sleep 1
./build/bin/rtidb --cmd="benchmark" --role=client --endpoint=127.0.0.1:19527 --interactive=false  | grep Percentile

clear_debug
