#! /bin/sh
#
# benchmark.sh



ROOT_DIR=`pwd`

clear_debug() {
    ps -ef | grep rtidb | grep 9426 | awk '{print $2}' | while read line; do kill -9 $line; done
}

clear_debug

./build/bin/rtidb --log_level=info --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:9426 --role=tablet &

sleep 2

./build/bin/rtidb --cmd="create t0 1 1 0" --role=client --endpoint=127.0.0.1:9426 --interactive=false 
./build/bin/rtidb --cmd="create t0 2 1 0 true" --role=client --endpoint=127.0.0.1:9426 --interactive=false 

./build/bin/rtidb --cmd="benchmark" --role=client --endpoint=127.0.0.1:9426 --interactive=false | grep Percentile
echo "start drop table"
./build/bin/rtidb --cmd="drop 1" --role=client --endpoint=127.0.0.1:9426 --interactive=false

sleep 2

clear_debug

