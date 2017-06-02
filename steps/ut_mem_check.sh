#! /bin/sh
#
# ut_mem_check.sh
#

ROOT_DIR=`pwd`

clear_debug() {
    ps -ef | grep rtidb | grep 9426 | awk '{print $2}' | while read line; do kill  $line; done
}

clear_debug

HEAPCHECK=normal ./build/bin/rtidb --log_level=info --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:9426 --role=tablet &

sleep 2

./build/bin/rtidb --cmd="create t0 1 1 0" --role=client --endpoint=127.0.0.1:9426 --interactive=false 

for i in {1..5}
do
   for j in {1..5}
   do
       ./build/bin/rtidb --cmd="put 1 1 test$i $j test" --role=client --endpoint=127.0.0.1:9426 --interactive=false
   done
done

for k in {1..5}
do
    ./build/bin/rtidb --cmd="scan 1 1 test$k 5 0" --role=client --endpoint=127.0.0.1:9426 --interactive=false
done

./build/bin/rtidb --cmd="drop 1" --role=client --endpoint=127.0.0.1:9426 --interactive=false

sleep 4 

clear_debug



