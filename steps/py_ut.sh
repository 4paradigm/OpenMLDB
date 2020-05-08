#! /bin/sh
#
# py_ut.sh

ROOT=`pwd`

clear_debug() {
    ps -ef | grep rtidb | grep 9501 | awk '{print $2}' | while read line; do kill -9 $line; done
}

clear_debug

./build/bin/rtidb --log_level=debug --gc_safe_offset=0 --gc_interval=1 --endpoint=0.0.0.0:9501 --role=tablet &

sleep 2

cp build/lib/librtidb_py.so python
cd python && python2 rtidb_client_test.py

clear_debug
sleep 3


