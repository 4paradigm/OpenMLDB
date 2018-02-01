#! /bin/sh
#
# boot.sh

python bin/monitor.py > /dev/null 2>&1 &
./bin/rtidb --flagfile=./conf/rtidb.flags
