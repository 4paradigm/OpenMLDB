#! /bin/sh
#
# boot.sh
ulimit -c unlimited
ulimit -n 655360
./bin/rtidb --flagfile=./conf/tablet.flags --enable_status_service=true
