#! /bin/sh
#
# boot.sh
ulimit -c unlimited
echo "sleep 10 seconds"
sleep 10
./bin/rtidb --flagfile=./conf/tablet.flags --enable_status_service=true
