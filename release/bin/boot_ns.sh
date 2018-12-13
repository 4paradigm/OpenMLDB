#! /bin/sh
#
# boot_ns.sh
ulimit -c unlimited
echo "sleep 10 seconds"
sleep 10
./bin/rtidb --flagfile=./conf/nameserver.flags --enable_status_service=true
