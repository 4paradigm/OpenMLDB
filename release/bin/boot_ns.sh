#! /bin/sh
#
# boot_ns.sh
ulimit -c unlimited
./bin/rtidb --flagfile=./conf/nameserver.flags --enable_status_service=true
