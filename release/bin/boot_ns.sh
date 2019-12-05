#! /bin/sh
#
# boot_ns.sh
ulimit -c unlimited
ulimit -n 655360
./bin/rtidb --flagfile=./conf/nameserver.flags --enable_status_service=true
