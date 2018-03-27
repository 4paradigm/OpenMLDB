#! /bin/sh
#
# start_ns.sh
CUR_DIR=`pwd`
P_DIR=`dirname $CUR_DIR`
cd $P_DIR && ./bin/mon ./bin/boot_ns.sh -d -l ./logs/rtidb_ns_mon.log
