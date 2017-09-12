#! /bin/sh
#
# start.sh
CUR_DIR=`pwd`
P_DIR=`dirname $CUR_DIR`
cd $P_DIR && ./bin/mon ./bin/boot_nameserver.sh -d -l ./logs/rtidb_mon.log

