#! /bin/sh
CUR_DIR=`pwd`
P_DIR=`dirname $CUR_DIR`
cd $P_DIR && $pwd/bin/mon $pwd/bin/boot.sh -d -l $pwd/logs/rtidb_mon.log
