#! /bin/sh
#
# start.sh
set -x
pwd=/home/liuchenlu/RTIDB/RTIDBHA/rtidb-0.9.6-static
CUR_DIR=`pwd`
P_DIR=`dirname $CUR_DIR`
cd $P_DIR && $pwd/bin/mon $pwd/bin/boot.sh -d -l $pwd/logs/rtidb_mon.log
