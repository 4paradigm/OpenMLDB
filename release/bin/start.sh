#! /bin/sh
#
# start.sh
CUR_DIR=`pwd`
P_DIR=`dirname $CUR_DIR`
cd $P_DIR && supervise ./service


