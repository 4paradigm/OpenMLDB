#! /bin/sh
#
# compile.sh

WORK_DIR=`pwd`
sh steps/gen_code.sh
mkdir -p $WORK_DIR/build 
cd $WORK_DIR/build && cmake .. && make -j4 rtidb sql_pysdk_package sql_javasdk_package parse_log
code=$?
cd $WORK_DIR
exit $code
