#! /bin/sh
#
# compile.sh

WORK_DIR=`pwd`
sh steps/gen_code.sh
cd ${ROOT_DIR}/fesql/java/fesql-common
mvn clean install
mkdir -p $WORK_DIR/build 
cd $WORK_DIR/build && cmake .. && make -j16 rtidb sql_pysdk_package sql_jsdk parse_log
code=$?
cd $WORK_DIR
exit $code
