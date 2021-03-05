#! /bin/sh
#
# compile.sh

WORK_DIR=`pwd`

sh tools/install_fesql_mac.sh
ln -sf /opt/depends/thirdparty thirdparty
ln -sf /opt/depends/thirdsrc thirdsrc
mkdir -p $WORK_DIR/build
cd $WORK_DIR/build && cmake .. && make -j4 rtidb sql_pysdk_package sql_jsdk
code=$?
cd $WORK_DIR
exit $code
