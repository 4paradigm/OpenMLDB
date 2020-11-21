#! /bin/sh
#
# compile.sh

WORK_DIR=`pwd`

sh tools/install_fesql_cj.sh
sh steps/gen_code.sh
mkdir -p $WORK_DIR/build
cd $WORK_DIR/build && cmake .. -DMAC_TABLET_ENABLE=ON && make -j4 rtidb sql_pysdk_package sql_jsdk
code=$?
cd $WORK_DIR
exit $code
