#! /bin/sh
#
# compile.sh

WORK_DIR=`pwd`
sh steps/gen_code.sh
mkdir -p $WORK_DIR/build
cd $WORK_DIR/build && cmake .. && make -j16 rtidb
code=$?
cd $WORK_DIR
exit $code
