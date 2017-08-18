#! /bin/sh
#
# ut.sh
#
WORK_DIR=`pwd`

#$WORK_DIR/test-common/unittest/run_ut

ls  build/bin/ | grep test | grep -v grep | while read line; do ./build/bin/$line; done



