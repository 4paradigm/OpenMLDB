#! /bin/sh

set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
########################################


WORK_DIR=`pwd`

sh get_deps.sh

sh gen_code.sh

# build 

mkdir -p $WORK_DIR/build 
cd $WORK_DIR/build && cmake .. && make -j8
cd $WORK_DIR

sh ut.sh

sh benchmark.sh

sh build_java_client.sh 
