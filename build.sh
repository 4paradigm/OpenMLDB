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

echo "start to do core ut ....."
sh ut.sh

echo "start to do benchmark"
sh benchmark.sh

echo "start to build python client"

sh build_python_client.sh

echo "start to build java client"

sh build_java_client.sh 


