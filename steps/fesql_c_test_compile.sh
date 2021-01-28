#! /bin/sh
#
# compile.sh
CMAKE_TYPE=$1
if [[ "${CMAKE_TYPE}" != "Debug" ]]; then
        CMAKE_TYPE="RelWithDebInfo"
fi
WORK_DIR=`pwd`

sh steps/gen_code.sh
mkdir -p $WORK_DIR/build
cd $WORK_DIR/build && cmake -DCMAKE_BUILD_TYPE=${CMAKE_TYPE} .. && make -j16 rtidb sql_sdk_test sql_cluster_test tabelt_engine_test
code=$?
cd $WORK_DIR
exit $code
