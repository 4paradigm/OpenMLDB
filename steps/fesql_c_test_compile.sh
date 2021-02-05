#! /bin/sh
#
# compile.sh
CMAKE_TYPE=$1
if [[ "${CMAKE_TYPE}" != "Debug" ]]; then
        CMAKE_TYPE="RelWithDebInfo"
fi
WORK_DIR=`pwd`

mkdir -p $WORK_DIR/build
if [ ! -f "sql_sdk_test" ]; then
  echo "sql_sdk_test not exist, make rtidb and sql_sdk_test..."
  sh steps/gen_code.sh
  cd $WORK_DIR/build && cmake -DCMAKE_BUILD_TYPE=${CMAKE_TYPE} .. && make -j16 rtidb sql_sdk_test sql_cluster_test tablet_engine_test
  code=$?
  cd $WORK_DIR
fi

exit $code
