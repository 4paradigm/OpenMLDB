#! /bin/sh
#
# compile.sh
CMAKE_TYPE=$1
if [[ "${CMAKE_TYPE}" != "Debug" ]]; then
        CMAKE_TYPE="Release"
fi
WORK_DIR=`pwd`

sh steps/gen_code.sh
mkdir -p $WORK_DIR/build
cd $WORK_DIR/build && cmake .. -DCMAKE_BUILD_TYPE=${CMAKE_TYPE} -DMAC_TABLET_ENABLE=ON && make -j4 rtidb sql_pysdk_package sql_jsdk
code=$?
cd $WORK_DIR
exit $code
