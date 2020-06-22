#! /bin/sh
#
# compile.sh
set -e

WORK_DIR=`pwd`
if [ "$1" = "DEBUG" ]
then
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE Debug)' CMakeLists.txt 
else
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE RelWithDebInfo)' CMakeLists.txt 
fi

[ -f "${WORK_DIR}/build/bin/rtidb" ] && exit 0
sh steps/gen_code.sh

mkdir -p $WORK_DIR/build 
cd $WORK_DIR/build && cmake .. && make -j10
code=$?
cd $WORK_DIR
exit $code
