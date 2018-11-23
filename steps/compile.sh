#! /bin/sh
#
# compile.sh

WORK_DIR=`pwd`
if [ "$1" = "DEBUG" ]
then
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE Debug)' CMakeLists.txt 
else
    sed -i '/set(CMAKE_BUILD_TYPE/c\set(CMAKE_BUILD_TYPE Release)' CMakeLists.txt 
fi

sed -i '73c DEFINE_int32\(make_snapshot_threshold_offset, 0, \"config the offset to reach the threshold\"\);' src/flags.cc
sh steps/gen_code.sh

mkdir -p $WORK_DIR/build 
cd $WORK_DIR/build && cmake .. && make -j8
cd $WORK_DIR


