#! /bin/sh
#
# compile.sh
PWD=`pwd`
export PATH=${PWD}/thirdparty/bin:$PATH
mkdir -p build && cd build && cmake .. && make -j1
#mkdir -p build && cd build && cmake .. && make -j4
