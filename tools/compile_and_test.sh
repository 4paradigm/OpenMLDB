#! /bin/sh
#
# compile.sh
PWD=`pwd`
export PATH=${PWD}/thirdparty/bin:$PATH
rm -rf build
mkdir -p build && cd build && cmake .. && make -j4 && make test
