#! /bin/sh
#
# compile.sh
PWD=`pwd`
export PATH=${PWD}/thirdparty/bin:$PATH
mkdir -p build && cd build 
cmake .. && make fesql_proto && make fesql_parser && make -j20

