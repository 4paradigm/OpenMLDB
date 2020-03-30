#! /bin/sh
#
# compile.sh
PWD=`pwd`
export JAVA_HOME=${PWD}/thirdparty/jdk1.8.0_141
export PATH=${PWD}/thirdparty/bin:$JAVA_HOME/bin:${PWD}/thirdparty/apache-maven-3.6.3/bin:$PATH
mkdir -p build && cd build 
cmake .. && make fesql_proto && make fesql_parser && make -j20
