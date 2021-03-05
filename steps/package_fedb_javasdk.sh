#! /bin/sh
#
# package.sh
#
WORKDIR=`pwd`
set -e
sdk_vesion=$1-SNAPSHOT
mkdir -p src/sdk/java/sql-native/src/main/resources/
sh tools/install_fesql.sh ON
test -f build/src/sdk/libsql_jsdk.dylib && cp build/src/sdk/libsql_jsdk.dylib  src/sdk/java/sql-native/src/main/resources/
mkdir -p build && cd build &&  cmake .. && make -j4 sql_jsdk
cd ${WORKDIR}
cp build/src/sdk/libsql_jsdk.so  src/sdk/java/sql-native/src/main/resources/
cd src/sdk/java/ &&  mvn versions:set -DnewVersion=${sdk_vesion} && mvn deploy -Dmaven.test.skip=true
