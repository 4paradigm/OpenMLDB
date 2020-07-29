#! /bin/sh
#
# install_fesql.sh
export JAVA_HOME=${RTIDB_DEV_JAVA_HOME:-/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.252.b09-0.fc32.x86_64}
export RTIDB_THIRDPARTY=${RTIDB_DEV_THIRDPARTY:-/depends/thirdparty}
WORK_DIR=`pwd`

cd fesql && ln -sf ${RTIDB_THIRDPARTY} thirdparty && mkdir -p build
cd build && cmake -DCMAKE_INSTALL_PREFIX="${RTIDB_THIRDPARTY}" -DTESTING_ENABLE=OFF -DCOVERAGE_ENABLE=OFF -DPYSDK_ENABLE=OFF -DJAVASDK_ENABLE=OFF -DEXPRIRMENT_ENABLE=OFF .. && make fesql_proto fesql_parser &&make -j16 install

export JAVA_HOME=/depends/thirdparty/jdk1.8.0_141
cd ${WORK_DIR}/fesql/java/fesql-common
mvn clean install

