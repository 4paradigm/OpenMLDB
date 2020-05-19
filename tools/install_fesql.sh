#! /bin/sh
#
# install_fesql.sh
export JAVA_HOME=${RTIDB_DEV_JAVA_HOME:-/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.252.b09-0.fc32.x86_64}
export RTIDB_THIRDPARTY=${RTIDB_DEV_THIRDPARTY:-/depends/thirdparty}
echo $JAVA_HOME
echo $RTIDB_THIRDPARTY
exit 1
cd fesql && ln -sf ${RTIDB_DEV_THIRDPARTY} thirdparty && mkdir -p build
cd build && cmake -DCMAKE_INSTALL_PREFIX="${RTIDB_THIRDPARTY}" -DTESTING_ENABLE=OFF -DCOVERAGE_ENABLE=OFF -DPYSDK_ENABLE=OFF -DJAVASDK_ENABLE=OFF -DEXPRIRMENT_ENABLE=OFF .. && make -j10 install


