#! /bin/sh
#
# install_fesql.sh
CMAKE_TYPE=$1

if [[ "${CMAKE_TYPE}" != "Debug" ]]; then
        CMAKE_TYPE="RelWithDebInfo"
fi
echo "CMake Type "${CMAKE_TYPE}

export JAVA_HOME=${RTIDB_DEV_JAVA_HOME:-/depends/thirdparty/jdk1.8.0_141}
export RTIDB_THIRDPARTY=${RTIDB_DEV_THIRDPARTY:-/depends/thirdparty}
WORK_DIR=`pwd`

cd fesql && ln -sf ${RTIDB_THIRDPARTY} thirdparty && mkdir -p build
cd build && cmake -DCMAKE_BUILD_TYPE=${CMAKE_TYPE} -DCMAKE_INSTALL_PREFIX="${RTIDB_THIRDPARTY}" -DTESTING_ENABLE=OFF -DCOVERAGE_ENABLE=OFF -DPYSDK_ENABLE=OFF -DJAVASDK_ENABLE=ON -DEXPRIRMENT_ENABLE=OFF ..  && make -j5 install

cd ${WORK_DIR}/fesql/java/fesql-native && mvn install
cd ${WORK_DIR}/fesql/java/fesql-common && mvn install
