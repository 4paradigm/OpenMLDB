#! /bin/sh
# install_fesql_mac.sh
CMAKE_TYPE=$1

if [[ "${CMAKE_TYPE}" != "Debug" ]]; then
        CMAKE_TYPE="Release"
fi
echo "CMake Type "${CMAKE_TYPE}
WORK_DIR=`pwd`
cd fesql && mkdir -p build
export RTIDB_THIRDPARTY=/Users/chenjing/work/fedb/rtidb/fesql/thirdparty
cd fesql && mkdir -p build
cd build && cmake -DCMAKE_BUILD_TYPE=${CMAKE_TYPE} -DCMAKE_INSTALL_PREFIX="${RTIDB_THIRDPARTY}" -DTESTING_ENABLE=OFF -DCOVERAGE_ENABLE=OFF -DPYSDK_ENABLE=ON -DJAVASDK_ENABLE=ON -DEXPRIRMENT_ENABLE=OFF ..  && make -j5 install
cd ${WORK_DIR}/fesql/java/fesql-common && mvn install
