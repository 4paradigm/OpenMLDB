#! /bin/sh
# install_fesql_mac.sh
export RTIDB_THIRDPARTY=/opt/depends/thirdparty
cd fesql && ln -sf ${RTIDB_THIRDPARTY} thirdparty && mkdir -p build
cd build && cmake -DCMAKE_INSTALL_PREFIX="${RTIDB_THIRDPARTY}" -DTESTING_ENABLE=OFF -DCOVERAGE_ENABLE=OFF -DPYSDK_ENABLE=OFF -DJAVASDK_ENABLE=OFF -DEXPRIRMENT_ENABLE=OFF .. && make fesql_proto fesql_parser &&make -j4 install
cd ${WORK_DIR}/fesql/java/fesql-common
mvn clean install