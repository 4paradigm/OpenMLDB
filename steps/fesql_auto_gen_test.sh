#!/usr/bin/env bash

ROOT_DIR=`pwd`
ulimit -c unlimited

echo "ROOT_DIR:${ROOT_DIR}"
ls -al fesql/build
echo "AAAA"
ls -al fesql/build/src/
echo "BBB"
ls -al fesql/tools/autotest/

sh steps/gen_code.sh
#sh tools/install_fesql.sh

#CMAKE_TYPE=$1
#
#if [[ "${CMAKE_TYPE}" != "Debug" ]]; then
#        CMAKE_TYPE="Release"
#fi
#echo "CMake Type "${CMAKE_TYPE}

export JAVA_HOME=${RTIDB_DEV_JAVA_HOME:-/depends/thirdparty/jdk1.8.0_141}
export RTIDB_THIRDPARTY=${RTIDB_DEV_THIRDPARTY:-/depends/thirdparty}
WORK_DIR=`pwd`

cd fesql && ln -sf ${RTIDB_THIRDPARTY} thirdparty && mkdir -p build
cd build && cmake .. && make fesql_proto && make fesql_parser && make -j5

#cmake -DCMAKE_BUILD_TYPE=${CMAKE_TYPE} -DCMAKE_INSTALL_PREFIX="${RTIDB_THIRDPARTY}" -DTESTING_ENABLE=OFF -DCOVERAGE_ENABLE=OFF -DPYSDK_ENABLE=OFF -DJAVASDK_ENABLE=OFF -DEXPRIRMENT_ENABLE=OFF ..  && make -j5 install


#mkdir -p ${ROOT_DIR}/build  && cd ${ROOT_DIR}/build && cmake ..
#if [ -z "${FEDEV}" ]; then
#    make -j5 sql_javasdk_package || { echo "compile error"; exit 1; }
#else
#    make -j16 || { echo "compile error"; exit 1; }
#fi
cd ${ROOT_DIR}
echo "CCC"
ls -al fesql/build
echo "DDDD"
ls -al fesql/build/src/

cd ${ROOT_DIR}/src/sdk/java/
mvn install -Dmaven.test.skip=true
cd ${ROOT_DIR}/src/sdk/java/fesql-auto-test-java
mvn test -DsuiteXmlFile=test_suite/${case_xml}
