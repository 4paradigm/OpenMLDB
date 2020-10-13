#!/usr/bin/env bash

ROOT_DIR=`pwd`
ulimit -c unlimited

sh steps/gen_code.sh
export JAVA_HOME=${RTIDB_DEV_JAVA_HOME:-/depends/thirdparty/jdk1.8.0_141}
export RTIDB_THIRDPARTY=${RTIDB_DEV_THIRDPARTY:-/depends/thirdparty}
cd fesql && ln -sf ${RTIDB_THIRDPARTY} thirdparty && mkdir -p build
cd build && cmake .. && make fesql_proto && make fesql_parser && make -j5
cd ${ROOT_DIR}
./fesql/build/src/export_udf_info --output_file=./udf_defs.yaml
python3 fesql/tools/autotest/auto_cases.py  \
    --bin_path=build  \
    --udf_path=udf_defs.yaml  \
    --expr_num=2  \
    --expr_depth=3  \
    --max_cases=1 \
    --workers=4
#
#cd ${ROOT_DIR}
#echo "CCC"
#ls -al fesql/build
#echo "DDDD"
#ls -al fesql/build/src/
#
#cd ${ROOT_DIR}/src/sdk/java/
#mvn install -Dmaven.test.skip=true
#cd ${ROOT_DIR}/src/sdk/java/fesql-auto-test-java
#mvn test -DsuiteXmlFile=test_suite/${case_xml}
