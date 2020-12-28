#!/usr/bin/env bash

ROOT_DIR=`pwd`
ulimit -c unlimited

git submodule update

cd fesql

git pull

echo "-----------"

ls -al tools/autotest

cd ${ROOT_DIR}

sh steps/gen_code.sh
export JAVA_HOME=${RTIDB_DEV_JAVA_HOME:-/depends/thirdparty/jdk1.8.0_141}
export RTIDB_THIRDPARTY=${RTIDB_DEV_THIRDPARTY:-/depends/thirdparty}
cd fesql && ln -sf ${RTIDB_THIRDPARTY} thirdparty && mkdir -p build
cd build && cmake .. && make fesql_proto && make fesql_parser && make -j5
cd ${ROOT_DIR}
./fesql/build/src/export_udf_info --output_file=./udf_defs.yaml
ls -al
echo "+++++++++++++"
sh tools/install_fesql.sh
cd ${ROOT_DIR}/fesql/java/fesql-common; mvn install
echo "================="

mkdir -p ${ROOT_DIR}/build  && cd ${ROOT_DIR}/build && cmake ..
if [ -z "${FEDEV}" ]; then
    make -j5 sql_javasdk_package || { echo "compile error"; exit 1; }
else
    make -j16 || { echo "compile error"; exit 1; }
fi

echo "AAAAAAAAAAAAA"

python3 -m pip install numpy -i https://pypi.tuna.tsinghua.edu.cn/simple
python3 -m pip install PyYaml -i https://pypi.tuna.tsinghua.edu.cn/simple

python3 fesql/tools/autotest/gen_case_yaml_main.py  \
    --udf_path=udf_defs.yaml --yaml_count=1

ls -al fesql/cases/auto_gen_cases
echo "BBBBBBBBBB"

cd ${ROOT_DIR}
test -d /rambuild/ut_zookeeper && rm -rf /rambuild/ut_zookeeper/*
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.14/conf
cd thirdsrc/zookeeper-3.4.14
netstat -atnp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs kill -9
./bin/zkServer.sh start && cd $ROOT_DIR
sleep 5
cd onebox && sh start_onebox_on_rambuild.sh && cd $ROOT_DIR
sleep 5
case_xml=test_auto_gen_case_standalone.xml
cd ${ROOT_DIR}/src/sdk/java/
mvn install -Dmaven.test.skip=true

echo "CCCCCCC"

cd ${ROOT_DIR}/src/sdk/java/fesql-auto-test-java
mvn test -DsuiteXmlFile=test_suite/${case_xml}
