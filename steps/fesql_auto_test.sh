#!/usr/bin/env bash

# compile.sh

CASE_LEVEL=$1
if [[ "${CASE_LEVEL}" == "" ]]; then
        CASE_LEVEL="0"
fi
echo "fesql auto test : case_level ${CASE_LEVEL}"
ROOT_DIR=`pwd`
ulimit -c unlimited

echo "ROOT_DIR:${ROOT_DIR}"
sh steps/gen_code.sh
sh tools/install_fesql.sh
cd ${ROOT_DIR}/fesql/java/fesql-common; mvn install

mkdir -p ${ROOT_DIR}/build  && cd ${ROOT_DIR}/build && cmake .. 
if [ -z "${FEDEV}" ]; then
    make -j5 sql_javasdk_package || { echo "compile error"; exit 1; }
else
    make -j16 || { echo "compile error"; exit 1; }
fi
cd ${ROOT_DIR}
test -d /rambuild/ut_zookeeper && rm -rf /rambuild/ut_zookeeper/*
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.14/conf
cd thirdsrc/zookeeper-3.4.14
netstat -atnp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs kill -9
./bin/zkServer.sh start && cd $ROOT_DIR
sleep 5
cd onebox && sh start_onebox_on_rambuild.sh && cd $ROOT_DIR
sleep 5
case_xml=test_v1.xml
cd ${ROOT_DIR}/src/sdk/java/
mvn install -Dmaven.test.skip=true
cd ${ROOT_DIR}/src/sdk/java/fesql-auto-test-java
mvn test -DsuiteXmlFile=test_suite/${case_xml} -DcaseLevel=$CASE_LEVEL
