#!/usr/bin/env bash

ROOT_DIR=`pwd`
test -d /rambuild/ut_zookeeper && rm -rf /rambuild/ut_zookeeper/*
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.14/conf
cd thirdsrc/zookeeper-3.4.14
netstat -anp | grep 6181 | awk '{print $1}' | awk -F '/' '{print $1}'| xargs kill -9
./bin/zkServer.sh start && cd $ROOT_DIR
sleep 5
cd onebox && sh start_onebox_on_rambuild.sh && cd $ROOT_DIR
sleep 5
echo "ROOT_DIR:${ROOT_DIR}"
sh steps/gen_code.sh
sh tools/install_fesql.sh

cd ${ROOT_DIR}/fesql/java/fesql-common
mvn clean install

mkdir -p ${ROOT_DIR}/build  && cd ${ROOT_DIR}/build && cmake .. && make -j16

case_xml=test_v1.xml
cd ${ROOT_DIR}/src/sdk/java/
mvn install -Dmaven.test.skip=true
cd ${ROOT_DIR}/src/sdk/java/fesql-auto-test-java
mvn test -DsuiteXmlFile=test_suite/${case_xml}
