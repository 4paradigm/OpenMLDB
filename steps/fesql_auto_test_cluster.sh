#!/usr/bin/env bash

ROOT_DIR=`pwd`
ulimit -c unlimited
CASE_LEVEL=$1
if [[ "${CASE_LEVEL}" == "" ]]; then
        CASE_LEVEL="0"
fi
echo "fesql auto test cluster: case_level ${CASE_LEVEL}"
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
cd onebox && sh start_onebox_on_rambuild_cluster.sh && cd $ROOT_DIR
sleep 5
IP=127.0.0.1
cd ${ROOT_DIR}
cd src/sdk/java/fesql-auto-test-java/src/main/resources
echo "cluster_tb_endpoint_0=$IP:9520" >> fesql.properties
echo "cluster_tb_endpoint_1=$IP:9521" >> fesql.properties
echo "cluster_tb_endpoint_2=$IP:9522" >> fesql.properties
case_xml=test_v1_cluster.xml
cd ${ROOT_DIR}/src/sdk/java/
mvn install -Dmaven.test.skip=true
cd ${ROOT_DIR}/src/sdk/java/fesql-auto-test-java
mvn test -DsuiteXmlFile=test_suite/${case_xml} -DcaseLevel=$CASE_LEVEL