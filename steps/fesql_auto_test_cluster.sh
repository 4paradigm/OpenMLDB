#!/usr/bin/env bash

# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ROOT_DIR=`pwd`
ulimit -c unlimited
CASE_LEVEL=$1
if [[ "${CASE_LEVEL}" == "" ]]; then
        CASE_LEVEL="0"
fi
if [[ "${YAML_CASE_BASE_DIR}" == "" ]]; then
        YAML_CASE_BASE_DIR=${ROOT_DIR}
fi
echo "fesql auto test cluster: case_level ${CASE_LEVEL}"
echo "ROOT_DIR:${ROOT_DIR}"
sh tools/install_hybridse.sh

mkdir -p ${ROOT_DIR}/build  && cd ${ROOT_DIR}/build && cmake .. 
make -j5 sql_javasdk_package || { echo "compile error"; exit 1; }
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
mvn test -DsuiteXmlFile=test_suite/${case_xml} -DcaseLevel=$CASE_LEVEL -DyamlCaseBaseDir=$YAML_CASE_BASE_DIR
