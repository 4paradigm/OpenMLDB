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

#udf_defs.yaml 一般改动很小 为了节省性能 我们不用每次生成
#export JAVA_HOME=${FEDB_DEV_JAVA_HOME:-/depends/thirdparty/jdk1.8.0_141}
#export FEDB_THIRDPARTY=${FEDB_DEV_THIRDPARTY:-/depends/thirdparty}
#cd fesql && ln -sf ${FEDB_THIRDPARTY} thirdparty && mkdir -p build
#cd build && cmake .. && make fesql_proto && make fesql_parser && make -j5
#cd ${ROOT_DIR}
#./fesql/build/src/export_udf_info --output_file=fesql/tools/autotest/udf_defs.yaml

python3 -m pip install numpy -i https://pypi.tuna.tsinghua.edu.cn/simple
python3 -m pip install PyYaml -i https://pypi.tuna.tsinghua.edu.cn/simple

#python3 fesql/tools/autotest/gen_case_yaml_main.py  \
#    --udf_path=fesql/tools/autotest/udf_defs.yaml --yaml_count=1
sh tools/install_hybridse.sh
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
sed -i
sed -i "s/log4j\.rootLogger.*/log4j\.rootLogger=debug,stdout,warn,error/g" src/sdk/java/fesql-auto-test-java/src/main/resources/log4j.properties
case_xml=test_auto_gen_case_standalone.xml
cd ${ROOT_DIR}/src/sdk/java/
mvn install -Dmaven.test.skip=true
cd ${ROOT_DIR}/src/sdk/java/fesql-auto-test-java
mvn test -DsuiteXmlFile=test_suite/${case_xml}
