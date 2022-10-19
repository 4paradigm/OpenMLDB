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


#bash openmldb-sdk-test-java.sh -b SRC -c test_all.xml -d cluster -l 0
#-b SRC表示从源码进行编译，会从github上下载代码然后进行编译，PKG表示直接从github上下载压缩包部署
#-c 执行的suite_xml,决定了跑哪些case
#-d 部署模式，有cluster和standalone两种，默认cluster
#-l 测试的case级别，有0，1，2，3，4，5六个级别，默认为0，也可以同时跑多个级别的case，例如：1,2,3,4,5

while getopts ":c:t:s:v:" opt
do
   case $opt in
        c)
        echo "参数c的值:$OPTARG"
        CASE_XML=$OPTARG
        ;;
        t)
        echo "参数t的值:$OPTARG"
        TEST_TYPE=$OPTARG
        ;;
        s)
        echo "参数s的值:$OPTARG"
        TABLE_STORAGE_MODE=$OPTARG
        ;;
        v)
        echo "参数v的值:$OPTARG"
        PRE_UPGRADE_VERSION=$OPTARG
        ;;
        ?) echo "未知参数"
           exit 1
        ;;
   esac
done
if [[ "${CASE_XML}" == "" ]]; then
    CASE_XML="test_all.xml"
fi
if [[ "${TEST_TYPE}" == "" ]]; then
    TEST_TYPE="upgrade"
fi

echo "CASE_XML:${CASE_XML}"
echo "TEST_TYPE:${TEST_TYPE}"
echo "TABLE_STORAGE_MODE:${TABLE_STORAGE_MODE}"

ROOT_DIR=$(pwd)
# 安装wget
yum install -y wget
yum install -y net-tools
ulimit -c unlimited
echo "ROOT_DIR:${ROOT_DIR}"

# 从源码编译
deployConfigPath="test/integration-test/openmldb-test-java/openmldb-deploy/src/main/resources/deploy.properties"
OPENMLDB_SERVER_VERSION="SRC"
SERVER_URL=$(more ${deployConfigPath} | grep "${OPENMLDB_SERVER_VERSION}")
echo "SERVER_URL:${SERVER_URL}"
if [[ "${SERVER_URL}" == "" ]]; then
  echo -e "\n${OPENMLDB_SERVER_VERSION}=${ROOT_DIR}/openmldb-linux.tar.gz\n" >> ${deployConfigPath}
else
  sed -i "s#${OPENMLDB_SERVER_VERSION}=.*#${OPENMLDB_SERVER_VERSION}=${ROOT_DIR}/openmldb-linux.tar.gz#" ${deployConfigPath}
fi

JAVA_SDK_VERSION=$(more java/pom.xml | grep "<version>.*</version>" | head -1 | sed 's#.*<version>\(.*\)</version>.*#\1#')
JAVA_NATIVE_VERSION=$(more java/pom.xml | grep "<version>.*</version>" | head -1 | sed 's#.*<version>\(.*\)</version>.*#\1#')
sh test/steps/build-java-sdk.sh

echo "JAVA_SDK_VERSION:${JAVA_SDK_VERSION}"
echo "JAVA_NATIVE_VERSION:${JAVA_NATIVE_VERSION}"
echo "deploy config:"
cat ${deployConfigPath}
# install command tool
cd test/test-tool/command-tool || exit
mvn clean install -Dmaven.test.skip=true
cd "${ROOT_DIR}" || exit
# modify config
sh test/steps/modify_devops_config.sh "${CASE_XML}" "${PRE_UPGRADE_VERSION}" "${JAVA_SDK_VERSION}" "" "${OPENMLDB_SERVER_VERSION}" "${JAVA_NATIVE_VERSION}" "${TABLE_STORAGE_MODE}"

# install jar
cd test/integration-test/openmldb-test-java || exit
mvn clean install -Dmaven.test.skip=true
# run case
cd "${ROOT_DIR}"/test/integration-test/openmldb-test-java/openmldb-devops-test || exit
mvn clean test -e -U -Dsuite=test_suite/"${CASE_XML}"

if [[ "${TEST_TYPE}" == "upgrade" ]]; then
  if [[ "${TABLE_STORAGE_MODE}" == "memory" ]]; then
    SDK_CASE_XML="test_cluster.xml"
  else
    SDK_CASE_XML="test_cluster_disk.xml"
  fi
  echo "SDK_CASE_XML:${SDK_CASE_XML}"
  # run case
  cd "${ROOT_DIR}"/test/integration-test/openmldb-test-java/openmldb-sdk-test || exit
  mvn clean test -e -U -DsuiteXmlFile=test_suite/"${SDK_CASE_XML}" -DcaseLevel="0"
fi
