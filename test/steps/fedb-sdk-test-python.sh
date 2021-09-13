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


#bash fedb-sdk-test-python.sh -b SRC -d cluster -l 0
#-b SRC表示从源码进行编译，会从github上下载代码然后进行编译，PKG表示直接从github上下载压缩包部署
#-d 部署模式，有cluster和standalone两种，默认cluster
#-l 测试的case级别，有0，1，2，3，4，5六个级别，默认为0，也可以同时跑多个级别的case，例如：1,2,3,4,5

while getopts ":b:c:d:l:" opt
do
   case $opt in
        b)
        echo "参数b的值:$OPTARG"
        BUILD_MODE=$OPTARG
        ;;
        d)
        echo "参数d的值:$OPTARG"
        DEPLOY_MODE=$OPTARG
        ;;
        l) echo "参数l的值:$OPTARG"
        CASE_LEVEL=$OPTARG
        ;;
        ?) echo "未知参数"
           exit 1
        ;;
   esac
done
if [[ "${BUILD_MODE}" == "" ]]; then
    BUILD_MODE="PKG"
fi
if [[ "${DEPLOY_MODE}" == "" ]]; then
    DEPLOY_MODE="cluster"
fi
if [[ "${CASE_LEVEL}" == "" ]]; then
    CASE_LEVEL="0"
fi

echo "BUILD_MODE:${BUILD_MODE}"
echo "DEPLOY_MODE:${DEPLOY_MODE}"
echo "CASE_LEVEL:${CASE_LEVEL}"
ROOT_DIR=$(pwd)
echo "ROOT_DIR:${ROOT_DIR}"
source steps/read_properties.sh
sh steps/download-case.sh "${CASE_BRANCH}"
echo "FEDB_SERVER_VERSION:${FEDB_SERVER_VERSION}"
echo "FEDB_PY_SDK_VERSION:${FEDB_PY_SDK_VERSION}"

# 安装wget
yum install -y wget
yum install -y  net-tools
python3 -m ensurepip
# 部署fedb
if [[ "${BUILD_MODE}" == "SRC" ]]; then
    IP=127.0.0.1
    # 下载编译fedb
    sh steps/build-fedb.sh
    # 部署zk
    cd OpenMLDB || exit
    test -d /rambuild/ut_zookeeper && rm -rf /rambuild/ut_zookeeper/*
    cp steps/zoo.cfg thirdsrc/zookeeper-3.4.14/conf
    cd thirdsrc/zookeeper-3.4.14 || exit
    netstat -anp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs kill -9
    ./bin/zkServer.sh start
    cd "$ROOT_DIR" || exit
    sleep 5

    # 部署fedb cluster
    cd OpenMLDB || exit
    if [[ "${DEPLOY_MODE}" == "cluster" ]]; then
        cd onebox && sh start_onebox_on_rambuild_cluster.sh
        cd "$ROOT_DIR" || exit
    else
        cd onebox && sh start_onebox_on_rambuild.sh
        cd "$ROOT_DIR" || exit
    fi
    sleep 5
    # 安装fedb模块
    cd "${ROOT_DIR}"/OpenMLDB/build/python/dist || exit
    # shellcheck disable=SC2010
    whl_name=$(ls | grep .whl)
    echo "whl_name:${whl_name}"
#    python3 -m pip install ${whl_name} -i https://pypi.tuna.tsinghua.edu.cn/simple
    python3 -m pip --default-timeout=100 install -U "${whl_name}"
else
    IP=$(hostname -i)
    sh steps/deploy_fedb.sh "${FEDB_SERVER_VERSION}" "${DEPLOY_MODE}"
    cd "$ROOT_DIR" || exit
    wget http://pkg.4paradigm.com:81/rtidb/test/fedb-"${FEDB_PY_SDK_VERSION}"-py3-none-any.whl
    # shellcheck disable=SC2010
    whl_name=$(ls | grep .whl)
#    python3 -m pip install ${whl_name} -i https://pypi.tuna.tsinghua.edu.cn/simple
    python3 -m pip --default-timeout=100 install -U "${whl_name}"
fi
#sleep 5
## 安装fedb模块
#cd ${ROOT_DIR}/fedb/build/python/dist
#whl_name=`ls | grep *.whl`
#echo "whl_name:${whl_name}"
#python3 -m pip install ${whl_name} -i https://pypi.tuna.tsinghua.edu.cn/simple

cd "$ROOT_DIR" || exit
cd python/fedb-sdk-test || exit
# 安装其他模块
#python3 -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
python3 -m pip install -r requirements.txt
# 修改配置
sed -i "s/env=.*/env=cicd/" conf/fedb.conf
sed -i "s/levels=.*/levels=${CASE_LEVEL}/" conf/fedb.conf
{
  echo "cicd_tb_endpoint_0=$IP:9520"
  echo "cicd_tb_endpoint_1=$IP:9521"
  echo "cicd_tb_endpoint_2=$IP:9522"
} >> conf/fedb.conf
if [[ "${DEPLOY_MODE}" == "cluster" ]]; then
    sed -i "s#cicd_zk_root_path=.*#cicd_zk_root_path=/cluster#" conf/fedb.conf
fi
if [[ "${BUILD_MODE}" == "PKG" ]]; then
    sed -i "s#cicd_zk_cluster=.*#cicd_zk_cluster=$IP:6181#" conf/fedb.conf
    sed -i "s#cicd_zk_root_path=.*#cicd_zk_root_path=/fedb#" conf/fedb.conf
fi
cat conf/fedb.conf

pytest -s test/ --alluredir "${ROOT_DIR}"/python/report/allure-results
