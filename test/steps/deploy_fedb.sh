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

SERVER_VERSION=$1
DEPLOY_MODE=$2
echo "SERVER_VERSION:${SERVER_VERSION}"
echo "DEPLOY_MODE:${DEPLOY_MODE}"
#IP=`ifconfig | grep 'inet addr' | head -n 1 |awk '{print $2}' | awk -F ':' '{print $2}'`
IP=$(hostname -i)
ZK_PORT=6181
ZK_CLUSTER=$IP:$ZK_PORT
NS1=$IP:9622
NS2=$IP:9623
#NS3=$IP:9624
TABLET1=$IP:9520
TABLET2=$IP:9521
TABLET3=$IP:9522
API_SERVER=$IP:9530
echo "IP:${IP}"
echo "ZK_PORT:${ZK_PORT}"
echo "NS1:${NS1}"
echo "NS2:${NS2}"
echo "TABLET1:${TABLET1}"
echo "TABLET2:${TABLET2}"
echo "TABLET3:${TABLET3}"
echo "API_SERVER:${API_SERVER}"
# deploy zk
wget http://pkg.4paradigm.com:81/rtidb/test/zookeeper-3.4.14.tar.gz
tar -zxvf zookeeper-3.4.14.tar.gz
cd zookeeper-3.4.14 || exit
cp conf/zoo_sample.cfg conf/zoo.cfg || exit
sed -i 's#dataDir=/tmp/zookeeper#dataDir=./data#' conf/zoo.cfg
sed -i "s#clientPort=2181#clientPort=${ZK_PORT}#" conf/zoo.cfg
sh bin/zkServer.sh start
cd .. || exit
sleep 5
# deploy ns
function deploy_ns() {
  local ns_name=$1
  local ns_endpoint=$2
  cp -r fedb-cluster-"${SERVER_VERSION}" "${ns_name}"
  cd "${ns_name}" || exit
  sed -i "s#--zk_cluster=.*#--zk_cluster=${ZK_CLUSTER}#" conf/nameserver.flags
  echo '--request_timeout_ms=60000' >> conf/nameserver.flags
  sed -i "s#--endpoint=.*#--endpoint=${ns_endpoint}#" conf/nameserver.flags
  if [[ "${DEPLOY_MODE}" == "cluster" ]]; then
    sed -i 's#--enable_distsql=.*#--enable_distsql=true#' conf/nameserver.flags
  else
    sed -i 's#--enable_distsql=.*#--enable_distsql=false#' conf/nameserver.flags
  fi
  sh bin/start.sh start nameserver
  cd .. || exit
  sleep 5
}
wget http://pkg.4paradigm.com:81/rtidb/test/fedb-"${SERVER_VERSION}"-linux.tar.gz
tar -zxvf fedb-"${SERVER_VERSION}"-linux.tar.gz
# shellcheck disable=SC2010
pkg_name=$(ls | grep fedb-cluster)
echo "pkg_name:${pkg_name}"
deploy_ns fedb-ns-1 ${NS1}
deploy_ns fedb-ns-2 ${NS2}
# deploy tablet
function deploy_tablet() {
  local tablet_name=$1
  local tablet_endpoint=$2
  cp -r fedb-cluster-"${SERVER_VERSION}" ${tablet_name}
  cd "${tablet_name}" || exit
  sed -i "s#--zk_cluster=.*#--zk_cluster=${ZK_CLUSTER}#" conf/tablet.flags
  sed -i 's@--zk_root_path=.*@--zk_root_path=/fedb@' conf/tablet.flags
  sed -i "s#--endpoint=.*#--endpoint=${tablet_endpoint}#" conf/tablet.flags
  if [[ "${DEPLOY_MODE}" == "cluster" ]]; then
    sed -i 's#--enable_distsql=.*#--enable_distsql=true#' conf/tablet.flags
  else
    sed -i 's#--enable_distsql=.*#--enable_distsql=false#' conf/tablet.flags
  fi
  sh bin/start.sh start tablet
  cd ..
  sleep 5
}
deploy_tablet fedb-tablet-1 ${TABLET1}
deploy_tablet fedb-tablet-2 ${TABLET2}
deploy_tablet fedb-tablet-3 ${TABLET3}
# deploy api server
function deploy_api_server() {
  local api_serve_name=$1
  local api_serve_endpoint=$API_SERVER
  cp -r fedb-cluster-"${SERVER_VERSION}" "${api_serve_name}"
  cd "${api_serve_name}" || exit
  sed -i "s#--zk_cluster=.*#--zk_cluster=${ZK_CLUSTER}#" conf/apiserver.flags
  sed -i 's@--zk_root_path=.*@--zk_root_path=/fedb@' conf/apiserver.flags
  sed -i "s#--endpoint=.*#--endpoint=${api_serve_endpoint}#" conf/apiserver.flags
  sed -i "s#--role=.*#--role=apiserver#" conf/apiserver.flags
#  if [[ "${DEPLOY_MODE}" == "cluster" ]]; then
#    sed -i 's#--enable_distsql=.*#--enable_distsql=true#' conf/tablet.flags
#  else
#    sed -i 's#--enable_distsql=.*#--enable_distsql=false#' conf/tablet.flags
#  fi
  sh bin/start.sh start apiserver
  cd ..
  sleep 5
}
deploy_api_server fedb-apiserver-1