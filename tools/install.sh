#!/bin/bash
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

set -e
if ! [ -x "$(command -v java)" ]; then
  echo 'Error: java is not installed.' >&2
  exit 1
fi

if ! [ -x "$(command -v wget)" ]; then
  echo 'Error: wget is not installed.' >&2
  exit 1
fi

echo 'install zookeeper'
FILE=zookeeper-3.4.14.tar.gz
ZK=zookeeper-3.4.14
if [ -e "${FILE}" ]; then
    echo "${FILE} exist"
else 
  wget "https://archive.apache.org/dist/zookeeper/${ZK}/${ZK}.tar.gz"
  tar -zxvf ${FILE}
  cd ${ZK}
  cp conf/zoo_sample.cfg conf/zoo.cfg
  cd ..
fi

echo 'install nameserver and tablet'
FILE=openmldb-0.2.2-linux.tar.gz
if [ -e "${FILE}" ]; then
  echo "${FILE} exist"
else
  wget https://github.com/4paradigm/OpenMLDB/releases/download/0.2.2/openmldb-0.2.2-linux.tar.gz
  if [ -e "${FILE}" ]; then
    tar -zxvf openmldb-0.2.2-linux.tar.gz
    mv openmldb-0.2.2-linux openmldb-ns-0.2.2
    echo 'install tablet'
    tar -zxvf openmldb-0.2.2-linux.tar.gz
    mv openmldb-0.2.2-linux openmldb-tablet-2.2.0
  else
    echo 'download fail ; try script again'
    exit 1
  fi
fi

echo 'install complete'

echo 'starting zookeeper'
cd ${ZK}
dataDir="./data"
clientPort="6181"
#modify zookeeper config
sed -i "s:dataDir=[a-zA-Z0-9//]*:dataDir=${dataDir}:" conf/zoo.cfg
sed -i "s:clientPort=[0-9]*:clientPort=${clientPort}:" conf/zoo.cfg
PID=$(lsof -i :${clientPort}|grep -v "PID" | awk '{print $2}')
echo "${PID}"
if [ "${PID}" != "" ]; then
   echo "zk port is unavailable"
   exit 1
fi
bash bin/zkServer.sh start
cd ..
echo 'zookeeper OK'

echo 'starting nameserver'
cd openmldb-ns-0.2.2
IP=$(hostname -i)
#get available port
ns_port=6527
PID=$(lsof -i :${ns_port}|grep -v "PID" | awk '{print $2}')
if [ "${PID}" != "" ]; then
   echo "ns port is unavailable; try to get another port!"
   ns_port=0
   while [ "${ns_port}" -eq 0 ]; do
      temp1=$(shuf -i 1024-10000 -n1)
      PID=$(lsof -i :"${temp1}"|grep -v "PID" | awk '{print $2}')
      if [ "${PID}" != "" ] ; then
         echo "try to get another port!"
      else
         ns_port=${temp1}
      fi
   done
   echo "${ns_port}"
fi
#modify nameserver config
sed -i "s:--endpoint=[a-zA-Z0-9//.:]*:--endpoint=${IP}\:${ns_port}:" conf/nameserver.flags
sed -i "s:--role=[a-zA-Z0-9//.:]*:--role=nameserver:" conf/nameserver.flags
sed -i "s:--zk_cluster=[a-zA-Z0-9//.:]*:--zk_cluster=${IP}\:${clientPort}:" conf/nameserver.flags
sed -i "s:--zk_root_path=[a-zA-Z0-9//.:]*:--zk_root_path=/openmldb_cluster:" conf/nameserver.flags
sed -i "s:--enable_distsql=[a-zA-Z0-9//.:]*:--enable_distsql=true:" conf/nameserver.flags
sh bin/start.sh start nameserver
cd ..
echo 'nameserver Ok'
 
echo 'starting tablet'
cd openmldb-tablet-2.2.0
IP=$(hostname -i)
#get available port
tablet_port=9527
PID=$(lsof -i :${tablet_port}|grep -v "PID" | awk '{print $2}')
if [ "${PID}" != "" ]; then
   echo "ns port is unavailable; try to get another port!"
   tablet_port=0
   while [ "${tablet_port}" -eq 0 ]; do
      temp1=$(shuf -i 1024-10000 -n1)
      PID=$(lsof -i :"${temp1}"|grep -v "PID" | awk '{print $2}')
      if [ "${PID}" != "" ] ; then
         echo "try to get another port!"
      else
         tablet_port=${temp1}
      fi
   done
   echo "${tablet_port}"
fi
#modify tablet config
sed -i "s:--endpoint=[a-zA-Z0-9//.:]*:--endpoint=${IP}\:${ns_port}:" conf/tablet.flags
sed -i "s:--role=[a-zA-Z0-9//.:]*:--role=tablet:" conf/tablet.flags
sed -i "s:--zk_cluster=[a-zA-Z0-9//.:]*:--zk_cluster=${IP}\:${clientPort}:" conf/tablet.flags
sed -i "s:--zk_root_path=[a-zA-Z0-9//.:]*:--zk_root_path=/openmldb_cluster:" conf/tablet.flags
sed -i "s:--enable_distsql=[a-zA-Z0-9//.:]*:--enable_distsql=true:" conf/tablet.flags
sh bin/start.sh start tablet
cd ..
echo 'tablet Ok'

echo 'employ complete'


