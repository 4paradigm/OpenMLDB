#! /usr/bin/env bash

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

home="$(cd "`dirname "$0"`"/..; pwd)"

. $home/conf/openmldb-env.sh
. $home/bin/init.sh

distribute() {
  host=$1
  dest=$2
  if [[ $home = $dest ]]; then
    echo "dest ($home) = src ($dest), skip copy"
  fi
  ssh $host "mkdir -p $dest > /dev/null 2>&1"
  for folder in bin conf
  do
    rsync -arz $home/$folder/ $host:$dest/$folder/
  done
}

old_IFS=$IFS
IFS=$'\n'

# deploy tablets
for line in $(cat conf/tablets | sed  "s/#.*$//;/^$/d")
do
  host_port=$(echo $line | awk -F ' ' '{print $1}')
  host=$(echo ${host_port} | awk -F ':' '{print $1}')
  port=$(echo ${host_port} | awk -F ':' '{print $2}')
  dir=$(echo $line | awk -F ' ' '{print $2}')

  if [[ -z $dir ]]; then
    dir=${OPENMLDB_HOME}
  fi
  if [[ -z $port ]]; then
    port=${OPENMLDB_TABLET_PORT}
  fi
  echo "deploy tablet to $host:$port $dir"
  distribute $host ${dir}
  ssh $host "cd $dir; OPENMLDB_HOST=$host OPENMLDB_TABLET_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} bin/deploy.sh tablet"
done

# deploy nameservers
for line in $(cat conf/nameservers | sed  "s/#.*$//;/^$/d")
do
  host_port=$(echo $line | awk -F ' ' '{print $1}')
  host=$(echo ${host_port} | awk -F ':' '{print $1}')
  port=$(echo ${host_port} | awk -F ':' '{print $2}')
  dir=$(echo $line | awk -F ' ' '{print $2}')

  if [[ -z $dir ]]; then
    dir=${OPENMLDB_HOME}
  fi
  if [[ -z $port ]]; then
    port=${OPENMLDB_TABLET_PORT}
  fi
  echo "deploy nameserver to $host:$port $dir"
  distribute $host $dir
  ssh $host "cd $dir; OPENMLDB_HOST=$host OPENMLDB_NAMESERVER_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} bin/deploy.sh nameserver"
done

# deploy apiservers
for line in $(cat conf/apiservers | sed  "s/#.*$//;/^$/d")
do
  host_port=$(echo $line | awk -F ' ' '{print $1}')
  host=$(echo ${host_port} | awk -F ':' '{print $1}')
  port=$(echo ${host_port} | awk -F ':' '{print $2}')
  dir=$(echo $line | awk -F ' ' '{print $2}')

  if [[ -z $dir ]]; then
    dir=${OPENMLDB_HOME}
  fi
  if [[ -z $port ]]; then
    port=${OPENMLDB_TABLET_PORT}
  fi
  echo "deploy apiserver to $host:$port $dir"
  distribute $host $dir
  ssh $host "cd $dir; OPENMLDB_HOST=$host OPENMLDB_APISERVER_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} bin/deploy.sh apiserver"
done

# deploy openmldbspark
if [[ -z ${SPARK_HOME} ]]; then
  echo "[ERROR] SPARK_HOME is not set"
else
  if [[ ! -e ${SPARK_HOME} ]]; then
    echo "Downloading openmldbspark..."
    spark_name=spark-3.2.1-bin-openmldbspark
    spark_tar=${spark_name}.tgz
    if [[ -e ${spark_tar} ]]; then
      echo "Skip downloading openmldbspark as ${spark_tar} already exists"
    else
      curl -SLo ${spark_tar} "https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.6.6/${spark_tar}"
    fi
    tar -xzf ${spark_tar}
    ln -s `pwd`/${spark_name} ${SPARK_HOME}
  else
    echo "${SPARK_HOME} already exists. Skip deploy spark."
  fi
fi

# deploy taskmanager
cp conf/taskmanager.properties.template conf/taskmanager.properties

# deploy zookeeper
# TODO: base on the OPENMLDB_ZK_CLUSTER and change the port
if [[ "${OPENMLDB_USE_EXISTING_ZK_CLUSTER}" != "true" ]]; then
  if [[ ! -e ${ZK_HOME} ]]; then
    zk_name=zookeeper-3.4.14
    zk_tar=${zk_name}.tar.gz
    if [[ -e ${zk_tar} ]]; then
      echo "Skip downloading zookeeper as ${zk_tar} already exists"
    else
      echo "Downloading zookeeper..."
      curl -SLo ${zk_tar} https://archive.apache.org/dist/zookeeper/${zk_name}/${zk_tar}
    fi
    tar -zxf ${zk_tar}
    ln -s `pwd`/${zk_name} ${ZK_HOME}
    cp ${ZK_HOME}/conf/zoo_sample.cfg ${ZK_HOME}/conf/zoo.cfg
  else
    echo "${ZK_HOME} already exists. Skip deploy zookeeper."
  fi
fi

IFS=$old_IFS