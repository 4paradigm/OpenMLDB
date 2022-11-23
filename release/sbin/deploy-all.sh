#! /usr/bin/env bash
# shellcheck disable=SC1091

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

home="$(cd "$(dirname "$0")"/.. || exit; pwd)"
sbin="$(cd "$(dirname "$0")" || exit; pwd)"
. "$home"/conf/openmldb-env.sh
. "$sbin"/init.sh

distribute() {
  host=$1
  dest=$2
  src="$home"
  type=openmldb
  if [[ $# -ge 3 ]]; then
    src=$3
  fi
  if [[ $# -ge 4 ]]; then
    type=$4
  fi
  if [[ "$src" = "$dest" ]]; then
    echo "dest = src: $dest, skip copy"
  fi
  ssh -n "$host" "mkdir -p $dest"
  echo "$src/* $host:$dest/"
  if [[ "$type" = "openmldb" ]]; then
    for folder in bin sbin conf
    do
      rsync -arz "$src/$folder"/ "$host:$dest/$folder"/
    done
  else
    rsync -arz "$src"/* "$host:$dest"/
    rsync -arz "$home"/sbin/deploy.sh "$host:$dest"/sbin/
  fi
}

echo "OPENMLDB envs:"
printenv | grep OPENMLDB_
echo ""

old_IFS="$IFS"
IFS=$'\n'
# deploy tablets
for line in $(parse_host conf/hosts tablet)
do
  host=$(echo "$line" | awk -F ' ' '{print $1}')
  port=$(echo "$line" | awk -F ' ' '{print $2}')
  dir=$(echo "$line" | awk -F ' ' '{print $3}')

  echo "deploy tablet to $host:$port $dir"
  distribute "$host" "$dir"
  ssh -n "$host" "cd $dir; OPENMLDB_HOST=$host OPENMLDB_TABLET_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} sbin/deploy.sh tablet"
done

# deploy nameservers
for line in $(parse_host conf/hosts nameserver)
do
  host=$(echo "$line" | awk -F ' ' '{print $1}')
  port=$(echo "$line" | awk -F ' ' '{print $2}')
  dir=$(echo "$line" | awk -F ' ' '{print $3}')

  echo "deploy nameserver to $host:$port $dir"
  distribute "$host" "$dir"
  ssh -n "$host" "cd $dir; OPENMLDB_HOST=$host OPENMLDB_NAMESERVER_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} sbin/deploy.sh nameserver"
done

# deploy apiservers
for line in $(parse_host conf/hosts apiserver)
do
  host=$(echo "$line" | awk -F ' ' '{print $1}')
  port=$(echo "$line" | awk -F ' ' '{print $2}')
  dir=$(echo "$line" | awk -F ' ' '{print $3}')

  echo "deploy apiserver to $host:$port $dir"
  distribute "$host" "$dir"
  ssh -n "$host" "cd $dir; OPENMLDB_HOST=$host OPENMLDB_APISERVER_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} sbin/deploy.sh apiserver"
done

# deploy openmldbspark
if [[ -z "${SPARK_HOME}" ]]; then
  echo "[ERROR] SPARK_HOME is not set"
else
  if [[ ! -e "${SPARK_HOME}" ]]; then
    echo "Downloading openmldbspark..."
    spark_name=spark-3.2.1-bin-openmldbspark
    spark_tar="${spark_name}".tgz
    if [[ -e "${spark_tar}" ]]; then
      echo "Skip downloading openmldbspark as ${spark_tar} already exists"
    else
      curl -SLo ${spark_tar} "https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.6.6/${spark_tar}"
    fi
    tar -xzf ${spark_tar}
    ln -s "$(pwd)"/"${spark_name}" "${SPARK_HOME}"
  else
    echo "${SPARK_HOME} already exists. Skip deploy spark."
  fi
fi

# deploy taskmanager locally
OPENMLDB_TASKMANAGER_PORT="$OPENMLDB_TASKMANAGER_PORT" sbin/deploy.sh taskmanager

# deploy zookeeper
# TODO: base on the OPENMLDB_ZK_CLUSTER and change the port
if [[ "${OPENMLDB_USE_EXISTING_ZK_CLUSTER}" != "true" ]]; then
  if [[ ! -e "$OPENMLDB_ZK_HOME" ]]; then
    zk_name=zookeeper-3.4.14
    zk_tar=${zk_name}.tar.gz
    if [[ -e ${zk_tar} ]]; then
      echo "Skip downloading zookeeper as ${zk_tar} already exists"
    else
      echo "Downloading zookeeper..."
      curl -SLo ${zk_tar} https://archive.apache.org/dist/zookeeper/${zk_name}/${zk_tar}
    fi
    tar -zxf "${zk_tar}"
    ln -s "$zk_name" "$OPENMLDB_ZK_HOME"
  else
    echo "${OPENMLDB_ZK_HOME} already exists. Skip download zookeeper."
  fi

  i=1
  quorum_config=
  for line in $(parse_host conf/hosts zookeeper)
  do
    host=$(echo "$line" | awk -F ' ' '{print $1}')
    port=$(echo "$line" | awk -F ' ' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $3}')
    peer_port=$(echo "$line" | awk -F ' ' '{print $4}')
    election_port=$(echo "$line" | awk -F ' ' '{print $5}')

    server="server.${i}=${host}:${peer_port}:${election_port}"
    if [[ -z "$quorum_config" ]]; then
      quorum_config="${server}"
    else
      quorum_config="${quorum_config}|${server}"
    fi
    i=$((i+1))
  done

  i=1
  for line in $(parse_host conf/hosts zookeeper)
  do
    host=$(echo "$line" | awk -F ' ' '{print $1}')
    port=$(echo "$line" | awk -F ' ' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $3}')
    peer_port=$(echo "$line" | awk -F ' ' '{print $4}')
    election_port=$(echo "$line" | awk -F ' ' '{print $5}')

    echo "deploy zookeeper to $host:$port $dir"
    distribute "$host" "$dir" "$OPENMLDB_ZK_HOME" zookeeper
    ssh -n "$host" "cd $dir; OPENMLDB_ZK_HOME=$dir OPENMLDB_ZK_QUORUM=\"$quorum_config\" OPENMLDB_ZK_MYID=$i OPENMLDB_ZK_CLUSTER_CLIENT_PORT=$port sbin/deploy.sh zookeeper"
    i=$((i+1))
  done
fi
IFS="$old_IFS"