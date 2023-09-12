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

home="$(cd "$(dirname "$0")"/.. || exit 1; pwd)"
sbin="$(cd "$(dirname "$0")" || exit 1; pwd)"
. "$home"/conf/openmldb-env.sh
. "$sbin"/init.sh
cd "$home" || exit 1

distribute() {
  local host=$1
  local dest=$2
  local src="$home"
  local type=openmldb
  if [[ $# -ge 3 ]]; then
    src=$3
  fi
  if [[ $# -ge 4 ]]; then
    type=$4
  fi
  local use_ssh=true
  if [[ $host = "localhost" || $host = "127.0.0.1" ]]; then
    use_ssh=false
    if [[ "$dest" = "$src" ]]; then
      echo "skip rsync as dest=src: $dest"
      return
    fi
  fi

  local cmd="mkdir -p $dest > /dev/null 2>&1"
  run_auto "$host" "$cmd"

  if [[ "$use_ssh" = true ]]; then
    full_dest="$host:$dest"
  else
    full_dest=$dest
  fi
  echo "copy $src to $host:$dest"
  if [[ "$type" = "zookeeper" ]]; then
    rsync -arz "$src"/* "$full_dest"/
    rsync -arz "$home"/sbin/deploy.sh "$full_dest"/sbin/
  else
    if [[ "$type" = "taskmanager" ]]; then
      dir_list=(bin sbin conf taskmanager)
      if [[ "$use_ssh" = true ]]; then
        rsync -arz "${SPARK_HOME}/" "$host:${SPARK_HOME}/"
      fi
    else
      dir_list=(bin sbin conf udf)
    fi
    for folder in "${dir_list[@]}"
    do
      rsync -arz "$src/$folder"/ "$full_dest/$folder"/
    done
  fi
}

echo "OPENMLDB envs:"
printenv | grep OPENMLDB_
printenv | grep SPARK_HOME
echo ""

if [[ "$OPENMLDB_MODE" != "cluster" ]]; then
  echo "Deploy is only needed when OPENMLDB_MODE=cluster"
  exit 2
fi

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
  cmd="cd $dir && OPENMLDB_HOST=$host OPENMLDB_TABLET_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} sbin/deploy.sh tablet"
  run_auto "$host" "$cmd"
done

# deploy nameservers
for line in $(parse_host conf/hosts nameserver)
do
  host=$(echo "$line" | awk -F ' ' '{print $1}')
  port=$(echo "$line" | awk -F ' ' '{print $2}')
  dir=$(echo "$line" | awk -F ' ' '{print $3}')

  echo "deploy nameserver to $host:$port $dir"
  distribute "$host" "$dir"
  cmd="cd $dir && OPENMLDB_HOST=$host OPENMLDB_NAMESERVER_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} sbin/deploy.sh nameserver"
  run_auto "$host" "$cmd"
done

# deploy apiservers
for line in $(parse_host conf/hosts apiserver)
do
  host=$(echo "$line" | awk -F ' ' '{print $1}')
  port=$(echo "$line" | awk -F ' ' '{print $2}')
  dir=$(echo "$line" | awk -F ' ' '{print $3}')

  echo "deploy apiserver to $host:$port $dir"
  distribute "$host" "$dir"
  cmd="cd $dir && OPENMLDB_HOST=$host OPENMLDB_APISERVER_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} sbin/deploy.sh apiserver"
  run_auto "$host" "$cmd"
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
      #url="https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb${OPENMLDB_VERSION}/${spark_tar}"
      # TODO: Change to 0.7.1
      url="https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.7.1/${spark_tar}"
      echo "Download spark from $url"
      curl -SLo ${spark_tar} "$url"
    fi
    tar -xzf ${spark_tar}
    ln -s "$(pwd)"/"${spark_name}" "${SPARK_HOME}"
  else
    echo "${SPARK_HOME} already exists. Skip deploy spark locally"
  fi
fi

# deploy taskmanagers
for line in $(parse_host conf/hosts taskmanager)
do
  host=$(echo "$line" | awk -F ' ' '{print $1}')
  port=$(echo "$line" | awk -F ' ' '{print $2}')
  dir=$(echo "$line" | awk -F ' ' '{print $3}')

  echo "deploy taskmanager to $host:$port $dir"
  distribute "$host" "$dir" "$home" taskmanager
  cmd="cd $dir && OPENMLDB_VERSION=${OPENMLDB_VERSION} SPARK_HOME=${SPARK_HOME} OPENMLDB_HOST=$host OPENMLDB_TASKMANAGER_PORT=$port OPENMLDB_ZK_CLUSTER=${OPENMLDB_ZK_CLUSTER} OPENMLDB_ZK_ROOT_PATH=${OPENMLDB_ZK_ROOT_PATH} sbin/deploy.sh taskmanager"
  run_auto "$host" "$cmd"
done

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
    cmd="cd $dir && OPENMLDB_HOST=$host OPENMLDB_ZK_HOME=$dir OPENMLDB_ZK_QUORUM=\"$quorum_config\" OPENMLDB_ZK_MYID=$i OPENMLDB_ZK_CLUSTER_CLIENT_PORT=$port sbin/deploy.sh zookeeper"
    run_auto "$host" "$cmd"
    i=$((i+1))
  done
fi
IFS="$old_IFS"
