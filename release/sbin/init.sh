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

function parse_host {
  host_file=$1
  type=$2

  start=false
  grep -v '^ *#' < "$host_file" | while IFS= read -r line
  do
    if [[ -z "$line" ]]; then
      continue
    elif [[ "$line" = "[$type]" ]]; then
      start=true
      continue
    elif echo "$line" | grep -q "^ *\["; then
      start=false
    fi

    if [[ "$start" = false ]]; then
      continue
    fi

    host_port=$(echo "$line" | awk -F ' ' '{print $1}')
    host=$(echo "${host_port}" | awk -F ':' '{print $1}')
    port=$(echo "${host_port}" | awk -F ':' '{print $2}')
    second_port=$(echo "${host_port}" | awk -F ':' '{print $3}')
    third_port=$(echo "${host_port}" | awk -F ':' '{print $4}')
    dir=$(echo "$line" | awk -F ' ' '{print $2}')

    if [[ -z "$port" ]]; then
      if [[ "$type" = "tablet" ]]; then
        port="$OPENMLDB_TABLET_PORT"
      elif [[ "$type" = "nameserver" ]]; then
        port="$OPENMLDB_NAMESERVER_PORT"
      elif [[ "$type" = "apiserver" ]]; then
        port="$OPENMLDB_APISERVER_PORT"
      elif [[ "$type" = "taskmanager" ]]; then
          port="$OPENMLDB_TASKMANAGER_PORT"
      elif [[ "$type" = "zookeeper" ]]; then
          port="$OPENMLDB_ZK_CLUSTER_CLIENT_PORT"
      fi
    fi

    if [[ "$type" = "zookeeper" ]]; then
      if [[ -z "$second_port" ]]; then
        second_port="$OPENMLDB_ZK_CLUSTER_PEER_PORT"
      fi
      if [[ -z "$third_port" ]]; then
        third_port="$OPENMLDB_ZK_CLUSTER_ELECTION_PORT"
      fi
    fi

    if [[ -z "$dir" ]]; then
      dir="$OPENMLDB_HOME"
    fi

    echo "$host $port $dir $second_port $third_port"
    i=$((i+1))
  done

  return 0
}

if [ -z "${OPENMLDB_HOME}" ]; then
  OPENMLDB_HOME="$(cd "$(dirname "$0")"/.. || exit; pwd)"
  export OPENMLDB_HOME
fi

if [ -z "${SPARK_HOME}" ]; then
  SPARK_HOME=${OPENMLDB_HOME}/spark
  export SPARK_HOME
fi

if [ -z "${OPENMLDB_ZK_HOME}" ]; then
  OPENMLDB_ZK_HOME=${OPENMLDB_HOME}/zookeeper
  export OPENMLDB_ZK_HOME
fi

if [[ "$OPENMLDB_MODE" = "cluster" && "$OPENMLDB_USE_EXISTING_ZK_CLUSTER" != "true" ]]; then
  if [ -z "${OPENMLDB_ZK_CLUSTER}" ]; then
    old_IFS="$IFS"
    IFS=$'\n'
    zk_cluster=
    for line in $(parse_host conf/hosts zookeeper)
    do
      host=$(echo "$line" | awk -F ' ' '{print $1}')
      port=$(echo "$line" | awk -F ' ' '{print $2}')
      dir=$(echo "$line" | awk -F ' ' '{print $3}')
      if [[ -z "$zk_cluster" ]]; then
        zk_cluster="$host:$port"
      else
        zk_cluster="$zk_cluster,$host:$port"
      fi
    done
    IFS="$old_IFS"
    OPENMLDB_ZK_CLUSTER="$zk_cluster"
    export OPENMLDB_ZK_CLUSTER
  fi
fi