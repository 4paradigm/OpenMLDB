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

home="$(cd "$(dirname "$0")"/.. || exit 1; pwd)"

function parse_host {
  local host_file=$1
  local type=$2

  local start=false
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

    local host_port
    host_port=$(echo "$line" | awk -F ' ' '{print $1}')
    local host
    host=$(echo "${host_port}" | awk -F ':' '{print $1}')
    local port
    port=$(echo "${host_port}" | awk -F ':' '{print $2}')
    local second_port
    second_port=$(echo "${host_port}" | awk -F ':' '{print $3}')
    local third_port
    third_port=$(echo "${host_port}" | awk -F ':' '{print $4}')
    local dir
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
      if [[ "$type" = "zookeeper" ]]; then
        dir="$OPENMLDB_ZK_HOME"
      else
        dir="$OPENMLDB_HOME"
      fi
    fi

    echo "$host $port $dir $second_port $third_port"
    i=$((i+1))
  done

  return 0
}

run_auto() {
  local host=$1
  local cmd=$2
  if [[ "$OPENMLDB_FORCE_LOCAL" = true || "$host" = "localhost" || "$host" = "127.0.0.1" ]]; then
    local cur_dir
    cur_dir=$(pwd)
    bash -c "$cmd"
    cd "$cur_dir" || return
  else
    ssh -n "$host" "$cmd"
  fi
}

if [ -z "${OPENMLDB_HOME}" ]; then
  OPENMLDB_HOME="$(cd "$(dirname "$0")"/.. || exit 1; pwd)"
  export OPENMLDB_HOME
fi

if [ -n "$RUNNER_EXISTING_SPARK_HOME" ]; then
  echo "use existing spark $RUNNER_EXISTING_SPARK_HOME on runner, overwrite SPARK_HOME"
  SPARK_HOME="$RUNNER_EXISTING_SPARK_HOME"
  export SPARK_HOME
elif [ -z "${SPARK_HOME}" ]; then
  SPARK_HOME=${OPENMLDB_HOME}/spark
  export SPARK_HOME
fi

if [ -z "${OPENMLDB_ZK_HOME}" ]; then
  OPENMLDB_ZK_HOME=${OPENMLDB_HOME}/zookeeper
  export OPENMLDB_ZK_HOME
fi

if [[ -n "$OPENMLDB_MODE" && "$OPENMLDB_MODE" = "cluster" && "$OPENMLDB_USE_EXISTING_ZK_CLUSTER" != "true" ]]; then
  if [ -z "${OPENMLDB_ZK_CLUSTER}" ]; then
    old_IFS="$IFS"
    IFS=$'\n'
    zk_cluster=
    for line in $(parse_host "$home"/conf/hosts zookeeper)
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