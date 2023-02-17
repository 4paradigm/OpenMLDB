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

exchange() {
  local key=$1
  local value=$2
  local file=$3
  sed -i "s/^$key/# $key/g" "$file"
  echo "$key=$value" >> "$file"
}

config_zk() {
  local file=$1
  if [[ -n ${OPENMLDB_ZK_CLUSTER} ]]; then
    if grep -q "zookeeper.cluster" < "$file"; then
      exchange "zookeeper.cluster" "${OPENMLDB_ZK_CLUSTER}" "${file}"
    else
      exchange "--zk_cluster" "${OPENMLDB_ZK_CLUSTER}" "${file}"
    fi
  fi

  if [[ -n ${OPENMLDB_ZK_ROOT_PATH} ]]; then
    if grep -q "zookeeper.root_path" < "$file"; then
      exchange "zookeeper.root_path" "${OPENMLDB_ZK_ROOT_PATH}" "${file}"
    else
      exchange "--zk_root_path" "${OPENMLDB_ZK_ROOT_PATH}" "${file}"
    fi
  fi
}

common_config() {
  local config_file=$1
  local host=$2
  local port=$3
  local tmp_config="${config_file}".template
  if [[ $# -ge 4 ]]; then
    tmp_config=$4
  fi
  local with_zk=true
  if [[ $# -ge 5 ]]; then
    with_zk=$5
  fi

  printf "# This file is generated automatically from %s\n\n" "${tmp_config}" > "${config_file}"
  cat "${tmp_config}" >> "${config_file}"

  echo "" >> "$config_file"
  echo "# below configs are generated automatically" >> "$config_file"

  # configure zookeeper
  if [[ -n ${with_zk} && ${with_zk} != "false" ]]; then
    config_zk "${config_file}"
  fi

  # configure host:port
  if [[ -n ${port} ]]; then
    # tablet/nameserver
    if grep -q "\-\-endpoint=" < "${config_file}"; then
      exchange "--endpoint" "${host}:${port}" "${config_file}"
    elif grep -q "server.host" < "${config_file}"; then  # taskmanager
      exchange "server.host" "${host}" "${config_file}"
      exchange "server.port" "${port}" "${config_file}"
      exchange "spark.home" "${SPARK_HOME}"
    elif grep -q "clientPort" < "${config_file}"; then  # zookeeper
      exchange "clientPort" "${port}" "${zk_conf}"
    fi
  fi
}

component=$1
case $component in
  tablet)
    # configure tablet
    tablet_conf=conf/tablet.flags
    common_config ${tablet_conf} "${OPENMLDB_HOST}" "${OPENMLDB_TABLET_PORT}"
    ;;
  nameserver)
    # configure nameserver
    ns_conf=conf/nameserver.flags
    common_config ${ns_conf} "${OPENMLDB_HOST}" "${OPENMLDB_NAMESERVER_PORT}"
    ;;
  apiserver)
    # configure apiserver
    api_conf=conf/apiserver.flags
    common_config ${api_conf} "${OPENMLDB_HOST}" "${OPENMLDB_APISERVER_PORT}"
    ;;
  taskmanager)
    # configure taskmanager
    taskmanager_conf=conf/taskmanager.properties
    common_config ${taskmanager_conf} "${OPENMLDB_HOST}" "${OPENMLDB_TASKMANAGER_PORT}"
    ;;
  zookeeper)
    # configure zookeeper
    zk_conf=conf/zoo.cfg
    zk_tmp_conf=conf/zoo_sample.cfg
    common_config "$zk_conf" "${OPENMLDB_HOST}" "${OPENMLDB_ZK_CLUSTER_CLIENT_PORT}" "$zk_tmp_conf" "false"
    data_dir="${OPENMLDB_ZK_HOME}/data"
    exchange "dataDir" "$data_dir" ${zk_conf}
    echo "initLimit=5" >> ${zk_conf}
    echo "syncLimit=2" >> ${zk_conf}
    echo "$OPENMLDB_ZK_QUORUM" | tr '|' '\n' >> ${zk_conf}
    if [[ ! -e "$data_dir" ]]; then
      mkdir -p "$data_dir"
    fi
    echo "$OPENMLDB_ZK_MYID" > "$data_dir"/myid
    ;;
  *)
    echo "Only support {tablet|nameserver|apiserver|taskmanager|zookeeper}" >&2
esac