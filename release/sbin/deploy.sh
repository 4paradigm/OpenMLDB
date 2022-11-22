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
  key=$1
  value=$2
  file=$3
  sed -i "s/^$key/# $key/g" "$file"
  echo "$key=$value" >> "$file"
}

config_zk() {
  file=$1
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
  config_file=$1
  printf "# This file is generated automatically from %s.template\n\n" "${config_file}" > "${config_file}"
  cat "${config_file}".template >> "${config_file}"

  echo "" >> "$config_file"
  echo "# below configs are generated automatically" >> "$config_file"

  # configure zookeeper
  config_zk "${config_file}"
}

component=$1
case $component in
  tablet)
    # configure tablet
    tablet_conf=conf/tablet.flags
    common_config ${tablet_conf}
    if [[ -n ${OPENMLDB_TABLET_PORT} ]]; then
      exchange "--endpoint" "${OPENMLDB_HOST}:${OPENMLDB_TABLET_PORT}" ${tablet_conf}
    fi
    ;;
  nameserver)
    # configure nameserver
    ns_conf=conf/nameserver.flags
    common_config ${ns_conf}
    if [[ -n ${OPENMLDB_NAMESERVER_PORT} ]]; then
      exchange "--endpoint" "${OPENMLDB_HOST}:${OPENMLDB_NAMESERVER_PORT}" ${ns_conf}
    fi
    ;;
  apiserver)
    # configure apiserver
    api_conf=conf/apiserver.flags
    common_config ${api_conf}
    if [[ -n ${OPENMLDB_APISERVER_PORT} ]]; then
      exchange "--endpoint" "${OPENMLDB_HOST}:${OPENMLDB_APISERVER_PORT}" ${api_conf}
    fi
    ;;
  taskmanager)
    # configure taskmanager
    taskmanager_conf=conf/taskmanager.properties
    common_config  ${taskmanager_conf}
    if [[ -n ${OPENMLDB_TASKMANAGER_PORT} ]]; then
      exchange "server.port" "${OPENMLDB_TASKMANAGER_PORT}" ${taskmanager_conf}
    fi
    ;;
  *)
    echo "Only support {tablet|nameserver|apiserver|taskmanager}" >&2
esac