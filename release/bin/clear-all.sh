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

set -e

home="$(cd "`dirname "$0"`"/..; pwd)"
. $home/conf/openmldb-env.sh
. $home/bin/init.sh

if [[ ${OPENMLDB_MODE} == "standalone" ]]; then
  rm -rf standalone_db standalone_logs
else
  old_IFS=$IFS
  IFS=$'\n'

  # delete tablet data and log
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
    echo "clear tablet data and log in $dir with endpoint $host:$port "
    ssh $host "cd $dir; rm -rf recycle db logs"
  done

  # delete taskmanager data and log
  rm -rf $home/logs
  rm -rf /tmp/openmldb_offline_storage/

  # delete zk data
  if [[ "${OPENMLDB_USE_EXISTING_ZK_CLUSTER}" != "true" ]]; then
    rm -rf /tmp/zookeeper
    rm -rf ${ZK_HOME}/zookeeper.out
  fi

  IFS=$old_IFS
fi