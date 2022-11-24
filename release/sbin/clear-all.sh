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

if [[ ${OPENMLDB_MODE} == "standalone" ]]; then
  rm -rf standalone_db standalone_logs
else
  old_IFS="$IFS"
  IFS=$'\n'
  # delete tablet data and log
  for line in $(parse_host conf/hosts tablet)
  do
    host=$(echo "$line" | awk -F ' ' '{print $1}')
    port=$(echo "$line" | awk -F ' ' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $3}')

    echo "clear tablet data and log in $dir with endpoint $host:$port "
    cmd="cd $dir; rm -rf recycle db logs"
    run_auto "$host" "$cmd"
  done

  # delete apiserver log
  for line in $(parse_host conf/hosts apiserver)
  do
    host=$(echo "$line" | awk -F ' ' '{print $1}')
    port=$(echo "$line" | awk -F ' ' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $3}')

    echo "clear apiserver log in $dir with endpoint $host:$port "
    cmd="cd $dir; rm -rf logs"
    run_auto "$host" "$cmd"
  done

  # delete nameserver log
  for line in $(parse_host conf/hosts nameserver)
  do
    host=$(echo "$line" | awk -F ' ' '{print $1}')
    port=$(echo "$line" | awk -F ' ' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $3}')

    echo "clear nameserver log in $dir with endpoint $host:$port "
    cmd="cd $dir; rm -rf logs"
    run_auto "$host" "$cmd"
  done

  # delete taskmanager data and log
  echo "clear taskmanager data and log in /tmp/openmldb_offline_storage/ and $home/logs"
  rm -rf "$home"/logs
  rm -rf /tmp/openmldb_offline_storage/

  # delete zk data
  if [[ "${OPENMLDB_USE_EXISTING_ZK_CLUSTER}" != "true" ]]; then
    for line in $(parse_host conf/hosts zookeeper)
    do
      host=$(echo "$line" | awk -F ' ' '{print $1}')
      port=$(echo "$line" | awk -F ' ' '{print $2}')
      dir=$(echo "$line" | awk -F ' ' '{print $3}')

      echo "clear zookeeper data and log in $dir with endpoint $host:$port"
      cmd="cd $dir; rm zookeeper.out > /dev/null 2>&1"
      run_auto "$host" "$cmd"
      cmd="cd $dir/data; find -type d -not -path '.' -exec rm -rf {} +"
      run_auto "$host" "$cmd"
    done
  fi
  IFS="$old_IFS"
fi