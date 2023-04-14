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

rm_dir() {
    local host=$1
    local dir=$2
    if [[ $dir == "" ]] || [[ $dir == "/" ]]; then
        echo "invalid dir $dir"
        exit 1
    fi
    local cmd="rm -rf $dir"
    run_auto "$host" "$cmd"
}

if [[ ${OPENMLDB_MODE} == "standalone" ]]; then
  rm -rf standalone_db standalone_logs
else
  conf_file="conf/tablet.flags.template"
  dirname=""
  while IFS= read -r line
  do
    if echo "$line" | grep -q '^#'; then
      continue
    fi
    if echo "$line" | grep -v "zk_root_path" | grep -q "root_path" ||
        echo "$line" |  grep -q "openmldb_log_dir"; then
      cur_dirname=$(echo "${line}" | awk -F '=' '{print $2}')
      dirname="${dirname} ${cur_dirname}"
    fi
  done < "$conf_file"
  old_IFS="$IFS"
  IFS=$'\n'
  # delete tablet data and log
  for line in $(parse_host conf/hosts tablet)
  do
    host=$(echo "$line" | awk -F ' ' '{print $1}')
    port=$(echo "$line" | awk -F ' ' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $3}')
    if [[ ${CLEAR_OPENMLDB_INSTALL_DIR} == "true" ]]; then
      echo "clear $dir with endpoint $host:$port "
      rm_dir "$host" "$dir"
    else
      echo "clear tablet data and log in $dir with endpoint $host:$port "
      cmd="cd $dir && rm -rf ${dirname}"
      run_auto "$host" "$cmd"
    fi
  done

  # delete apiserver log
  for line in $(parse_host conf/hosts apiserver)
  do
    host=$(echo "$line" | awk -F ' ' '{print $1}')
    port=$(echo "$line" | awk -F ' ' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $3}')
    if [[ ${CLEAR_OPENMLDB_INSTALL_DIR} == "true" ]]; then
      echo "clear $dir with endpoint $host:$port "
      rm_dir "$host" "$dir"
    else
      echo "clear apiserver log in $dir with endpoint $host:$port "
      cmd="cd $dir && rm -rf logs"
      run_auto "$host" "$cmd"
    fi
  done

  # delete nameserver log
  for line in $(parse_host conf/hosts nameserver)
  do
    host=$(echo "$line" | awk -F ' ' '{print $1}')
    port=$(echo "$line" | awk -F ' ' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $3}')
    if [[ ${CLEAR_OPENMLDB_INSTALL_DIR} == "true" ]]; then
      echo "clear $dir with endpoint $host:$port "
      rm_dir "$host" "$dir"
    else
      echo "clear nameserver log in $dir with endpoint $host:$port "
      cmd="cd $dir && rm -rf logs"
      run_auto "$host" "$cmd"
    fi
  done

  # delete taskmanager data and log
  for line in $(parse_host conf/hosts taskmanager)
  do
    host=$(echo "$line" | awk -F ' ' '{print $1}')
    port=$(echo "$line" | awk -F ' ' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $3}')

    if [[ ${CLEAR_OPENMLDB_INSTALL_DIR} == "true" ]]; then
      echo "clear $dir with endpoint $host:$port "
      rm_dir "$host" "$dir"
    else
      echo "clear taskmanager log in $dir with endpoint $host:$port "
      cmd="cd $dir && rm -rf logs taskmanager/bin/logs"
      run_auto "$host" "$cmd"
    fi
    # TODO(zhanghao): support to delete file:// or hdfs:// style path
    cmd="rm -rf /tmp/openmldb_offline_storage/"
    echo "clear taskmanager data in $dir with endpoint $host:$port "
    run_auto "$host" "$cmd"
  done

  # delete zk data
  if [[ "${OPENMLDB_USE_EXISTING_ZK_CLUSTER}" != "true" ]]; then
    for line in $(parse_host conf/hosts zookeeper)
    do
      host=$(echo "$line" | awk -F ' ' '{print $1}')
      port=$(echo "$line" | awk -F ' ' '{print $2}')
      dir=$(echo "$line" | awk -F ' ' '{print $3}')
      if [[ ${CLEAR_OPENMLDB_INSTALL_DIR} == "true" ]]; then
        echo "clear $dir with endpoint $host:$port "
        rm_dir "$host" "$dir"
      else

        echo "clear zookeeper data and log in $dir with endpoint $host:$port"
        cmd="cd $dir && rm zookeeper.out > /dev/null 2>&1"
        run_auto "$host" "$cmd"
        cmd="cd $dir/data; find -type d -not -path '.' -exec rm -rf {} +"
        run_auto "$host" "$cmd"
      fi
    done
  fi
  IFS="$old_IFS"
fi
