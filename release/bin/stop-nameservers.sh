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

set -e

home="$(cd "$(dirname "$0")"/.. || exit; pwd)"
. "$home"/conf/openmldb-env.sh
. "$home"/bin/init.sh

if [[ ${OPENMLDB_MODE} == "standalone" ]]; then
  bin/start.sh stop standalone_nameserver
else
  grep -v '^ *#' < conf/nameservers | while IFS= read -r line
  do
    host_port=$(echo "$line" | awk -F ' ' '{print $1}')
    host=$(echo "${host_port}" | awk -F ':' '{print $1}')
    port=$(echo "${host_port}" | awk -F ':' '{print $2}')
    dir=$(echo "$line" | awk -F ' ' '{print $2}')

    if [[ -z $dir ]]; then
      dir=${OPENMLDB_HOME}
    fi
    if [[ -z $port ]]; then
      port=${OPENMLDB_NAMESERVER_PORT}
    fi
    echo "stop nameserver in $dir with endpoint $host:$port "
    ssh -n "$host" "cd $dir; bin/start.sh stop nameserver"
    sleep 2
  done
fi