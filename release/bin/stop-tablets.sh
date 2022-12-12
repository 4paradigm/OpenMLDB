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
  bin/start.sh stop standalone_tablet
else
  old_IFS=$IFS
  IFS=$'\n'

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
    echo "stop tablet in $dir with endpoint $host:$port "
    ssh $host "cd $dir; bin/start.sh stop tablet"
    sleep 2
  done

  IFS=$old_IFS
fi