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

home="$(cd "$(dirname "$0")"/.. || exit 1; pwd)"
sbin="$(cd "$(dirname "$0")" || exit 1; pwd)"
. "$home"/conf/openmldb-env.sh
. "$sbin"/init.sh
cd "$home" || exit 1

old_IFS="$IFS"
IFS=$'\n'
for line in $(parse_host conf/hosts zookeeper)
do
  host=$(echo "$line" | awk -F ' ' '{print $1}')
  port=$(echo "$line" | awk -F ' ' '{print $2}')
  dir=$(echo "$line" | awk -F ' ' '{print $3}')

  echo "start zookeeper in $dir with endpoint $host:$port "
  # ref deploy.sh zk_conf
  cmd="cd $dir && bin/zkServer.sh start conf/zoo.cfg"
  # special for java
  pre=""
  if [[ -n $RUNNER_JAVA_HOME ]]; then
    echo "overwrite java env by RUNNER_JAVA_HOME:$RUNNER_JAVA_HOME"
    pre="export JAVA_HOME=$RUNNER_JAVA_HOME && export PATH=$JAVA_HOME/bin:$PATH &&"
  fi
  run_auto "$host" "$pre $cmd"
done
IFS="$old_IFS"
