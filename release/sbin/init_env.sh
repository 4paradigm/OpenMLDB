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

user=`whoami`
if [[ ${user} != "root" ]]; then
  echo "please switch to 'root' user to run this script"
  exit
fi

home="$(cd "$(dirname "$0")"/.. || exit 1; pwd)"
sbin="$(cd "$(dirname "$0")" || exit 1; pwd)"
. "$home"/conf/openmldb-env.sh
. "$sbin"/init.sh
cd "$home" || exit 1

old_IFS="$IFS"
IFS=$'\n'
for host in $(parse_host conf/hosts tablet | awk -F ' ' '{print $1}' | xargs -n 1 | sort | uniq)
do
  limit_conf_file="/etc/security/limits.conf"
  run_auto "$host" "echo '*       soft    core    unlimited' >> ${limit_conf_file}"
  run_auto "$host" "echo '*       hard    core    unlimited' >> ${limit_conf_file}"
  run_auto "$host" "echo '*       soft    nofile  655360' >> ${limit_conf_file}"
  run_auto "$host" "echo '*       hard    nofile  655360' >> ${limit_conf_file}"

  # disable swap
  run_auto "$host" "swapoff -a"

  # disable THP (Transparent Huge Pages)
  run_auto "$host" "echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled"
  run_auto "$host" "echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag"
done
IFS="$old_IFS"
