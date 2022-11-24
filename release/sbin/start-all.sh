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

echo "OPENMLDB envs:"
printenv | grep OPENMLDB_
printenv | grep SPARK_HOME
echo ""

# if in cluster mode, start zk
if [[ -n "${OPENMLDB_MODE}" && ${OPENMLDB_MODE} = "cluster" ]]; then
  # start zk if OPENMLDB_USE_EXISTING_ZK_CLUSTER is not true
  if [[ "${OPENMLDB_USE_EXISTING_ZK_CLUSTER}" != "true" ]]; then
    cd "$home" || exit
    "$sbin"/start-zks.sh
    sleep 5
  fi
fi

# Start Tablets
"$sbin"/start-tablets.sh

# Start Nameservers
"$sbin"/start-nameservers.sh

cd "$home" || exit
if [[ -n "${OPENMLDB_MODE}" && ${OPENMLDB_MODE} = "cluster" ]]; then
  echo "Start recovering data..."
  mkdir -p logs > /dev/null 2>&1
  cmd="python3 tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=${OPENMLDB_ZK_CLUSTER} --zk_root_path=${OPENMLDB_ZK_ROOT_PATH} --cmd=recoverdata"
  if $cmd > logs/recover.log 2>&1; then
    echo "Recovering data done"
  else
    echo "Recovering data failed. Pls check log in logs/recover.log"
  fi
fi

# Start Apiservers
"$sbin"/start-apiservers.sh

# if in cluster mode, start taskmanager
if [[ -n "${OPENMLDB_MODE}" && ${OPENMLDB_MODE} = "cluster" ]]; then
  # start taskmanager
  cd "$home" || exit
  "$sbin"/start-taskmanagers.sh
fi

echo "OpenMLDB start success"
