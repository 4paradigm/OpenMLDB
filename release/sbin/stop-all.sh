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

# Stop Apiservers
"$sbin"/stop-apiservers.sh

if [[ -n "${OPENMLDB_MODE}" && ${OPENMLDB_MODE} = "cluster" ]]; then
  # stop taskmanager
  "$sbin"/stop-taskmanagers.sh
fi

# Stop Nameservers
"$sbin"/stop-nameservers.sh

# Stop Tablets
"$sbin"/stop-tablets.sh

if [[ -n "${OPENMLDB_MODE}" && ${OPENMLDB_MODE} = "cluster" ]]; then
  # stop zk if OPENMLDB_USE_EXISTING_ZK_CLUSTER is not true
  if [[ "${OPENMLDB_USE_EXISTING_ZK_CLUSTER}" != "true" ]]; then
    sleep 10
    "$sbin"/stop-zks.sh
  fi
fi

echo "OpenMLDB stopped"
