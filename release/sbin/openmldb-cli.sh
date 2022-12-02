#!/bin/bash

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

if [[ -n "$OPENMLDB_MODE" && "$OPENMLDB_MODE" = "cluster" ]]; then
  bin/openmldb --zk_cluster="${OPENMLDB_ZK_CLUSTER}" --zk_root_path="${OPENMLDB_ZK_ROOT_PATH}" --role=sql_client "$@"
else
  bin/openmldb --host 127.0.0.1 --port 6527 "$@"
fi
