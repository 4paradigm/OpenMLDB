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

set -eE -x
CURRENT_DIR=$(dirname "$0")
pushd "${CURRENT_DIR}"
cp -r ../../../openmldb ./
sed -i"" -e "s/OPENMLDB_MODE:=standalone/OPENMLDB_MODE:=cluster/g" openmldb/conf/openmldb-env.sh
sed -i"" -e "s/.*make_snapshot_threshold_offset.*/--make_snapshot_threshold_offset=1/g" openmldb/conf/tablet.flags.template
rm -rf /tmp/openmldb
python3 genhost.py
bash openmldb/sbin/deploy-all.sh
bash openmldb/sbin/start-all.sh
popd
