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
VERSION="$1"
if [[ -z ${VERSION} ]]; then
    VERSION=0.6.3
fi
echo "version: ${VERSION}"
curl -SLo openmldb.tar.gz "https://github.com/4paradigm/OpenMLDB/releases/download/v${VERSION}/openmldb-${VERSION}-linux.tar.gz"
mkdir -p "openmldb"
tar xzf openmldb.tar.gz -C "openmldb" --strip-components 1
pushd "openmldb"

mv ../hosts conf/hosts
sed -i"" -e "s/OPENMLDB_VERSION=[0-9]\.[0-9]\.[0-9]/OPENMLDB_VERSION=${VERSION}/g" conf/openmldb-env.sh
sed -i"" -e "s/OPENMLDB_MODE:=standalone/OPENMLDB_MODE:=cluster/g" conf/openmldb-env.sh
sh sbin/deploy-all.sh
sh sbin/start-all.sh
