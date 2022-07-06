#!/bin/bash
# File   : setup_openmldb.sh

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
    VERSION=0.3.0
fi
echo "version: ${VERSION}"

curl -SLo zookeeper.tar.gz https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
curl -SLo openmldb.tar.gz "https://github.com/4paradigm/OpenMLDB/releases/download/v${VERSION}/openmldb-${VERSION}-linux.tar.gz"
curl -SLo spark-3.0.0-bin-openmldbspark.tgz "https://github.com/4paradigm/spark/releases/download/v3.0.0-openmldb${VERSION}/spark-3.0.0-bin-openmldbspark.tgz"

WORKDIR=/work

mkdir -p "$WORKDIR"

tar xzf zookeeper.tar.gz -C "$WORKDIR"
pushd $WORKDIR/zookeeper-3.4.14/
mv conf/zoo_sample.cfg conf/zoo.cfg
popd

mkdir -p "${WORKDIR}/openmldb"
tar xzf openmldb.tar.gz -C "${WORKDIR}/openmldb" --strip-components 1
# remove symbols and sections
strip -s "${WORKDIR}/openmldb/bin/openmldb"

mkdir -p "${WORKDIR}/openmldb/spark-3.0.0-bin-openmldbspark"
tar xzf spark-3.0.0-bin-openmldbspark.tgz -C "${WORKDIR}/openmldb/spark-3.0.0-bin-openmldbspark" --strip-components 1
rm -rf "${WORKDIR}/openmldb/spark-3.0.0-bin-openmldbspark/python"

rm -f ./*.tar.gz
rm -f ./*.tgz
