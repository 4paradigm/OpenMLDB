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
    VERSION=0.8.2
fi
echo "version: ${VERSION}"

ZK_TAR="zookeeper-3.4.14.tar.gz"
OPENMLDB_TAR="openmldb-${VERSION}-linux.tar.gz"
# TODO(hw): spark release pkg name should add version
SPARK_TAR="spark-3.2.1-bin-openmldbspark.tgz"

if [ $# -gt 1 ] && [ "$2" = "skip_download" ]; then
    echo "skip download packages, the 3 packages should in current dir"
else
    curl -SLO "https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/${ZK_TAR}"
    curl -SLO "https://github.com/4paradigm/OpenMLDB/releases/download/v${VERSION}/${OPENMLDB_TAR}"
    curl -SLO "https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb${VERSION}/${SPARK_TAR}"
fi

WORKDIR=/work

mkdir -p "$WORKDIR"

tar xzf "${ZK_TAR}" -C "$WORKDIR"
pushd $WORKDIR/zookeeper-3.4.14/
cp conf/zoo_sample.cfg conf/zoo.cfg
popd

mkdir -p "${WORKDIR}/openmldb"
tar xzf "${OPENMLDB_TAR}" -C "${WORKDIR}/openmldb" --strip-components 1
# remove symbols and sections
strip -s "${WORKDIR}/openmldb/bin/openmldb"
# do not install sync tools in demo docker
rm "${WORKDIR}/openmldb/bin/data_collector"
rm -rf "${WORKDIR}/openmldb/synctool"

mkdir -p "${WORKDIR}/openmldb/spark-3.2.1-bin-openmldbspark"
tar xzf "${SPARK_TAR}" -C "${WORKDIR}/openmldb/spark-3.2.1-bin-openmldbspark" --strip-components 1

pushd "${WORKDIR}/openmldb"
ln -s "${WORKDIR}/zookeeper-3.4.14" zookeeper
ln -s spark-3.2.1-bin-openmldbspark spark
popd

rm -f ./*.tar.gz
rm -f ./*.tgz
