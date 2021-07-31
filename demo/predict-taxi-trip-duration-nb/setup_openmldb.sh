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

set -eE

curl -SLo zookeeper.tar.gz https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
if [[ $OSTYPE = 'linux-gnu' && $(arch) = 'aarch64' ]]; then
    curl -SLo openmldb.tar.gz https://github.com/aceforeverd/fedb/releases/download/v0.2.1-beta1/openmldb-0.2.0-linux-aarch64.tar.gz
    curl -SLo _sql_router_sdk.so https://github.com/aceforeverd/fedb/releases/download/v0.2.1-beta1/_sql_router_sdk_aarch64.so
else
    echo "WIP"
fi

WORKDIR=/work

mkdir -p "$WORKDIR"

tar xzf zookeeper.tar.gz -C "$WORKDIR"
pushd $WORKDIR/zookeeper-3.4.14/
mv conf/zoo_sample.cfg conf/zoo.cfg
popd

mkdir -p $WORKDIR/openmldb
tar xzf openmldb.tar.gz -C "$WORKDIR/openmldb" --strip-components 1

pushd $WORKDIR/openmldb/batch
tar xzf spark-3.0.0-bin-openmldb.tar.gz
rm -f spark-3.0.0-bin-openmldb.tar.gz
pushd spark-3.0.0-bin-openmldb/python/
python3 setup.py install
popd
popd

if [[ $OSTYPE = 'linux-gnu' && $(arch) = 'aarch64' ]]; then
    mv _sql_router_sdk.so /usr/local/lib/python3.8/site-packages/sqlalchemy_openmldb/openmldbapi/_sql_router_sdk.so
fi

rm -f ./*.tar.gz
