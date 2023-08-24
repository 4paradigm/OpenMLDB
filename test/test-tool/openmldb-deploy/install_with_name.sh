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
VERSION=$1
if [[ -z ${VERSION} ]]; then
    VERSION=0.7.2
fi
echo "version: ${VERSION}"
curl -SLo openmldb.tar.gz "https://github.com/4paradigm/OpenMLDB/releases/download/v${VERSION}/openmldb-${VERSION}-linux.tar.gz"
mkdir -p "openmldb"
tar xzf openmldb.tar.gz -C "openmldb" --strip-components 1
pushd "openmldb"
rm -rf sbin conf
rm -f bin/*.sh
/bin/cp -r ../release/sbin ../release/conf ./
/bin/cp -f ../release/bin/*.sh bin/

/bin/cp -f ../test/test-tool/openmldb-deploy/hosts conf/hosts
sed -i"" -e "s/OPENMLDB_VERSION=[0-9]\.[0-9]\.[0-9]/OPENMLDB_VERSION=${VERSION}/g" conf/openmldb-env.sh
sed -i"" -e "s/OPENMLDB_MODE:=standalone/OPENMLDB_MODE:=cluster/g" conf/openmldb-env.sh
sh sbin/deploy-all.sh

for (( i=0; i<=2; i++ ))
do
    mkdir -p /tmp/openmldb/tablet-${i}/data && echo "tablet-${i}" > /tmp/openmldb/tablet-${i}/data/name.txt
    conf_file="/tmp/openmldb/tablet-${i}/conf/tablet.flags"
    sed -i "s/^--endpoint/# --endpoint/g" ${conf_file} 
    port=$((i + 10921))
    echo "--port=${port}" >> ${conf_file}
    echo "--use_name=true" >> ${conf_file}
done
for (( i=0; i<=1; i++ ))
do
    conf_file="/tmp/openmldb/ns-${i}/conf/nameserver.flags"
    mkdir -p /tmp/openmldb/ns-${i}/data && echo "ns-${i}" > /tmp/openmldb/ns-${i}/data/name.txt
    sed -i "s/^--endpoint/# --endpoint/g" ${conf_file} 
    port=$((i + 7527))
    echo "--port=${port}" >> ${conf_file}
    echo "--use_name=true" >> ${conf_file}
done

sh sbin/start-all.sh
popd

pushd test/test-tool/openmldb-deploy
echo "collect_ignore_glob = [\"test_install.py\", \"test_recoverdata.py\"]" > cases/conftest.py
cat > hosts <<EOF
[tablet]
tablet-0 /tmp/openmldb/tablet-0
tablet-1 /tmp/openmldb/tablet-1
tablet-2 /tmp/openmldb/tablet-2

[nameserver]
ns-0 /tmp/openmldb/ns-0
ns-1 /tmp/openmldb/ns-1

[zookeeper]
localhost:2181:2888:3888 /tmp/openmldb/zookeeper
EOF
popd
