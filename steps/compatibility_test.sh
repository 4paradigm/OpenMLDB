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

#! /bin/sh
#
# steps/compatibility.sh
#
SERVER_VERSION='release/1.4.3'
CLIENT_VERSION='v1.4.2'

#SERVER_VERSION='v1.4.2'
#CLIENT_VERSION='release/1.4.3'

ROOT_DIR=`pwd`
ulimit -c unlimited
cp steps/zoo.cfg thirdsrc/zookeeper-3.4.10/conf
cd thirdsrc/zookeeper-3.4.10
test -d ut_zookeeper && rm -rf ut_zookeeper
netstat -anp | grep 6181 | awk '{print $NF}' | awk -F '/' '{print $1}'| xargs kill -9
./bin/zkServer.sh start && cd $ROOT_DIR

sleep 5

cd $ROOT_DIR
git checkout $SERVER_VERSION 
sh steps/compile.sh

cd onebox && sh start_onebox.sh 
sleep 3

cd $ROOT_DIR
git checkout .
git checkout $CLIENT_VERSION
PROTO_BIN=$ROOT_DIR/thirdparty/bin/protoc
sed -i "/protocExecutable/c\<protocExecutable>${PROTO_BIN}<\/protocExecutable>" java/pom.xml
mkdir -p java/src/main/proto/
cp -rf src/proto/tablet.proto java/src/main/proto/
cp -rf src/proto/name_server.proto java/src/main/proto/
cp -rf src/proto/common.proto java/src/main/proto/

cd $ROOT_DIR/java
mvn clean test 
code=$?
cd $ROOT_DIR
cd onebox && sh stop_all.sh
cd $ROOT_DIR
cd thirdsrc/zookeeper-3.4.10 && ./bin/zkServer.sh stop
exit $code
