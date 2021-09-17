#!/usr/bin/env bash

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

sh fedb-ns-1/bin/start_ns.sh stop
sh fedb-ns-2/bin/start_ns.sh stop
sh fedb-tablet-1/bin/start.sh stop
sh fedb-tablet-2/bin/start.sh stop
sh fedb-tablet-3/bin/start.sh stop
sh zookeeper-3.4.14/bin/zkServer.sh stop

sh fedb-ns-1/bin/start.sh stop nameserver
sh fedb-ns-2/bin/start.sh stop nameserver
sh fedb-tablet-1/bin/start.sh stop tablet
sh fedb-tablet-2/bin/start.sh stop tablet
sh fedb-tablet-3/bin/start.sh stop tablet
sh fedb-apiserver-1/bin/start.sh stop apiserver
sh zookeeper-3.4.14/bin/zkServer.sh stop

sh openmldb-ns-1/bin/start.sh stop nameserver
sh openmldb-ns-2/bin/start.sh stop nameserver
sh openmldb-tablet-1/bin/start.sh stop tablet
sh openmldb-tablet-2/bin/start.sh stop tablet
sh openmldb-tablet-3/bin/start.sh stop tablet
sh openmldb-apiserver-1/bin/start.sh stop apiserver
sh zookeeper-3.4.14/bin/zkServer.sh stop

rm fedb-ns-1/bin/fedb
rm fedb-ns-2/bin/fedb
rm fedb-tablet-1/bin/fedb
rm fedb-tablet-2/bin/fedb
rm fedb-tablet-3/bin/fedb
rm fedb-apiserver-1/bin/fedb

rm fedb-ns-1/bin/openmldb
rm fedb-ns-2/bin/openmldb
rm fedb-tablet-1/bin/openmldb
rm fedb-tablet-2/bin/openmldb
rm fedb-tablet-3/bin/openmldb
rm fedb-apiserver-1/bin/openmldb

cp -r fedb fedb-ns-1/bin/
cp -r fedb fedb-ns-2/bin/
cp -r fedb fedb-tablet-1/bin/
cp -r fedb fedb-tablet-2/bin/
cp -r fedb fedb-tablet-3/bin/
cp -r fedb fedb-apiserver-1/bin/

cp -r openmldb fedb-ns-1/bin/
cp -r openmldb fedb-ns-2/bin/
cp -r openmldb fedb-tablet-1/bin/
cp -r openmldb fedb-tablet-2/bin/
cp -r openmldb fedb-tablet-3/bin/
cp -r openmldb fedb-apiserver-1/bin/

sh fedb-ns-1/bin/start.sh start nameserver
sh fedb-ns-2/bin/start.sh start nameserver
sh fedb-tablet-1/bin/start.sh start tablet
sh fedb-tablet-2/bin/start.sh start tablet
sh fedb-tablet-3/bin/start.sh start tablet
sh fedb-apiserver-1/bin/start.sh start apiserver

sh fedb-ns-1/bin/start.sh restart nameserver
sh fedb-ns-2/bin/start.sh restart nameserver
sh fedb-tablet-1/bin/start.sh restart tablet
sh fedb-tablet-2/bin/start.sh restart tablet
sh fedb-tablet-3/bin/start.sh restart tablet
sh fedb-apiserver-1/bin/start.sh restart apiserver

sh fedb-ns-1/bin/start_ns.sh restart
sh fedb-ns-2/bin/start_ns.sh restart
sh fedb-tablet-1/bin/start.sh restart
sh fedb-tablet-2/bin/start.sh restart
sh fedb-tablet-3/bin/start.sh restart
sh fedb-apiserver-1/bin/start.sh restart


sh fedb-ns-1/bin/start_ns.sh stop
sh fedb-ns-2/bin/start_ns.sh stop
sh fedb-tablet-1/bin/start.sh stop
sh fedb-tablet-2/bin/start.sh stop
sh fedb-tablet-3/bin/start.sh stop
sh rtidb-blob-1/bin/start.sh stop
sh rtidb-blob-proxy-1/bin/start.sh stop
sh zookeeper-3.4.10/bin/zkServer.sh stop
