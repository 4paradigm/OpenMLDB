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

sh openmldb-ns-1/bin/start.sh stop nameserver
sh openmldb-ns-2/bin/start.sh stop nameserver
sh openmldb-tablet-1/bin/start.sh stop tablet
sh openmldb-tablet-2/bin/start.sh stop tablet
sh openmldb-tablet-3/bin/start.sh stop tablet
sh openmldb-tablet-4/bin/start.sh stop tablet
sh openmldb-apiserver-1/bin/start.sh stop apiserver
sh openmldb-task_manager-1/bin/start.sh stop taskmanager
sh zookeeper-3.4.14/bin/zkServer.sh stop

sh openmldb-ns-1/bin/start.sh start nameserver
sh openmldb-ns-2/bin/start.sh start nameserver
sh openmldb-tablet-1/bin/start.sh start tablet
sh openmldb-tablet-2/bin/start.sh start tablet
sh openmldb-tablet-3/bin/start.sh start tablet
sh openmldb-apiserver-1/bin/start.sh start apiserver
sh openmldb-task_manager-1/bin/start.sh start taskmanager
sh zookeeper-3.4.14/bin/zkServer.sh start

sh openmldb-ns-1/bin/start.sh restart nameserver
sh openmldb-ns-2/bin/start.sh restart nameserver
sh openmldb-tablet-1/bin/start.sh restart tablet
sh openmldb-tablet-2/bin/start.sh restart tablet
sh openmldb-tablet-3/bin/start.sh restart tablet
sh openmldb-apiserver-1/bin/start.sh restart apiserver
sh openmldb-task_manager-1/bin/start.sh restart taskmanager
sh zookeeper-3.4.14/bin/zkServer.sh restart

cp -r openmldb openmldb-ns-1/bin/
cp -r openmldb openmldb-ns-2/bin/
cp -r openmldb openmldb-tablet-1/bin/
cp -r openmldb openmldb-tablet-2/bin/
cp -r openmldb openmldb-tablet-3/bin/
cp -r openmldb openmldb-apiserver-1/bin/
cp -r openmldb openmldb-task_manager-1/bin/

rm -rf openmldb-ns-1/bin/openmldb
rm -rf openmldb-ns-2/bin/openmldb
rm -rf openmldb-tablet-1/bin/openmldb
rm -rf openmldb-tablet-2/bin/openmldb
rm -rf openmldb-tablet-3/bin/openmldb

