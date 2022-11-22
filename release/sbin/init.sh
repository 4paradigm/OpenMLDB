#! /usr/bin/env bash

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

if [ -z "${OPENMLDB_HOME}" ]; then
  OPENMLDB_HOME="$(cd "$(dirname "$0")"/.. || exit; pwd)"
  export OPENMLDB_HOME
fi

if [ -z "${SPARK_HOME}" ]; then
  SPARK_HOME=${OPENMLDB_HOME}/spark
  export SPARK_HOME
fi

if [ -z "${ZK_HOME}" ]; then
  ZK_HOME=${OPENMLDB_HOME}/zookeeper
  export ZK_HOME
fi

if [ -z "${OPENMLDB_ZK_CLUSTER}" ]; then
  OPENMLDB_ZK_CLUSTER="$(hostname):2181"
  export OPENMLDB_ZK_CLUSTER
fi