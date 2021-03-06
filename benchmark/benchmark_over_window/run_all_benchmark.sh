#!/bin/bash

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

set -x -e

export SPARK_HOME=`pwd`/../distribution/spark-3.0.0-bin-llvm-spark/
export FLINK_HOME=`pwd`/../distribution/flink-1.11-SNAPSHOT/

# Test Spark++ 3.0
time ./run_spark.sh

# Test Spark 3.0
export DISABLE_FESQL=true
export DISABLE_FESQL_FALLBACK=true
time ./run_spark.sh
unset DISABLE_FESQL
unset DISABLE_FESQL_FALLBACK

# Test Spark++ 2.3
export SPARK_HOME=`pwd`/../distribution/spark-2.3.0-bin-llvm-spark/
time ./run_spark.sh

# Test Spark 2.3
export DISABLE_FESQL=true
export DISABLE_FESQL_FALLBACK=true
time ./run_spark.sh
unset DISABLE_FESQL
unset DISABLE_FESQL_FALLBACK

# Test FEQL
export SPARK_HOME=`pwd`/../distribution/spark-2.3.0-bin-hadoop2.7_scala211/
time ./run_feql.sh

# Test Flink++ batch
./flink-1.11-SNAPSHOT/bin/start-cluster.sh
#time ./run_flink_batch.sh
./flink-1.11-SNAPSHOT/bin/stop-cluster.sh

# Test Flink++ streaming
#time ./run_flink_streaming.sh

# Text Flink streaming
export DISABLE_FESQL=true
export DISABLE_FESQL_FALLBACK=true
#time ./run_flink_streaming.sh
unset DISABLE_FESQL
unset DISABLE_FESQL_FALLBACK
