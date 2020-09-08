#!/bin/bash

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
