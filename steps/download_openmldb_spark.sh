#!/bin/bash

set -xe

SPARK_HOME_PATH=$1

mkdir -p "$SPARK_HOME_PATH"
# Download OpenMLDB Spark distribution
curl -L -o spark-3.0.0-bin-openmldbspark.tgz https://github.com/4paradigm/spark/releases/download/v3.0.0-openmldb0.4.0/spark-3.0.0-bin-openmldbspark.tgz

tar xzf spark-3.0.0-bin-openmldbspark.tgz -C "$SPARK_HOME_PATH" --strip-components=1

rm ./spark-3.0.0-bin-openmldbspark.tgz
