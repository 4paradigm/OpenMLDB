#!/bin/bash

set -xe

SPARK_HOME_PATH=$1

# Download OpenMLDB Spark distribution
curl -L -o spark-3.0.0-bin-openmldbspark.tgz https://github.com/4paradigm/spark/releases/download/v3.0.0-openmldb0.4.0/spark-3.0.0-bin-openmldbspark.tgz
tar xzf ./spark-3.0.0-bin-openmldbspark.tgz

# Move Spark files to $SPARK_HOME
mv ./spark-3.0.0-bin-openmldbspark/ "$SPARK_HOME_PATH"

rm ./spark-3.0.0-bin-openmldbspark.tgz
