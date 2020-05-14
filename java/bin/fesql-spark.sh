#!/bin/bash

CUR_DIR=$(cd $(dirname $0); pwd)
DEFAULT_FESQL_JAR_PATH=$(find ${CUR_DIR}/../java | grep fesql-spark-.*-with-dependencies.jar)
FESQL_SPARK_JAR=${FESQL_SPARK_JAR:-${DEFAULT_FESQL_JAR_PATH}}
echo "Find fesql-spark jar package: ${FESQL_SPARK_JAR}"

java -cp ${FESQL_SPARK_JAR} Main $@



