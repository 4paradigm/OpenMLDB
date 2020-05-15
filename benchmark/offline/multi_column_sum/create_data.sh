#!/usr/bin/env bash

YARN_JARS="hdfs:///spark_benchmark/dependency/spark230_jars/*.jar"

OUTPUT_PATH="hdfs:///baoxinqi/test_multi_column_sum/data"

ARGS=""
ARGS+=" --rows 10000000"
ARGS+=" --cols 100"
ARGS+=" --ids 1000"
ARGS+=" --master yarn"
ARGS+=" --output ${OUTPUT_PATH}"

${SPARK_HOME}/bin/spark-submit \
    --executor-memory 4g \
    --driver-memory 4g \
    --executor-cores 1 \
    --num-executors 16 \
    --master yarn --deploy-mode cluster \
    --conf spark.yarn.jars=${YARN_JARS} \
    --conf spark.hadoop.yarn.timeline-service.enabled=false \
    --conf spark.yarn.maxAppAttempts=1 \
    create_data.py ${ARGS}

