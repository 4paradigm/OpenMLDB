#!/usr/bin/env bash

# benchmark/offline/multi_column_sum/create_data.sh
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

