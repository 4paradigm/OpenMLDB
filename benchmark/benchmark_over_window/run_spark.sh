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

set -x -x

$SPARK_HOME/bin/spark-submit \
     --master local[4] --deploy-mode client \
     --num-executors 1 \
     --executor-cores 4 \
     --driver-memory 4g \
     --executor-memory 4g \
     ./taxi_hour_pyspark_single_window.py
