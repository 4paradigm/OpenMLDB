#!/bin/bash

set -x -e

$SPARK_HOME/bin/spark-submit \
     --master local[4] --deploy-mode client \
     --num-executors 1 \
     --executor-cores 4 \
     --driver-memory 4g \
     --executor-memory 4g \
     --files ./taxi_hour_window_feql_single_window.json \
     --class com._4paradigm.ferrari.prophet.ParallelizeFerrari \
     ./ferrari-prophet-1.7.7.8-rc3.jar ./taxi_hour_window_feql_single_window.json
