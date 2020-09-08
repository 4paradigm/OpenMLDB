#!/bin/bash

set -x -x

$SPARK_HOME/bin/spark-submit \
     --master local[4] --deploy-mode client \
     --num-executors 1 \
     --executor-cores 4 \
     --driver-memory 4g \
     --executor-memory 4g \
     ./taxi_hour_pyspark_single_window.py
