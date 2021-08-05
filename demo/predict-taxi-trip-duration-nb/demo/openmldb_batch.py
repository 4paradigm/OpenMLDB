#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd


def run_batch_sql(sql):
    spark = SparkSession.builder.appName("OpenMLDB Demo").getOrCreate()
    parquet_predict = "./data/taxi_tour_table_predict_simple.snappy.parquet"
    parquet_train = "./data/taxi_tour_table_train_simple.snappy.parquet"
    train = spark.read.parquet(parquet_train)
    train.createOrReplaceTempView("t1")
    train_df = spark.sql(sql)
    train_set = train_df.toPandas()
    predict = spark.read.parquet(parquet_predict)
    predict.createOrReplaceTempView("t1")
    predict_df = spark.sql(sql)
    predict_set = predict_df.toPandas()
    return train_set, predict_set
