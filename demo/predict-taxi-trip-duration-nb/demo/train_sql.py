#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV

spark = SparkSession.builder.appName("Dataframe demo").getOrCreate()

parquet_predict = "./data/taxi_tour_table_predict_simple.snappy.parquet"
parquet_train = "./data/taxi_tour_table_train_simple.snappy.parquet"
predict = spark.read.parquet(parquet_predict)
predict.createOrReplaceTempView("t2")
train = spark.read.parquet(parquet_train)
train.createOrReplaceTempView("t1")
sql_tpl = """select trip_duration, passenger_count,
sum(pickup_latitude) over w as vendor_sum_pl,
max(pickup_latitude) over w as vendor_max_pl,
min(pickup_latitude) over w as vendor_min_pl,
avg(pickup_latitude) over w as vendor_avg_pl,
sum(pickup_latitude) over w2 as pc_sum_pl,
max(pickup_latitude) over w2 as pc_max_pl,
min(pickup_latitude) over w2 as pc_min_pl,
avg(pickup_latitude) over w2 as pc_avg_pl ,
count(vendor_id) over w2 as pc_cnt,
count(vendor_id) over w as vendor_cnt
from {}
window w as (partition by vendor_id order by pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
w2 as (partition by passenger_count order by pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW)"""
train_sql = sql_tpl.format('t1')
predict_sql = sql_tpl.format('t2')
train_df = spark.sql(train_sql)
predict_df = spark.sql(predict_sql)
train_set = train_df.toPandas()
predict_set = predict_df.toPandas()
y_train = train_set['trip_duration']
x_train = train_set.drop(columns=['trip_duration'])
y_predict = predict_set['trip_duration']
x_predict = predict_set.drop(columns=['trip_duration'])

lgb_train = lgb.Dataset(x_train, y_train)
lgb_eval = lgb.Dataset(x_predict, y_predict, reference=lgb_train)

# specify your configurations as a dict
params = {
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': {'l2', 'l1'},
    'num_leaves': 31,
    'learning_rate': 0.05,
    'feature_fraction': 0.9,
    'bagging_fraction': 0.8,
    'bagging_freq': 5,
    'verbose': 0
}

print('Starting training...')
gbm = lgb.train(params,
                lgb_train,
                num_boost_round=20,
                valid_sets=lgb_eval,
                early_stopping_rounds=5)
gbm.save_model('/tmp/model.txt')

print("save model.txt done")
