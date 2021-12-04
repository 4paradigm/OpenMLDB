
# OpenMLDB QuickStart (Cluster Mode)

This tutorial is targeted at the cluster mode of OpenMLDB and it will cover the whole lifecycle of how to build a machine learning application with the help of OpenMLDB, including feature extraction, model training, online data import, online feature extraction, model prediction, etc. From this tutorial, readers can understand how to use OpenMLDB to complete the machine learning lifecycle from raw data to model deployment. OpenMLDB provides both Java and Python SDKs. In this tutorial, we will use Python SDK.

In order to better understand the workflow, we use Kaggle Competition [Predict Taxi Tour Duration Dataset](https://github.com/4paradigm/OpenMLDB/tree/main/demo/predict-taxi-trip-duration-nb/script/data) to demonstrate the whole process. Dataset and source code can be found
[here](https://github.com/4paradigm/OpenMLDB/tree/main/demo/predict-taxi-trip-duration-nb/script).

## 1. Offline Workflow
### 1.1. Feature Extraction
In order to do feature extraction, users have to know the data and construct a SQL script. For example, for the Taxi Tour Duration Dataset, we can construct the following [SQL](https://github.com/4paradigm/OpenMLDB/blob/main/demo/predict-taxi-trip-duration-nb/script/fe.sql) for feature extraction:

```sql
SELECT trip_duration, passenger_count,
sum(pickup_latitude) OVER w AS vendor_sum_pl,
max(pickup_latitude) OVER w AS vendor_max_pl,
min(pickup_latitude) OVER w AS vendor_min_pl,
avg(pickup_latitude) OVER w AS vendor_avg_pl,
sum(pickup_latitude) OVER w2 AS pc_sum_pl,
max(pickup_latitude) OVER w2 AS pc_max_pl,
min(pickup_latitude) OVER w2 AS pc_min_pl,
avg(pickup_latitude) OVER w2 AS pc_avg_pl,
count(vendor_id) OVER w2 AS pc_cnt,
count(vendor_id) OVER w AS vendor_cnt
FROM t1
WINDOW w AS (PARTITION BY vendor_id ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
w2 AS (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW);
```

After executing the feature extraction SQL, we can extract the features from the raw data, which will be used for model training. The SQL can executed in Spark to extract features from the raw dataset, which are stored directly in local filesystem or HDFS. Sample Python code is shown as follows（the complete code can be found [here](https://github.com/4paradigm/OpenMLDB/blob/main/demo/predict-taxi-trip-duration-nb/script/train.py)):

```python
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd

spark = SparkSession.builder.appName("OpenMLDB Demo").getOrCreate()
parquet_train = "./data/taxi_tour_table_train_simple.snappy.parquet"
train = spark.read.parquet(parquet_train)
train.createOrReplaceTempView("t1")
train_df = spark.sql(sql)
df = train_df.toPandas()
```

***NOTE***: [OpenMLDB Spark Distribution](https://github.com/4paradigm/OpenMLDB/blob/main/docs/en/compile.md#optimized-spark-distribution-for-openmldb-optional) is used for this example


### 1.2. Model Training
After we get the train and predict datasets from feature extraction, we can use the standard methods to train models. For example, we can use Gradient Boosting Machine (GBM) to train the model and produce a model file（the complete source code can be found [here](https://github.com/4paradigm/OpenMLDB/blob/main/demo/predict-taxi-trip-duration-nb/script/train.py)):

```python
import lightgbm as lgb
from sklearn.model_selection import train_test_split

train_set, predict_set = train_test_split(df, test_size=0.2)
y_train = train_set['trip_duration']
x_train = train_set.drop(columns=['trip_duration'])
y_predict = predict_set['trip_duration']
x_predict = predict_set.drop(columns=['trip_duration'])
lgb_train = lgb.Dataset(x_train, y_train)
lgb_eval = lgb.Dataset(x_predict, y_predict, reference=lgb_train)

gbm = lgb.train(params,
                lgb_train,
                num_boost_round=20,
                valid_sets=lgb_eval,
                early_stopping_rounds=5)
gbm.save_model(model_path)
```

## 2. Online Workflow
Online service requires two inputs:
- the model trained from offline process
- online dataset

The feature extraction SQL is generally based on time windows. Thus in the online model prediction, we usually need recent history data to extract features from time ranges of data.
This recent history data is called online dataset. Online dataset is generally restricted to the recent time range, which is small compared to the offline dataset.

### 2.1. Online Dataset Import
The online dataset can be imported to OpenMLDB in a similar way to the traditional database. The following code shows how to import a csv file to OpenMLDB (the complete code can be found [here](https://github.com/4paradigm/OpenMLDB/blob/main/demo/predict-taxi-trip-duration-nb/script/import.py)）

```python
import sqlalchemy as db


ddl="""
create table t1(
id string,
vendor_id int,
pickup_datetime timestamp,
dropoff_datetime timestamp,
passenger_count int,
pickup_longitude double,
pickup_latitude double,
dropoff_longitude double,
dropoff_latitude double,
store_and_fwd_flag string,
trip_duration int,
index(key=vendor_id, ts=pickup_datetime),
index(key=passenger_count, ts=pickup_datetime)
);
"""

engine = db.create_engine('openmldb:///db_test?zk=127.0.0.1:2181&zkPath=/openmldb')
connection = engine.connect()
# create database
connection.execute("create database db_test;")
# create table
connection.execute(ddl)

# read data from csv file and insert into table
with open('data/taxi_tour_table_train_simple.csv', 'r') as fd:
    for line in fd:
        row = line.split(',')
        insert = "insert into t1 values('%s', %s, %s, %s, %s, %s, %s, %s, %s, '%s', %s);"% tuple(row)
        connection.execute(insert)
```

### 2.2. Online Feature Extraction
Online feature extraction requires both input data and online dataset. The feature extraction SQL should be the same as the offline [SQL](https://github.com/4paradigm/OpenMLDB/blob/main/demo/predict-taxi-trip-duration-nb/script/fe.sql). Sample code is as follows (the related complete code can be found [here](https://github.com/4paradigm/OpenMLDB/blob/main/demo/predict-taxi-trip-duration-nb/script/predict_server.py)):

```python
import sqlalchemy as db

engine = db.create_engine('openmldb:///db_test?zk=127.0.0.1:2181&zkPath=/openmldb')
connection = engine.connect()
features = connection.execute(sql, request_data)
```

### 2.3. Model Prediction
Base on the online feature extracted and the model trained from the offline process, we can get the prediction result. Sample code is as follows:

```python
import lightgbm as lgb

bst = lgb.Booster(model_path)
duration = bst.predict(feature)
```

***NOTE***: In practice, we usually launch a prediction service, which will accept online requests and serve the prediction, and then return the results back to the users. We skip the online deployment step here, but it is included in the [demo tour](https://github.com/4paradigm/OpenMLDB/tree/main/demo).
