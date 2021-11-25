#  Taxi Tour Duration Prediction

We demonstrate how to use [OpenMLDB](https://github.com/4paradigm/OpenMLDB) together with other opensource software to develop an application for predicting the New York City Taxi Trip Duration (reading more about this application on [Kaggle](https://www.kaggle.com/c/nyc-taxi-trip-duration/overview)).

## 1. Feature Extraction SQL Script

The below script shows the feature extraction SQL used for this application.

```sql
sql_tpl = ""select trip_duration, passenger_count,
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
```

## 2. Demo with The Cluster Mode

```bash
# Pull the docker and start it
docker run -it 4pdosc/openmldb:0.3.2 bash

# Initilize the environment
./init.sh

# Run feature extraction and model training. Feature extraction will read offline data from the local file
python3 train.py ./fe.sql /tmp/model.txt

# Import the data to online database
python3 import.py

# Start the HTTP service for inference with OpenMLDB
./start_predict_server.sh ./fe.sql /tmp/model.txt

# Run inference with a HTTP request
python3 predict.py
# The following output is expected (the numbers might be slightly different)
----------------ins---------------
[[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
  40.774097 40.774097  1.        1.      ]]
---------------predict trip_duration -------------
848.014745715936 s
```
:bulb: To read more details about the cluster mode, please refer to the [QuickStart (Cluster Mode)](https://github.com/4paradigm/OpenMLDB/blob/main/docs/en/cluster.md)

## 3. Demo with The Standalone Mode
**Start docker**

```bash
docker run -it 4pdosc/openmldb:0.3.2 bash
```
**Initialize environment**

```bash
./init.sh standalone
```
**Create table and import the data to OpenMLDB.**

```bash
# Start the OpenMLDB CLI for the standalone mode
../openmldb/bin/openmldb --host 127.0.0.1 --port 6527
```
```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE t1(id string, vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, pickup_longitude double, pickup_latitude double, dropoff_longitude double,dropoff_latitude double, store_and_fwd_flag string,trip_duration int, index(ts=pickup_datetime));
> LOAD DATA INFILE './data/taxi_tour.csv' INTO TABLE t1;
```
**Run offline feature extraction**

```sql
> SET PERFORMANCE_SENSITIVE = false;
> select trip_duration, passenger_count,
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
from t1
window w as (partition by vendor_id order by pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
w2 as (partition by passenger_count order by pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
> quit
```
**Train model**

```bash
python3 train_s.py /tmp/feature.csv /tmp/model.txt
```
**Online SQL deployment**

```bash
# Start the OpenMLDB CLI for the standalone mode
../openmldb/bin/openmldb --host 127.0.0.1 --port 6527
```
```sql
> USE demo_db;
> DEPLOY demo select trip_duration, passenger_count,
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
from t1
window w as (partition by vendor_id order by pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
w2 as (partition by passenger_count order by pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW);
> quit
```
:bulb: Note that:

- The SQL used for the online deployment should be the same as that for offline feature extraction.
- For a real-world application, the user may import another copy of recent data for online inference before SQL deployment (refer to the cluster-mode demo). For the sake of simplicity, this demo just uses the same data for both offline training and online inference. If different data sets are used, then the online table name (`t1` in this example) should be changed accordingly.

**Start HTTP service for inference with OpenMLDB**

```
./start_predict_server.sh /tmp/model.txt
```

**Run inference with HTTP request**

```
python3 predict.py
# The following output is expected (the numbers might be slightly different)
----------------ins---------------
[[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
  40.774097 40.774097  1.        1.      ]]
---------------predict trip_duration -------------
880.3688347542294 s
```

:bulb: To read more details about the standalone mode, please refer to the [QuickStart (Standalone Mode)](https://github.com/4paradigm/OpenMLDB/blob/main/docs/en/standalone.md)

