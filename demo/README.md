#  Predict Taxi Tour Duration

This demo uses [OpenMLDB](https://github.com/4paradigm/OpenMLDB) to develop a realtime prediction appliction for the New York City Taxi Trip Duration on [Kaggle](https://www.kaggle.com/c/nyc-taxi-trip-duration/overview).

## Feature Engineering SQL Script

```
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

## Running the demo with cluster mode

```
docker run -it 4pdosc/openmldb:0.3.2 bash

# Initilize the environment
./init.sh

# Run feature extraction and model training. Feature extraction will read offline data from local file which may be stored in HDFS in real scene.
python3 train.py ./fe.sql /tmp/model.txt

# Import to online database
python3 import.py

# Start HTTP serevice for inference with OpenMLDB
./start_predict_server.sh ./fe.sql /tmp/model.txt

# Run inference with HTTP request
python3 predict.py
# the output we will see
----------------ins---------------
[[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
  40.774097 40.774097  1.        1.      ]]
---------------predict trip_duration -------------
848.014745715936 s
```
To read more details about cluster mode, please refer [here](https://github.com/4paradigm/OpenMLDB/blob/main/docs/en/cluster.md)

## Running the demo with standalone mode
### Start docker
```bash
docker run -it 4pdosc/openmldb:0.3.2 bash
```
### Initilize environment
```bash
./init.sh standalone
```
### Create table and import the data to OpenMLDB.
```bash
../openmldb/bin/openmldb --host 127.0.0.1 --port 6527
```
```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE t1(id string, vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, pickup_longitude double, pickup_latitude double, dropoff_longitude double,dropoff_latitude double, store_and_fwd_flag string,trip_duration int, index(ts=pickup_datetime));
> LOAD DATA INFILE './data/taxi_tour.csv' INTO TABLE t1;
```
### Run feature extraction
```
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
### Train model
```bash
python3 train_s.py /tmp/feature.csv /tmp/model.txt
```
### Deploy SQL
```bash
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
Note that for a real-world application, the user may import another copy of recent data for online inference before SQL deployment (refer to the cluster-mode demo). For the sake of simplicity, this demo just uses the same data for both offline training and online inference.

### Start HTTP serevice for inference with OpenMLDB
```
./start_predict_server.sh /tmp/model.txt
```

### Run inference with HTTP request
```
python3 predict.py
# the output we will see
----------------ins---------------
[[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
  40.774097 40.774097  1.        1.      ]]
---------------predict trip_duration -------------
880.3688347542294 s
```

To read more details about standalone mode, please refer [here](https://github.com/4paradigm/OpenMLDB/blob/main/docs/en/standalone.md)
