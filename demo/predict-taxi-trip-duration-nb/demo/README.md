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
docker run -dt ghcr.io/4paradigm/openmldb:0.3.0
# find the container id
CONTAINER_ID=`docker ps | grep openmldb | awk '{print $1}'`
docker exec -it ${CONTAINER_ID} /bin/bash

# Initilize the environment
sh init.sh

# Import the data to OpenMLDB
python3 import.py

# Run feature extraction and model training
python3 train.py ./fe.sql /tmp/model.txt

# Start HTTP serevice for inference with OpenMLDB
sh start_predict_server.sh ./fe.sql /tmp/model.txt

# Run inference with HTTP request
python3 predict.py
# the output we will see
----------------ins---------------
[[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
  40.774097 40.774097  1.        1.      ]]
---------------predict trip_duration -------------
848.014745715936 s
```

## Running the demo with standalone mode
Start docker
```bash
docker run -it ghcr.io/4paradigm/openmldb:0.3.0 bash
```
Initilize environment
```bash
sh init.sh standalone
```
Create table and import the data to OpenMLDB
```bash
../openmldb/bin/openmldb --host 127.0.0.1 --port 6527
```
```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE t1(id string, vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, pickup_longitude double, pickup_latitude double, dropoff_longitude double,dropoff_latitude double, store_and_fwd_flag string,trip_duration int, index(ts=pickup_datetime));
> LOAD DATA INFILE './data/taxi_tour.csv' INTO TABLE t1;
```
Run feature extraction
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
Train model
```bash
python3 train_s.py /tmp/feature.csv /tmp/model.txt
```
Deploy SQL
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

Start HTTP serevice for inference with OpenMLDB
```
sh start_predict_server.sh /tmp/model.txt
```

Run inference with HTTP request
```
python3 predict.py
# the output we will see
----------------ins---------------
[[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
  40.774097 40.774097  1.        1.      ]]
---------------predict trip_duration -------------
880.3688347542294 s
```