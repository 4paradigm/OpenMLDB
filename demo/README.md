#  Taxi Tour Duration Prediction

We demonstrate how to use [OpenMLDB](https://github.com/4paradigm/OpenMLDB) together with other opensource software to develop a complete machine learning application for predicting the New York City Taxi Trip Duration (read more about this application on [Kaggle](https://www.kaggle.com/c/nyc-taxi-trip-duration/overview)).

## 1. Feature Extraction SQL Script

The below script shows the feature extraction SQL used for this application.

```sql
sql_tpl = ""SELECT trip_duration, passenger_count,
sum(pickup_latitude) OVER w AS vendor_sum_pl,
max(pickup_latitude) OVER w AS vendor_max_pl,
min(pickup_latitude) OVER w AS vendor_min_pl,
avg(pickup_latitude) OVER w AS vendor_avg_pl,
sum(pickup_latitude) OVER w2 AS pc_sum_pl,
max(pickup_latitude) OVER w2 AS pc_max_pl,
min(pickup_latitude) OVER w2 AS pc_min_pl,
avg(pickup_latitude) OVER w2 AS pc_avg_pl ,
count(vendor_id) OVER w2 AS pc_cnt,
count(vendor_id) OVER w AS vendor_cnt
FROM {}
WINDOW w AS (PARTITION BY vendor_id ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW),
w2 as (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW)"""
```

## 2. Demo with The Cluster Mode

> :warning: Required docker engine version >= 18.03

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

> :warning: Required docker engine version >= 18.03

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
# The below commands are executed in the CLI
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE t1(id string, vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, pickup_longitude double, pickup_latitude double, dropoff_longitude double, dropoff_latitude double, store_and_fwd_flag string, trip_duration int, INDEX(ts=pickup_datetime));
> LOAD DATA INFILE './data/taxi_tour.csv' INTO TABLE t1;
```
**Run offline feature extraction**

```sql
# The below commands are executed in the CLI
> SET PERFORMANCE_SENSITIVE = false;
> SELECT trip_duration, passenger_count,
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
w2 AS (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
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
# The below commands are executed in the CLI
> USE demo_db;
> DEPLOY demo SELECT trip_duration, passenger_count,
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

