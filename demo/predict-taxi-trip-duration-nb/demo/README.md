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

## Running the Tour

```
docker run -dt ghcr.io/4paradigm/openmldb:0.2.0
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
sh start_predict_server.sh ./fe.sql 8887 /tmp/model.txt

# Run inference with HTTP request
python3 predict.py
# the output we will see
----------------ins---------------
[[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
  40.774097 40.774097  1.        1.      ]]
---------------predict trip_duration -------------
859.3298781277192 s
```
