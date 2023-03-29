# OpenMLDB + LightGBM: Taxi Trip Duration Prediction

In this document, we will take [the taxi travel time prediction problem on Kaggle as an example](https://www.kaggle.com/c/nyc-taxi-trip-duration/overview) to demonstrate how to use the OpenMLDB and LightGBM together to build a complete machine learning application. 

Note that: (1) this case is based on the OpenMLDB cluster version for tutorial demonstration; (2) this document uses the pre-compiled docker image. If you want to test it in the OpenMLDB environment compiled and built by yourself, you need to configure and use our [Spark Distribution for Feature Engineering Optimization](https://github.com/4paradigm/spark). Please refer to relevant documents of [compilation](https://openmldb.ai/docs/en/main/deploy/compile.html) (Refer to Chapter: "Spark Distribution Optimized for OpenMLDB") and the [installation and deployment documents](https://openmldb.ai/docs/en/main/deploy/install_deploy.html) (Refer to the section: [Deploy TaskManager](https://openmldb.ai/docs/en/main/deploy/install_deploy.html#deploy-taskmanager)).

### 1. Preparation and Preliminary Knowledge

#### 1.1. Pull and Start the OpenMLDB Docker Image

- Note: Please make sure that the Docker Engine version number is > = 18.03

- Pull the OpenMLDB docker image and run the corresponding container:

```bash
docker run -it 4pdosc/openmldb:0.7.3 bash
```

The image is preinstalled with OpenMLDB and preset with all scripts, third-party libraries, open-source tools and training data required for this case.

```{note}
Note that all the commands below run in the docker container by default, and are assumed to be in the default directory (`/work/taxi-trip`).
```

#### 1.2. Initialize Environment

```bash
./init.sh
cd taxi-trip
```

We provide the init.sh script in the image that helps users to quickly initialize the environment including:

- Configure zookeeper

- Start cluster version OpenMLDB

#### 1.3. Start OpenMLDB CLI Client

```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

```{note}
Note that most of the commands in this tutorial are executed under the OpenMLDB CLI. In order to distinguish from the ordinary shell environment, the commands executed under the OpenMLDB CLI use a special prompt of >.
```

#### 1.4. Preliminary Knowledge: Non-Blocking Task of Cluster Version

Some commands in the cluster version are non-blocking tasks, including `LOAD DATA` in online mode and `LOAD DATA`, `SELECT`, `SELECT INTO` commands in the offline mode. After submitting a task, you can use relevant commands such as `SHOW JOBS` and `SHOW JOB` to view the task progress. For details, see the offline task management document.

### 2. Machine Learning Based on OpenMLDB and LightGBM

#### 2.1. Creating Databases and Data Tables

The following commands are executed in the OpenMLDB CLI environment.

```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE t1(id string, vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, pickup_longitude double, pickup_latitude double, dropoff_longitude double, dropoff_latitude double, store_and_fwd_flag string, trip_duration int);
```

#### 2.2. Offline Data Preparation

First, you need to switch to offline execution mode. Next, import the sample data `/work/taxi-trip/data/taxi_tour_table_train_simple.csv` as offline data that is used for offline feature calculation. 

The following commands are executed under the OpenMLDB CLI.

```sql
> USE demo_db;
> SET @@execute_mode='offline';
> LOAD DATA INFILE '/work/taxi-trip/data/taxi_tour_table_train_simple.snappy.parquet' INTO TABLE t1 options(format='parquet', header=true, mode='append');
```

```{note}
Note that `LOAD DATA` is a non-blocking task. You can use the command `SHOW JOBS` to view the running status of the task. Please wait for the task to run successfully (`state` to `FINISHED` status) before proceeding to the next step.
```

#### 2.3. The Feature Extraction Script

Usually, users need to analyze the data according to the goal of machine learning before designing the features, and then design and investigate the features according to the analysis. Data analysis and feature research of the machine learning are not the scope of this paper, and we will not expand it. We assumes that users already have the basic theoretical knowledge of machine learning, the ability to solve machine learning problems, the ability to understand SQL syntax, and the ability to use SQL syntax to construct features.

For this case, the user has designed several features after the analysis and research:

| Feature Name    | Feature Meaning                                              | SQL Feature Representation              |
| --------------- | ------------------------------------------------------------ | --------------------------------------- |
| trip_duration   | Travel time of a single trip                                 | `trip_duration`                         |
| passenger_count | Number of passengers                                         | `passenger_count`                       |
| vendor_sum_pl   | Cumulative number of taxis of the same brand in the time window in the past 1 day (pickup_latitude) | `sum(pickup_latitude) OVER w`           |
| vendor_max_pl   | The largest number of taxis of the same brand in the time window in the past 1 day (pickup_latitude) | `max(pickup_latitude) OVER w`           |
| vendor_min_pl   | The minimum number of taxis of the same brand in the time window in the past 1 day (pickup_latitude) | `min(pickup_latitude) OVER w`           |
| vendor_avg_pl   | Average number of taxis of the same brand in the time window in the past 1 day (pickup_latitude) | `avg(pickup_latitude) OVER w`           |
| pc_sum_pl       | Cumulative trips of the same passenger capacity in the time window in the past 1 day (pickup_latitude) | `sum(pickup_latitude) OVER w2`          |
| pc_max_pl       | The maximum number of trips with the same passenger capacity in the time window in the past 1 day (pickup_latitude) | `max(pickup_latitude) OVER w2`          |
| pc_min_pl       | The minimum number of trips with the same passenger capacity in the time window in the past 1 day (pickup_latitude) | `min(pickup_latitude) OVER w2`          |
| pc_avg_pl       | Average number of trips with the same passenger capacity in the time window in the past 1 day (pickup_latitude) | `avg(pickup_latitude) OVER w2`          |
| pc_cnt          | The total number of trips with the same passenger capacity in the time window in the past 1 day | `count(vendor_id) OVER w2`              |
| vendor_cnt      | Total trips of taxis of the same brand in the time window in the past 1 day | `count(vendor_id) OVER w AS vendor_cnt` |

#### 2.4. Offline Feature Extraction

In the offline mode, the user extracts features and outputs the feature results to `/tmp/feature_data` that is saved in the data directory for subsequent model training. The `SELECT` command corresponds to the SQL feature extraction script generated based on the above table. The following commands are executed under the OpenMLDB CLI.

```sql
> USE demo_db;
> SET @@execute_mode='offline';
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
w2 AS (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature_data';
```

Note that the cluster version `SELECT INTO` is a non-blocking task. You can use the command `SHOW JOBS` to view the running status of the task. Please wait for the task to run successfully (`state` to `FINISHED` status) before proceeding to the next step.

#### 2.5. Model Training

1. Model training will not be carry out in the OpenMLDB thus, exit the OpenMLDB CLI through the following `quit` command.

```bash
> quit
```

2. Then in the command line, you execute train.py. It uses the open-source training tool `lightgbm` to train the model based on the offline features generated in the previous step, and the training results are stored in `/tmp/model.txt`.

```bash
python3 train.py /tmp/feature_data /tmp/model.txt
```

#### 2.6. Online SQL Deployment

Assuming that the model produced by the features designed in Section 2.3 in the previous model training meets the expectation. The next step is to deploy the feature extraction SQL script online to provide real-time feature extraction.

1. Restart OpenMLDB CLI for SQL online deployment

```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

2. To execute online deployment, the following commands are executed in OpenMLDB CLI.

```sql
> USE demo_db;
> SET @@execute_mode='online';
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
```

#### 2.7. Online Data Import

We need to import the data for real-time feature extraction. First, you need to switch to **online** execution mode. Then, in the online mode, import the sample data `/work/taxi-trip/data/taxi_tour_table_train_simple.csv` as the online data source. The following commands are executed under the OpenMLDB CLI.

```sql
> USE demo_db;
> SET @@execute_mode='online';
> LOAD DATA INFILE 'file:///work/taxi-trip/data/taxi_tour_table_train_simple.csv' INTO TABLE t1 options(format='csv', header=true, mode='append');
```

Note that the cluster version `SELECT INTO` is a non-blocking task. You can use the command `SHOW JOBS` to view the running status of the task. Please wait for the task to run successfully (`state` to `FINISHED` status) before proceeding to the next step.

#### 2.8. Start Online Prediction Service

1. If you have not exited the OpenMLDB CLI, use the `quit` command to exit the OpenMLDB CLI.
2. Start the prediction service from the command line:

```
./start_predict_server.sh 127.0.0.1:9080 /tmp/model.txt
```

#### 2.9. Send Real-Time Request

The `predict.py` script will send a line of request data to the prediction service. A returned results will be received and finally, prints them out.

```bash
# Run inference with a HTTP request
python3 predict.py
# The following output is expected (the numbers might be slightly different)
----------------ins---------------
[[ 2.       40.774097 40.774097 40.774097 40.774097 40.774097 40.774097
  40.774097 40.774097  1.        1.      ]]
---------------predict trip_duration -------------
848.014745715936 s
```

