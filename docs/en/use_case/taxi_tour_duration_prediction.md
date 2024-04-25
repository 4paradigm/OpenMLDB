# Taxi Trip Duration Prediction (OpenMLDB+LightGBM)

This article will use [New York City Taxi Trip Duration](https://www.kaggle.com/c/nyc-taxi-trip-duration/overview) from Kaggle as an example to demonstrate how to use the combination of OpenMLDB and LightGBM to create a complete machine-learning application.

Please note that this document employs a pre-compiled Docker image. If you wish to perform tests in your self-compiled and built OpenMLDB environment, you will need to configure and utilize the [Spark Distribution Documentation for Feature Engineering Optimization](https://github.com/4paradigm/Spark/). Refer to the [Spark Distribution Documentation for OpenMLDB Optimization](../tutorial/openmldbspark_distribution.md#openmldb-spark-distribution) and the [Installation and Deployment Documentation](../deploy/install_deploy.md#modifyingtheconfigurationfileconftaskmanagerproperties) for more detailed information.

## Preparation and Preliminary Knowledge

This article is centered around the development and deployment of OpenMLDB CLI. To begin, you should download the sample data and initiate the OpenMLDB CLI. We recommend using Docker images for a streamlined experience.

- Docker version: >= 18.03

### Pull Image

Execute the following command from the command line to pull the OpenMLDB image and start the Docker container:

```bash
docker run -it 4pdosc/openmldb:0.9.0 bash
```

This image comes pre-installed with OpenMLDB and encompasses all the scripts, third-party libraries, open-source tools, and training data necessary for this case.

```{note}
Keep in mind that the demonstration commands in the OpenMLDB section of this tutorial are executed by default within the Docker container that has been started.
```

### Initialize Environment

```bash
./init.sh
cd taxi-trip
```

The init.sh script provided within the image helps users initialize the environment quickly, including:

- Configuring Zookeeper
- Starting the cluster version of OpenMLDB

### Start OpenMLDB CLI

```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

### Preliminary Knowledge: Asynchronous Tasks

Some OpenMLDB commands are asynchronous, such as the `LOAD DATA`, `SELECT`, and `SELECT INTO` commands in online/offline mode. After submitting a task, you can use relevant commands such as `SHOW JOBS` and `SHOW JOB` to view the progress of the task. For details, please refer to the [Offline Task Management Document](../openmldb_sql/task_manage/index.rst).

## Machine Learning Process

### Step 1: Create Database and Table

Create database `demo_db` and table  `t1`：

```sql
--OpenMLDB CLI
CREATE DATABASE demo_db;
USE demo_db;
CREATE TABLE t1(id string, vendor_id int, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, pickup_longitude double, pickup_latitude double, dropoff_longitude double, dropoff_latitude double, store_and_fwd_flag string, trip_duration int);
```

### Step 2: Import Offline Data

First, switch to offline execution mode. Then, import the sample data `/work/taxi-trip/data/taxi_tour_table_train_simple.snappy.parquet` as offline data for offline feature computation.

```sql
--OpenMLDB CLI
USE demo_db;
SET @@execute_mode='offline';
LOAD DATA INFILE '/work/taxi-trip/data/taxi_tour_table_train_simple.snappy.parquet' INTO TABLE t1 options(format='parquet', header=true, mode='append');
```

```{note}
`LOAD DATA` is an asynchronous task. Please use the `SHOW JOBS` command to check the task's running status. Wait for the task to run successfully (from `state` transitions to `FINISHED` status) before proceeding to the next step.
```

### Step 3: Feature Design

Typically, before designing features, users need to analyze the data based on machine learning goals and then design and explore features based on this analysis. However, this article does not cover data analysis and feature research in machine learning. It assumes that users have a basic understanding of machine learning theory, the ability to solve machine learning problems, proficiency in SQL syntax, and the capacity to construct features using SQL syntax. In this case, it is assumed that the user has designed the following features through analysis and research:

| Feature Name    | Characteristic Meaning                                       | SQL Feature Representation              |
| --------------- | ------------------------------------------------------------ | --------------------------------------- |
| trip_duration   | Travel time for a single trip                                | `trip_duration`                         |
| passenger_count | Number of passengers                                         | `passenger_count`                       |
| vendor_sum_pl   | The cumulative pickup_latitude for taxis of the same brand within the last 1-day time window. | m(pickup_latitude) OVER w`              |
| vendor_max_pl   | The maximum pickup_latitude for taxis of the same brand within the last 1-day time window | `max(pickup_latitude) OVER w`           |
| vendor_min_pl   | The minimum pickup_latitude for taxis of the same brand within the last 1-day time window | `min(pickup_latitude) OVER w`           |
| vendor_avg_pl   | The average pickup_latitude for taxis of the same brand within the last 1-day time window | `avg(pickup_latitude) OVER w`           |
| pc_sum_pl       | The cumulative pickup_latitude for trips with the same passenger count within the last 1-day time window. | `sum(pickup_latitude) OVER w2`          |
| pc_max_pl       | The maximum pickup_latitude for trips with the same passenger count within the last 1-day time window. | `max(pickup_latitude) OVER w2`          |
| pc_min_pl       | The minimum pickup_latitude for trips with the same passenger count within the last 1-day time window. | `min(pickup_latitude) OVER w2`          |
| pc_avg_pl       | The average pickup_latitude for trips with the same passenger count within the last 1-day time window. | `avg(pickup_latitude) OVER w2`          |
| pc_cnt          | The total number of trips with the same passenger capacity within the last 1-day time window | `count(vendor_id) OVER w2`              |
| vendor_cnt      | The total number of trips for taxis of the same brand within the last day's time window | `count(vendor_id) OVER w AS vendor_cnt` |

In the actual process of machine learning feature research, scientists conduct repeated experiments on features to find the best feature set for the model. So the process of "feature design -> offline feature extraction -> model training" will be repeated multiple times, and the features will be continuously adjusted to achieve the desired outcome.

### Step 4: Offline Feature Extraction

Users perform feature extraction in offline mode and save the feature results in the `/tmp/feature_data` directory for future model training. The `SELECT` command corresponds to the SQL feature computation script generated based on the above feature design.

```sql
--OpenMLDB CLI
USE demo_db;
SET @@execute_mode='offline';
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
w2 AS (PARTITION BY passenger_count ORDER BY pickup_datetime ROWS_RANGE BETWEEN 1d PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature_data';
```

```{note}
`SELECT INTO` is an asynchronous task. Please use the SHOW JOBS command to check the task's running status and wait for it to run successfully (transitioning to the FINISHED state) before proceeding to the next step.
```

### Step 5: Model Training

1. Model training is not completed within OpenMLDB. Therefore, exit the OpenMLDB CLI first using the `quit` command.

   ```
   quit;
   ```

2. On the regular command line, run `train.py` (located in the `/work/taxi-trip` directory) and use the open-source training tool `LightGBM` to train the model based on the offline feature table generated in the previous step. The training results are saved in `/tmp/model.txt`.

   ```bash
   python3 train.py /tmp/feature_data /tmp/model.txt
   ```

### Step 6: Launch Feature Extraction SQL Script

Assuming that the features designed in [Step 3: Feature Design](#step-3-feature-design) have produced the expected model in the previous training, the next step is to deploy the feature extraction SQL script online to provide online feature extraction services.

1. Restart the OpenMLDB CLI for SQL online deployment:

```bash
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

2. Execute online deployment:

```sql
--OpenMLDB CLI
USE demo_db;
SET @@execute_mode='online';
DEPLOY demo SELECT trip_duration, passenger_count,
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

```note
DEPLOY contains BIAS OPTIONS here because importing data files into online storage will not be updated. For the current time, it may exceed the TTL (Time-To-Live) of the table index after DEPLOY, leading to the expiration of this data in the table. Time-based expiration relies solely on the 'ts' column and 'ttl' of each index. If the value in that column of the data is < (current time - abs_ttl), it will be expired for that index, irrespective of other factors. The different indexes also do not influence each other. If your data does not generate real-time new timestamps, you also need to consider including BIAS OPTIONS.
```

### Step 7: Import Online Data

Firstly, switch to **online** execution mode. Then, in online mode, import the sample data from `/work/taxi-trip/data/taxi_tour_table_train_simple.csv` as online data for online feature computation.

```sql
--OpenMLDB CLI
USE demo_db;
SET @@execute_mode='online';
LOAD DATA INFILE 'file:///work/taxi-trip/data/taxi_tour_table_train_simple.csv' INTO TABLE t1 options(format='csv', header=true, mode='append');
```

```{note}
`LOAD DATA` is an asynchronous task. Please use the SHOW JOBS command to monitor the task's progress and wait for it to successfully complete (transition to the FINISHED state) before proceeding to the next step.
```

### Step 8: Start Prediction Service

1. If you have not already exited the OpenMLDB CLI, exit the OpenMLDB CLI first.

   ```
   quit;
   ```

2. Start the estimation service on the regular command line:

   ```bash
   ./start_predict_server.sh 127.0.0.1:9080 /tmp/model.txt
   ```

### Step 9: Send Prediction Request

Execute the built-in `predict.py` script from the regular command line. This script sends a request data line to the prediction service, receives the estimation results in return, and prints them out.

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
