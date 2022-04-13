# OpenMLDB Quickstart

This tutorial provides a quick start guide for OpenMLDB. Basic steps are: create database, import data, offline feature extraction, deploy SQL, online real-time feature extraction, the basic usage process of the standalone version of OpenMLDB and the cluster version of OpenMLDB is demonstrated.

## 1. Environment and data preparation

> :warning: docker engine version required >= 18.03

This tutorial is developed and deployed based on the OpenMLDB CLI, so first you need to download the sample data and start the OpenMLDB CLI. We recommend using the prepared docker image for a quick experience.

:bulb: To compile and install it yourself, you can refer to our [installation and deployment documentation](https://github.com/4paradigm/openmldb-docs-zh/blob/28159f05d2903a9f96e78d3205bd8e9a2d7cd5c3/deploy/install_deploy.md).

### 1.1 Mirror preparation

Pull the image (image download size is about 1GB, after decompression is about 1.7 GB) and start the docker container

```bash
docker run -it 4pdosc/openmldb:0.4.2 bash
```

:bulb: **After the container is successfully started, the subsequent commands in this tutorial are executed within the container by default. **

### 1.2 Sample data

Download sample data

```bash
curl https://openmldb.ai/demo/data.csv --output ./data/data.csv
curl https://openmldb.ai/demo/data.parquet --output ./data/data.parquet
```

## 2. Quick start with the standalone version of OpenMLDB

### 2.1 Standalone Server and Client

- Start the standalone OpenMLDB server

```bash
# 1. initialize the environment and start standlone openmldb server
./init.sh standalone
```

- Start the standalone OpenMLDB CLI client

```bash
# Start the OpenMLDB CLI for the cluster deployed OpenMLDB
../openmldb/bin/openmldb --host 127.0.0.1 --port 6527
```

The following screenshots show the correct execution of the above docker commands and the correct startup of the OpenMLDB CLI

![image-20220111142406534](https://github.com/4paradigm/openmldb-docs-zh/blob/28159f05d2903a9f96e78d3205bd8e9a2d7cd5c3/quickstart/images/cli.png)

### 2.2 Basic usage process

The workflow of the standalone version of OpenMLDB generally includes several stages: database and table establishment, data preparation, offline feature extraction, SQL solution online, and online real-time feature extraction.

:bulb: Unless otherwise specified, the commands shown below are executed under the standalone version of OpenMLDB CLI by default (CLI commands start with the prompt `>` for distinction).

#### 2.2.1 Create database and table

```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date);
```

#### 2.2.2 Data preparation

Import the previously downloaded sample data (the saved data in [1.2 Sample Data](#1.2-Sample Data)) as training data for offline and online feature extraction.

⚠️ Note: The standalone version can use the same data for offline and online feature extraction. Of course, users can also manually import different data for offline and online. For simplicity, the standalone version of this tutorial uses the same data for offline and online extraction.

```sql
> LOAD DATA INFILE 'data/data.csv' INTO TABLE demo_table1;
```

Preview data

```sql
> SELECT * FROM demo_table1 LIMIT 10;
 ----- ---- ---- ---------- ----------- --------------- - -------------
  c1 c2 c3 c4 c5 c6 c7
 ----- ---- ---- ---------- ----------- --------------- - -------------
  aaa 12 22 2.200000 12.300000 1636097390000 2021-08-19
  aaa 11 22 1.200000 11.300000 1636097290000 2021-07-20
  dd 18 22 8.200000 18.300000 1636097990000 2021-06-20
  aa 13 22 3.200000 13.300000 1636097490000 2021-05-20
  cc 17 22 7.200000 17.300000 1636097890000 2021-05-26
  ff 20 22 9.200000 19.300000 1636098000000 2021-01-10
  bb 16 22 6.200000 16.300000 1636097790000 2021-05-20
  bb 15 22 5.200000 15.300000 1636097690000 2021-03-21
  bb 14 22 4.200000 14.300000 1636097590000 2021-09-23
  ee 19 22 9.200000 19.300000 1636097000000 2021-01-10
 ----- ---- ---- ---------- ----------- --------------- - -------------
```

#### 2.2.3 Offline feature extraction

Execute SQL for feature extraction, and store the generated features in a file for subsequent model training.

```sql
> SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```

#### 2.2.4 SQL solution deploy

Deploy the explored SQL solution online. Notice, the deployed online SQL solution needs to be consistent with the corresponding offline feature extraction SQL solution.

```sql
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```

After going online, you can view the deployed SQL solution through the command `SHOW DEPLOYMENTS`;

```sql
> SHOW DEPLOYMENTS;
 --------- -------------------
  DB Deployment
 --------- -------------------
  demo_db demo_data_service
 --------- -------------------
1 row in set
```

:bulb: Notice, the standalone version of this tutorial uses the same data for offline and online feature extraction. During deployment, offline data is automatically switched to online data for online computing. If you want to use a different dataset, you can do a data update on this basis, or re-import a new dataset before deployment.

#### 2.2.5 Exit the CLI

```sql
> quit;
```

Up to this point, you have completed all the development and deployment work based on OpenMLDB CLI, and have returned to the operating system command line.

#### 2.2.6 Real-time feature extraction

Real-time online services can be provided through the following Web APIs:

```
http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service
        \___________/ \____/ \_____________/
              | | |
        APIServer address Database name Deployment name
```

The input data of the real-time request accepts the `json` format, and we put a line of data into the `input` field of the request. The following example:

```bash
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05- 20"]]}'
```

The following is the expected return result for this query (the computed features are stored in the `data` field):

```json
{"code":0,"msg":"ok","data":{"data":[["aaa",11,22]]}}
```
\* The api server executes the request, which can support batch requests, and the "input" field supports arrays. The request extraction is performed separately for each line of input. For detailed parameter formats, see [RESTful API](../reference/rest_api.md).

\* For the description of the request result, see "3.3.8 Result description of real-time feature extraction" at the end of the article.

## 3. Get started with cluster version of OpenMLDB

### 3.1 Cluster Edition preparation knowledge

The biggest difference between the cluster version and the standalone version is mainly the following two points

- Some commands in the cluster version are non-blocking tasks, including `LOAD DATA` in online mode, and `LOAD DATA`, `SELECT`, `SELECT INTO` commands in offline mode. After submitting a task, you can use related commands such as `SHOW JOBS`, `SHOW JOB` to view the task progress. For details, see the [Offline Task Management](../reference/sql/task_manage/reference.md) document.
- The cluster version needs to maintain offline and online data separately, and cannot use the same set of data as the stand-alone version.

The above differences will be demonstrated based on examples in the following tutorials.

### 3.2 Server and Client

- Start the cluster version of the OpenMLDB server

```bash
# 1. initialize the environment and start cluster openmldb server
./init.sh
```

- Start the clustered OpenMLDB CLI client

```bash
# Start the OpenMLDB CLI for the cluster deployed OpenMLDB
> ../openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

The following screenshot shows the screen after the cluster version of the OpenMLDB CLI is correctly started

![image-20220111141358808](https://github.com/4paradigm/openmldb-docs-zh/blob/28159f05d2903a9f96e78d3205bd8e9a2d7cd5c3/quickstart/images/cli_cluster.png)

### 3.3 Basic usage process

The workflow of the cluster version of OpenMLDB generally includes several stages: database and table creation, offline data preparation, offline feature extraction, SQL solution online, online data preparation, and online real-time feature extraction.

The cluster version of OpenMLDB needs to manage offline data and online data separately. Therefore, after completing the SQL solution online, you must do the online data preparation steps.

:bulb: Unless otherwise specified, the commands shown below are executed under the OpenMLDB CLI by default (the CLI command starts with a prompt `>` for distinction).

#### 3.3.1 Create database and table

```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date);
```

Check out the datasheet:

```sql
> desc demo_table1;
 --- ------- ----------- ------ ---------
  # Field Type Null Default
 --- ------- ----------- ------ ---------
  1 c1 Varchar YES
  2 c2 Int YES
  3 c3 BigInt YES
  4 c4 Float YES
  5 c5 Double YES
  6 c6 Timestamp YES
  7 c7 Date YES
 --- ------- ----------- ------ ---------
 --- -------------------- ------ ---- ------ ------------- ----
  # name keys ts ttl ttl_type
 --- -------------------- ------ ---- ------ ------------- ----
  1 INDEX_0_1641939290 c1 - 0min kAbsoluteTime
 --- -------------------- ------ ---- ------ ------------- ----
```

#### 3.3.2 Offline data preparation

First, please switch to offline execution mode. In this mode, only offline data import/insert and query operations are processed.

Next, import the previously downloaded sample data (which downloaded in [1.2 Sample Data] (#1.2 - Sample Data)) as offline data for offline feature extraction.

```sql
> USE demo_db;
> SET @@execute_mode='offline';
> LOAD DATA INFILE 'file:///work/taxi-trip/data/data.parquet' INTO TABLE demo_table1 options(format='parquet', header=true, mode='append');
```

Notice, the `LOAD DATA` command is non-blocking, and you can view the task progress through offline task management commands such as `SHOW JOBS`.

To preview the data, you can also use the `SELECT` statement, but this command is also a non-blocking command in offline mode. You need to view the log for query results, which will not be expanded here.

#### 3.3.3 Offline feature extraction

Execute SQL for feature extraction, and store the generated features in a file for subsequent model training.

```sql
> USE demo_db;
> SET @@execute_mode='offline';
> SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature_data';
```

Notice, the `SELECT INTO` command in offline mode is non-blocking, and you can view the running progress through offline task management commands such as `SHOW JOBS`.

#### 3.3.4 SQL solution online

Deploy the explored SQL solution online. Notice, the deployed online SQL solution needs to be consistent with the corresponding offline feature extraction SQL solution.

```sql
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```

After going online, you can view the deployed SQL solution through the command `SHOW DEPLOYMENTS`;

```sql
> SHOW DEPLOYMENTS;
 --------- -------------------
  DB Deployment
 --------- -------------------
  demo_db demo_data_service
 --------- -------------------
1 row in set
```

#### 3.3.5 Online data preparation

First, switch to **Online** execution mode. In this mode, only online data import/insert and query operations are processed. Then in the online mode, import the previously downloaded sample data (downloaded in [1.2 Sample Data] (#1.2-Sample Data)) as online data for online feature extraction.

```sql
> USE demo_db;
> SET @@execute_mode='online';
> LOAD DATA INFILE 'file:///work/taxi-trip/data/data.parquet' INTO TABLE demo_table1 options(format='parquet', header=true, mode='append');
```

Notice, the online mode of `LOAD DATA` is also a non-blocking command, and you can view the running progress through offline task management commands such as `SHOW JOBS`.

After waiting for the task to complete, preview the online data:

```sql
> USE demo_db;
> SET @@execute_mode='online';
> SELECT * FROM demo_table1 LIMIT 10;
 ----- ---- ---- ---------- ----------- --------------- - -------------
  c1 c2 c3 c4 c5 c6 c7
 ----- ---- ---- ---------- ----------- --------------- - -------------
  aaa 12 22 2.200000 12.300000 1636097890000 1970-01-01
  aaa 11 22 1.200000 11.300000 1636097290000 1970-01-01
  dd 18 22 8.200000 18.300000 1636111690000 1970-01-01
  aa 13 22 3.200000 13.300000 1636098490000 1970-01-01
  cc 17 22 7.200000 17.300000 1636108090000 1970-01-01
  ff 20 22 9.200000 19.300000 1636270090000 1970-01-01
  bb 16 22 6.200000 16.300000 1636104490000 1970-01-01
  bb 15 22 5.200000 15.300000 1636100890000 1970-01-01
  bb 14 22 4.200000 14.300000 1636099090000 1970-01-01
  ee 19 22 9.200000 19.300000 1636183690000 1970-01-01
 ----- ---- ---- ---------- ----------- --------------- - -------------
```

:bulb: Notice:

- Different from the stand-alone version of OpenMLDB, the cluster version of OpenMLDB needs to maintain offline and online data separately.
- Users need to successfully complete the online deployment of SQL before preparing to go online. Otherwise, the online data may fail.

#### 3.3.6 Exit the CLI

```sql
> quit;
```

Up to this point, you have completed all the development and deployment work based on the cluster version of OpenMLDB CLI, and have returned to the operating system command line.

#### 3.3.7 Real-time feature extraction

Notice: warning:: According to the default deployment configuration, the http port for apiserver deployment is 9080.

Real-time online services can be provided through the following Web APIs:

```
http://127.0.0.1:9080/dbs/demo_db/deployments/demo_data_service
        \___________/ \____/ \_____________/
              | | |
        APIServer address Database name Deployment name
```

The input data of the real-time request accepts the `json` format, and we put a line of data into the `input` field of the request. The following example:

```bash
curl http://127.0.0.1:9080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05- 20"]]}'
```

The following is the expected return result for this query (the computed features are stored in the `data` field):

```json
{"code":0,"msg":"ok","data":{"data":[["aaa",11,22]]}}

```

#### 3.3.8 Result description of real-time feature extraction

Real-time request (deployment execution) is SQL execution in request mode. Unlike batch mode (batch mode), request mode will only perform SQL extractions on request rows. In the previous example, the POST input is used as the request line, assuming this line of data exists in the table demo_table1, and execute SQL on it:
```
SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
The specific extraction logic is as follows (the actual extraction will be optimized to reduce the computation):
1. According to the request line and the `PARTITION BY` in window clause, filter out the lines whose c1 is "aaa", and sort them according to c6 from small to large. So theoretically, the intermediate data table after partition sorting is shown in the following table. Among them, the first row after the request behavior is sorted.
```
 ----- ---- ---- ---------- ----------- --------------- - -------------
  c1 c2 c3 c4 c5 c6 c7
 ----- ---- ---- ---------- ----------- --------------- - -------------
  aaa 11 22 1.2 1.3 1635247427000 2021-05-20
  aaa 12 22 2.200000 12.300000 1636097890000 1970-01-01
  aaa 11 22 1.200000 11.300000 1636097290000 1970-01-01
 ----- ---- ---- ---------- ----------- --------------- - -------------
```
2. The window range is `2 PRECEDING AND CURRENT ROW`, so we cut out the real window in the above table, the request row is the smallest row, the previous 2 rows do not exist, but the window contains the current row, so the window has only one row(the request row).
3. Window aggregation, sum c3 of the data in the window (only one row), and get 22.

The output is:
```
 ----- ---- -------------
  c1 c2 w1_c3_sum
 ----- ---- -------------
  aaa 11 22
 ----- ---- -------------
```
