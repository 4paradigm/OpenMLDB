# OpenMLDB Quickstart

This tutorial provides a quick start guide to use OpenMLDB. Basic steps are: creating a database, offline data import, offline feature extraction, SQL deployment, online data import, and online real-time feature extraction. The steps of the standalone and cluster versions are slightly different, and are demonstrated separately.

## 1. Environment and Data Preparation
```{warning}
Docker Engine version requirement: >= 18.03
```
This tutorial is demonstrated based on the OpenMLDB CLI, so first you need to download the sample data and start the OpenMLDB CLI. We recommend using the prepared docker image for a quick experience.

```{note}
If you wan to compile and install it by yourself, you can refer to our [installation and deployment documentation](../deploy/install_deploy.md).
```

### 1.1. Download the Docker Image

Pull the image (image download size is about 1GB, after decompression is about 1.7 GB) and start the docker container:

```bash
docker run -it 4pdosc/openmldb:0.6.9 bash
```

```{important}
After the container is successfully started, all the subsequent commands in this tutorial are executed within the container by default. 
```

(download_data)=
### 1.2. Download the Sample Data

Download sample data:

```bash
curl https://openmldb.ai/demo/data.csv --output ./taxi-trip/data/data.csv
curl https://openmldb.ai/demo/data.parquet --output ./taxi-trip/data/data.parquet
```

## 2. The Standalone Version

### 2.1. Start the Server and Client

- Start the standalone OpenMLDB server

```bash
# 1. initialize the environment and start standlone openmldb server
./init.sh standalone
```

- Start the standalone OpenMLDB CLI client

```bash
# Start the OpenMLDB CLI for the cluster deployed OpenMLDB
cd taxi-trip
../openmldb/bin/openmldb --host 127.0.0.1 --port 6527
```

### 2.2. Steps

```{important}
Unless otherwise specified, the commands shown below in this section are executed under the CLI by default (CLI commands start with the prompt `>` for distinction).
```

#### 2.2.1. Create the Database and Table

```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date);
```

#### 2.2.2. Offline Data Import

We should first import the previously downloaded sample data (the saved data in {ref}`download_data`) for offline feature extraction.

```sql
> LOAD DATA INFILE 'data/data.csv' INTO TABLE demo_table1;
```

We can preview the data by using `SELECT`.

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

#### 2.2.3. Offline Feature Extraction

Now we can execute SQL for feature extraction, and store the produced features in a file for subsequent model training.

```sql
> SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```

#### 2.2.4. Online SQL Deployment

When the feature extraction script is ready, we can create an online SQL deployment for it.

```sql
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```

You can also view the SQL deployments through the command `SHOW DEPLOYMENTS`;

```sql
> SHOW DEPLOYMENTS;
 --------- -------------------
  DB Deployment
 --------- -------------------
  demo_db demo_data_service
 --------- -------------------
1 row in set
```

Note that, this tutorial for the standalone version uses the same data for offline and online feature extraction. You can also use two different data sets for offline and online. Later on, for the cluster version, you will see that we must import another data set for online feature extraction.

#### 2.2.5. Exit the CLI

```sql
> quit;
```

Up to this point, you have completed all the development and deployment steps based on the CLI, and have returned to the OS command line.

#### 2.2.6. Real-Time Feature Extraction

Real-time online services can be provided through the following Web APIs:

```
http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service
        \___________/ \____/ \_____________/
              | | |
        APIServer address Database name Deployment name
```

The input data of the real-time request accepts the `json` format, and we put a line of data into the `input` field of the request. Here is the example:

```bash
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05- 20"]]}'
```

The following is the expected return result for this query:

```json
{"code":0,"msg":"ok","data":{"data":[["aaa",11,22]]}}
```
You may refer to [3.3.8. Result Explanation](#3.3.8.-Result Explanation) at the end of the article for the result explanation.

## 3. The Cluster Version

### 3.1. Preliminary Knowledge

The most significant differences between the cluster version and the standalone version are:

- Some commands in the cluster version are non-blocking tasks, including `LOAD DATA` in online mode, and `LOAD DATA`, `SELECT`, `SELECT INTO` commands in offline mode. After submitting a task for such a given command, you can use related commands such as `SHOW JOBS`, `SHOW JOB` to view the task progress. For details, see the [Offline Task Management](../reference/sql/task_manage/reference.md) document.
- The cluster version needs to maintain offline and online data separately, and cannot use the same data set as the stand-alone version.

The above differences will be demonstrated based on examples in the following tutorials.

### 3.2. Start the Server and Client

- Start the cluster version of the OpenMLDB server:

```bash
# 1. initialize the environment and start cluster openmldb server
./init.sh
```

- Start the OpenMLDB CLI:

```bash
# Start the OpenMLDB CLI for the cluster deployed OpenMLDB
cd taxi-trip
../openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client
```

### 3.3. Steps

```{important}
Unless otherwise specified, the commands shown below are executed under the OpenMLDB CLI by default (the CLI command starts with a prompt `>`).
```

#### 3.3.1. Create Database and Table

- Create the database and table:

```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date);
```

- You may view the information of the database and table:

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

#### 3.3.2. Offline Data Import

- First, please switch to the offline execution mode by using the command `SET @@execute_mode='offline'`.
- Next, import the previously downloaded sample data (downloaded in {ref}`download_data`) as offline data for offline feature extraction.

```sql
> USE demo_db;
> SET @@execute_mode='offline';
> LOAD DATA INFILE 'file:///work/taxi-trip/data/data.parquet' INTO TABLE demo_table1 options(format='parquet', header=true, mode='append');
```

Note that, the `LOAD DATA` command is non-blocking, and you can view the task progress through the task management commands such as `SHOW JOBS` and `SHOW JOBLOG`.

```sql
SHOW JOB $JOB_ID

SHOW JOBLOG $JOB_ID
```

#### 3.3.3. Offline Feature Extraction

You can now execute the SQL for feature extraction, and store the produced features in a file for subsequent model training.

```sql
> USE demo_db;
> SET @@execute_mode='offline';
> SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature_data';
```

Note that, the `SELECT INTO` command in offline mode is non-blocking, and you can view the running progress through offline task management commands such as `SHOW JOBS`.

#### 3.3.4. Online SQL Deployment

The SQL can be deployed online using the below command:

```sql
> SET @@execute_mode='online';
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```

After going online, you can view the deployed SQL solutions through the command `SHOW DEPLOYMENTS`;

```sql
> SHOW DEPLOYMENTS;
 --------- -------------------
  DB Deployment
 --------- -------------------
  demo_db demo_data_service
 --------- -------------------
1 row in set
```

#### 3.3.5. Online Data Import

First, you should switch to the online execution mode by using the command `SET @@execute_mode='online'`. Then in the online mode, you should import the previously downloaded sample data (downloaded in {ref}`download_data`) as online data for online feature extraction.

```{note}
As the storage engines for the offline and online data are separate in the cluster version, you must import the data again in the online mode even the same data set is used. For most real-world applications, usually two different data sets are used for offline and online modes.
```

```sql
> USE demo_db;
> SET @@execute_mode='online';
> LOAD DATA INFILE 'file:///work/taxi-trip/data/data.parquet' INTO TABLE demo_table1 options(format='parquet', header=true, mode='append');
```

Note that, the online mode of `LOAD DATA` is a non-blocking command, and you can view the progress through the task management commands such as `SHOW JOBS`.

```{note}
For real-world applications, you will most likely need an additional step to import real-time data. Otherwise OpenMLDB cannot keep fresh data up to date. This step can be done using the SDKs or data stream [connectors](../use_case/pulsar_openmldb_connector_demo.md).
```

#### 3.3.6. Exit the CLI

```sql
> quit;
```

Up to this point, you have completed all the development and deployment steps based on the cluster version of OpenMLDB CLI, and have returned to the OS command line.

#### 3.3.7. Real-Time Feature Extraction

Real-time online services can be provided through the following Web APIs (the default http port for the APIServer is 9080):

```
http://127.0.0.1:9080/dbs/demo_db/deployments/demo_data_service
        \___________/ \____/ \_____________/
              | | |
        APIServer address Database name Deployment name
```

The input data of the real-time request accepts the `json` format, and we put a line of data into the `input` field of the request. Here is the request example:

Example 1:
```bash
curl http://127.0.0.1:9080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]}'
```

The following is the expected return result for this query (the computed features are stored in the `data` field):

```json
{"code":0,"msg":"ok","data":{"data":[["aaa",11,22]]}}
```

Example 2:
```bash
curl http://127.0.0.1:9080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1637000000000, "2021-11-16"]]}'
```
Expect：
```json
{"code":0,"msg":"ok","data":{"data":[["aaa",11,66]]}}
```

#### 3.3.8. Result Explanation

The real-time feature extraction is executed in the request mode. Unlike the batch mode, the request mode will only perform SQL extractions on the request row. In the previous example, the POST input is used as the request row, assuming this row of data exists in the table demo_table1, and execute SQL on it:
```sql
SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
The computation of Example 1 is logically done as follows:
1. According to the request line and the `PARTITION BY` in window clause, filter out the lines whose `c1` is "aaa", and sort them according to `c6` from small to large. So theoretically, the intermediate data table after partition sorting is shown in the following table. Among them, the first row after the request behavior is sorted.
```
 ----- ---- ---- ---------- ----------- --------------- ------------
  c1    c2   c3   c4         c5          c6              c7
 ----- ---- ---- ---------- ----------- --------------- ------------
  aaa   11   22   1.2        1.3         1635247427000   2021-05-20
  aaa   11   22   1.200000   11.300000   1636097290000   1970-01-01
  aaa   12   22   2.200000   12.300000   1636097890000   1970-01-01
 ----- ---- ---- ---------- ----------- --------------- ------------
```
2. The window range is `2 PRECEDING AND CURRENT ROW`, so we cut out the real window in the above table, the request row is the smallest row, the previous 2 rows do not exist, but the window contains the current row, so the window has only one row (the request row).
3. Window aggregation is performed, to sum `c3` of the data in the window (only one row), and we have the result 22.

The output is:
```
 ----- ---- ----------- 
  c1    c2   w1_c3_sum   
 ----- ---- -----------
  aaa   11      22
 ----- ---- -----------
```

Example 2:
1. According to the request line and the `PARTITION BY` in window clause, filter out the lines whose `c1` is "aaa", and sort them according to `c6` from small to large. So theoretically, the intermediate data table after partition sorting is shown in the following table. The request row is the last row.
```
 ----- ---- ---- ---------- ----------- --------------- ------------
  c1    c2   c3   c4         c5          c6              c7
 ----- ---- ---- ---------- ----------- --------------- ------------
  aaa   11   22   1.200000   11.300000   1636097290000   1970-01-01
  aaa   12   22   2.200000   12.300000   1636097890000   1970-01-01
  aaa   11   22   1.2        1.3         1637000000000   2021-11-16
 ----- ---- ---- ---------- ----------- --------------- ------------
```
2. The window range is `2 PRECEDING AND CURRENT ROW`, so we cut out the real window in the above table, the request row is the largest row, so the previous 2 rows are exist, and the window contains the current row, so the window has 3 rows.
3. Window aggregation is performed, to sum `c3` of the data in the window (3 rows), and we have the result 22*3=66.

The output is:
```
 ----- ---- ----------- 
  c1    c2   w1_c3_sum   
 ----- ---- -----------
  aaa   11      66
 ----- ---- -----------
```
