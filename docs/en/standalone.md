# OpenMLDB QuickStart (Standalone Mode)

## 1. Deployment
### 1.1 Configuration
1. Please replace `127.0.0.1` with the server IP if you want to access remotely
2. (Optional) Please change the port number if it is occupied by another service

* conf/tablet.flags
   ```
   --endpoint=127.0.0.1:9921
   ```
* conf/nameserver.flags
   ```
   --endpoint=127.0.0.1:6527
   # set the same value as the endpoint in conf/tablet.flags
   --tablet=127.0.0.1:9921
   #--zk_cluster=127.0.0.1:7181
   #--zk_root_path=/openmldb_cluste
   ```
* conf/apiserver.flags
   ```
   --endpoint=127.0.0.1:8080
   # set the same value as the endpoint in conf/nameserver.flags
   --nameserver=127.0.0.1:6527
   ```
### 1.2. Starting service
```bash
sh bin/start-all.sh
```
If you want to stop, execute the command as follows 
```bash
sh bin/stop-all.sh
```
## 2. Usage
### 2.1. Starting CLI
```bash
# The value of host and port is the endpoint in conf/nameserver.flags
./openmldb --host 127.0.0.1 --port 6527
```
### 2.2. Creating Database and Tables
```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date, INDEX(ts=c6));
```
The `INDEX` function accept two important parameters `ts` and `key`. 

- `ts` specifies the ordered column, which is used by `ORDER BY`. Only the `timestamp` and `bigint` columns can be specified as `ts`. If `ts ` is not specified, OpenMLDB will use the timestamps of importing the row as the `ts` column.
- `key` specifies the index column. If it is not specified, OpenMLDB will use the first applicable column as the index column automatically. Necessary indexes will be built when deploying online SQL. You can also specific multiple indexes by more than one `INDEX` functions.
- `ts` and `key` also determine whether certain SQL queries can be executed, please refer to [Introduction to OpenMLDB's Performance-Sensitive Mode](performance_sensitive_mode.md) for more details. In this example, as `key` is unspecified, thus most offline queries have to be executed with `PERFORMANCE_SENSITIVE=false` .

### 2.3. Data Import
Currently, only the `csv` file format is supported (a sample csv file can be downloaded [here](../../demo/standalone/data/data.csv)).
```sql
> USE demo_db;
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1;
```
Alternatively, `OPTIONS` can be used for extra arguments.
Name | Type |  Default | Options
-- | -- |  --  | --
delimiter | String | , | Any char
header | Boolean | true | true/false
null_value | String | null | Any String

For example:

```sql
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1 OPTIONS (delimiter=',', header=false);
```
**Node**: Only a single character for delimiter is supported.

### 2.4. Data Exploration
Below demonstrates a data exploration task. Note that this is not necessary but helpful for feature extraction script development.
```sql
> USE demo_db;
> SET PERFORMANCE_SENSITIVE = false;
> SELECT sum(c4) as sum FROM demo_table1 where c2=11;
 ----------
  sum
 ----------
  1.200000
 ----------

1 rows in set
```

### 2.5. Offline Feature Extraction

The below SQL performs the feature extraction and outputs the result features into a file, which can be used by subsequent model training algorithms.

```sql
> USE demo_db;
> SET performance_sensitive=false;
> SELECT c1, c2, sum(c3) OVER w1 as w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```
Alternatively, `OPTIONS` can be used for extra arguments. 
Name | Type |  Default | Options
-- | -- |  --  | --
delimiter | String | , | Any char
header | Boolean | true | true/false
null_value | String | null | Any String
mode | String | error_if_exists | error_if_exists/overwrite/append

For example:

```sql
> SELECT c1, c2, sum(c3) OVER w1 as w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv' OPTIONS (mode = 'overwrite', delimiter=',');
```
### 2.6. Online SQL Deployment

Now we are ready to deploy our feature extraction solution for the online serving. Furthermore, you can deploy multiple SQL solutions, and OpenMLDB is able to serve multiple online deployments concurrently.

```sql
> USE demo_db;
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
After that, we can list deployments like this:
```sql
> USE demo_db;
> SHOW DEPLOYMENTS;
 --------- -------------------
  DB        Deployment
 --------- -------------------
  demo_db   demo_data_service
 --------- -------------------
1 row in set
```
Later on, if you would like to discard a deployment, you can use the command `DROP DEPLOYMENT`.

### 2.7. Online Feature Extraction

We can use RESTful APIs to execute online feature extraction. The format of URL is as follows:

```
http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service
        \___________/      \____/              \_____________/
              |               |                        |
       APIServer endpoint  Database name        Deployment name
```
As input data is in `json` format, we should pack rows into the `input` field.  For example:

```bash
> curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]}'

# the expected return:
# {"code":0,"msg":"ok","data":{"data":[["aaa",11,22]],"common_cols_data":[]}}
```
The IP and port number in the URL are `endpoint` configured in `conf/apiserver.flags`

In order to better understand the workflow, we use Kaggle Competition [Predict Taxi Tour Duration Dataset](https://github.com/4paradigm/OpenMLDB/tree/main/demo/predict-taxi-trip-duration-nb/script/data) to demonstrate the whole process. Dataset and source code can be found [here](https://github.com/4paradigm/OpenMLDB/tree/main/demo).
