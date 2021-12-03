# OpenMLDB QuickStart (Standalone Mode)

## 1. Deployment
### 1.1 Configuration
1. Please replace `127.0.0.1` with the server IP if you want to access remotely
2. (Optional) Please change the port number if it is occupied by another service

* conf/tablet.flags
   ```bash
   --endpoint=127.0.0.1:9921
   ```
* conf/nameserver.flags
   ```bash
   --endpoint=127.0.0.1:6527
   # set the same value as the endpoint in conf/tablet.flags
   --tablet=127.0.0.1:9921
   ```
* conf/apiserver.flags
   ```bash
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
## 2. Usage Guideline
### 2.1. Starting CLI
```bash
# The value of host and port is the endpoint in conf/nameserver.flags
./openmldb --host 127.0.0.1 --port 6527
```
### 2.2. Creating Database and Tables
```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date, INDEX(key=c1, ts=c6));
```
The `INDEX` function used in `CREATE TABLE` accepts two important parameters `key` and `ts`. 

- `key` specifies the index column. If it is not specified, OpenMLDB will use the first applicable column as the index column automatically. Necessary indexes will be built when deploying online SQL. 
- `ts` specifies the ordered column, which is used by `ORDER BY`. Only the `timestamp` and `bigint` columns can be specified as `ts`. 
- `ts` and `key` also determine whether certain SQL queries can be executed offline, please refer to [Introduction to OpenMLDB's Performance-Sensitive Mode](performance_sensitive_mode.md) for more details.

### 2.3. Data Import
We import a `csv` file for feature extraction (a sample csv file can be downloaded [here](../../demo/standalone/data/data.csv)).
```sql
> USE demo_db;
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1;
```
### 2.4. Offline Feature Extraction

The below SQL performs the feature extraction and outputs the result features into a file, which can be used by subsequent model training algorithms.

```sql
> USE demo_db;
> SELECT c1, c2, sum(c3) OVER w1 as w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```
### 2.5. Online SQL Deployment

Now we are ready to deploy our feature extraction SQL for the online serving. Note that the same SQL script should be used for both offline and online feature extraction.

```sql
> USE demo_db;
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
After that, we can list deployments like using the command `SHOW DEPLOYMENTS`, and also can discard a deployment by `DROP DEPLOYMENT`.

:bulb: Note that, in this example, the same data set is used for both offline and online feature extraction. In practice, importing another recent data set is usually necessary.

### 2.6. Online Feature Extraction

We can use RESTful APIs to execute online feature extraction. The format of URL is as follows:

```bash
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

### 2.7 Demo

In order to better understand the workflow, we provide a complete demo that can be found [here](https://github.com/4paradigm/OpenMLDB/tree/main/demo).
