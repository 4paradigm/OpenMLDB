# QuickStart (Standalone Version)

## Deployment
### Modify Config
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
### Start service
```bash
sh bin/start-all.sh
```
If you want to stop, execute the command as follows 
```bash
sh bin/stop-all.sh
```
## Usage
### Start CLI
```bash
# The value of host and port is the endpoint in conf/nameserver.flags
./openmldb --host 127.0.0.1 --port 6527
```
### Create DB
```sql
> CREATE DATABASE demo_db;
```

### Create Table
```sql
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date, index(ts=c7));
```
**Note**: Specify at least one index and set the `ts` column which is used for ORDERBY. The `ts` column is the key in `index` option and can be setted with `timestamp` or `bigint` column only. 
### Import Data
Only support csv file format (a sample csv file can be downloaded [here](../../demo/standalone/data/data.csv))
```sql
> USE demo_db;
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1;
```
Alternatively, you may use the below options for the advanced usage.
Name | Type |  Default | Options
-- | -- |  --  | --
delimiter | String | , | Any char
header | Boolean | true | true/false
null_value | String | null | Any String
```sql
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1 OPTIONS (delimiter=',', header=false);
```
**Node**: Only support single character for delimiter.
### Data exploration
Below demonstrates a data exploration task.
```sql
> USE demo_db;
> SET PERFORMANCE_SENSITIVE = false;
> SELECT sum(c5) as sum FROM demo_table1 where c3=11;
 ----------
  sum
 ----------
  56.000004
 ----------

1 rows in set
```
### Generate SQL
```sql
SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
### Offline feature extraction
```sql
> USE demo_db;
> SET performance_sensitive=false;
> SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```
Alternatively, you may use the below options for the advanced usage.
Name | Type |  Default | Options
-- | -- |  --  | --
delimiter | String | , | Any char
header | Boolean | true | true/false
null_value | String | null | Any String
mode | String | error_if_exists | error_if_exists/overwrite/append
```sql
> SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv' OPTIONS (mode = 'overwrite', delimiter=',');
```
### Deploy SQL to online
```sql
> USE demo_db;
> DEPLOY demo_data_service SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
We can also show and drop deployment like this:
```sql
> USE demo_db;
> SHOW DEPLOYMENTS;
 --------- -------------------
  DB        Deployment
 --------- -------------------
  demo_db   demo_data_service
 --------- -------------------
1 row in set
> DROP DEPLOYMENT demo_data_service;
```
### Online feature extraction
We can use RESTful APIs to execute online feature extraction.  
The format of url is as follows:
```
http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service
        \___________/      \____/              \_____________/
              |               |                        |
       APIServer endpoint  Database name        Deployment name
```
As input data is in the json format, we should pack rows into `input` values.  
Ex:
```bash
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{
"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]
}'
```
The ip and port in url is `endpoint` in `conf/apiserver.flags`

In order to better understand the workflow, we use Kaggle Competition [Predict Taxi Tour Duration Dataset](https://github.com/4paradigm/OpenMLDB/tree/main/demo/predict-taxi-trip-duration-nb/script/data)
to demostrate the whole process. Dataset and source code can be found
[here](https://github.com/4paradigm/OpenMLDB/tree/main/demo/predict-taxi-trip-duration-nb/script).
