# OpenMLDB 快速上手指南（单机模式）

## 1. 部署
### 1.1 修改配置文件
1. 如果需要在其他节点执行命令和访问 http, 需要把`127.0.0.1`替换成机器 ip 地址
2. 如果端口被占用需要改成其他端口

* conf/tablet.flags
   ```bash
   --endpoint=127.0.0.1:9921
   ```
* conf/nameserver.flags
   ```bash
   --endpoint=127.0.0.1:6527
   # 和conf/tablet.flags中endpoint保持一致
   --tablet=127.0.0.1:9921
   ```
* conf/apiserver.flags
   ```bash
   --endpoint=127.0.0.1:8080
   # 和conf/nameserver.flags中endpoint保存一致
   --nameserver=127.0.0.1:6527
   ```
### 1.2. 启动服务
```bash
sh bin/start-all.sh
```
如需停止, 执行如下命令 
```bash
sh bin/stop-all.sh
```
## 2. 使用
### 2.1 启动 CLI
```bash
# host为conf/nameserver.flags中配置endpoint的ip, port为对应的port
./openmldb --host 127.0.0.1 --port 6527
```
### 2.2. 创建数据库和表

```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date, INDEX(ts=c6));
```
`INDEX` 函数接受两个重要的参数  `ts` 和 `key`。关于这两个重要参数的如下说明：

- `ts` 列是指定的有序列，并且用来作为 `ORDER BY` 的列。只有 `timestamp` 或者 `bigint` 类型的列才能作为 `ts` 列。`ts` 如果不指定，系统将自动使用该行数据导入到数据库的时间戳作为 `ts`。
- `key` 代表了索引列。如果不进行指定，则系统将默认使用第一个符合条件的列作为索引列。在部署上线的过程中，OpenMLDB 将会根据实际使用的 SQL 脚本，自动进行优化和创建所需要的索引。我们也可以通过多个指定 `INDEX` 函数来建立多个索引。
- `ts` 和 `key` 也影响在离线模式下某些没有命中索引的 SQL 是否可以执行，该话题和性能敏感模式有关，详情参考：[OpenMLDB 性能敏感模式说明](performance_sensitive_mode.md) 。在本例中，由于没有指定 `key`，所以后续的离线特征计算都需要指定 `PERFORMANCE_SENSITIVE=false` 。

### 2.3. 导入数据
目前只支持导入本地csv文件（示例csv文件可以在[这里](../../demo/standalone/data/data.csv)下载）
```sql
> USE demo_db;
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1;
```
你也可以通过 `OPTIONS` 指定额外的配置
Name | Meaning | Type |  Default | Options
-- | -- | -- |  --  | --
delimiter | 分隔符| String | , | Any char
header | 是否有header| Boolean | true | true/false
null_value | null值 | String | null | Any String

比如：

```sql
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1 OPTIONS (delimiter=',', header=false);
```
**注**: 分隔符只支持单个字符

### 2.4. 分析数据
对数据集进行离线分析，为编写SQL语句进行特征抽取提供参考。注意，该过程只是为了更好地探索数据分布和调试特征抽取方案，对于特征开发和上线并非必须步骤。
```sql
> USE demo_db;
> SET PERFORMANCE_SENSITIVE=false;
> SELECT sum(c4) AS sum FROM demo_table1 WHERE c2=11;
 ----------
  sum
 ----------
  1.200000
 ----------

1 rows in set
```
### 2.5. 离线特征计算

如下的 SQL 脚本将会执行特征抽取脚本，并且将生成的特征存储在一个文件中，供后续的模型训练使用。

```sql
> USE demo_db;
> SET performance_sensitive=false;
> SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```
你也可以通过 `OPTIONS` 指定额外的配置
Name | Meaning | Type |  Default | Options
-- | -- | -- |  --  | --
delimiter | 分隔符| String | , | Any char
header | 是否有header| Boolean | true | true/false
null_value | null值 | String | null | Any String
mode | 模式 | String | error_if_exists | error_if_exists/overwrite/append

比如：

```sql
> SELECT c1, c2, sum(c3) OVER w1 as w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv' OPTIONS (mode = 'overwrite', delimiter=',');
```
### 2.6. SQL 方案上线
将探索好的SQL方案部署到线上，注意部署上线的 SQL 方案需要与对应的离线特征计算的 SQL 方案保持一致。另外，你也可以同时部署多个方案，OpenMLDB 可以同时对多个部署的方案进行线上服务。
```sql
> USE demo_db;
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
上线后可以查看SQL方案
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
如果稍后你不需要某个特定的 SQL 方案，你可以使用 `DROP DEPLOYUMENT` 命令将其删除。

### 2.7. 实时特征计算

实时线上服务可以通过如下 Web API 提供服务：
```
http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service
        \___________/      \____/              \_____________/
              |               |                        |
        APIServer地址     Database名字            Deployment名字
```
输入数据接受 `json` 格式，因此我们把一行数据放到 `input` 的value中。如下示例:

```bash
> curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]}'

# 预期的返回结果：
# {"code":0,"msg":"ok","data":{"data":[["aaa",11,22]],"common_cols_data":[]}}
```
URL 中 IP 和 端口号是 `conf/apiserver.flags` 中配置的 `endpoint`。

为了方便理解，我们会基于Kaggle比赛[Predict Taxi Tour Duration数据集](https://github.com/4paradigm/OpenMLDB/tree/main/demo/predict-taxi-trip-duration-nb/script/data)，来介绍整个流程。数据集和相关代码可以在[这里](https://github.com/4paradigm/OpenMLDB/tree/main/demo/predict-taxi-trip-duration-nb/script)查看，并且可以根据步骤尝试运行。

