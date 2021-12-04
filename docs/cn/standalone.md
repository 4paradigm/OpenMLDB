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
## 2. 使用指南
### 2.1 启动 CLI
```bash
# host为conf/nameserver.flags中配置endpoint的ip, port为对应的port
./openmldb --host 127.0.0.1 --port 6527
```
### 2.2. 创建数据库和表

```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date, INDEX(key=c1, ts=c6));
```
`CREATE TABLE` 的 `INDEX` 函数接受两个重要的参数  `key` 和 `ts`。关于这两个重要参数的如下说明：

- `key` 代表了索引列。如果创建表的时候不确定，也可以不进行指定，系统将默认使用第一个符合条件的列作为索引列。在部署上线的过程中，OpenMLDB 将会根据实际使用的 SQL 脚本，自动进行优化和创建所需要的索引。
- `ts` 列是指定的有序列，并且用来作为 `ORDER BY` 的列。只有 `timestamp` 或者 `bigint` 类型的列才能作为 `ts` 列。
- `ts` 和 `key` 也影响在离线模式下某些没有命中索引的 SQL 是否可以执行，该话题和性能敏感模式有关，详情参考：[OpenMLDB 性能敏感模式说明](performance_sensitive_mode.md) 。

### 2.3. 导入数据
 导入离线数据用于特征计算（示例csv文件可以在[这里](../../demo/standalone/data/data.csv)下载）。
```sql
> USE demo_db;
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1;
```
### 2.4. 离线特征计算

如下的 SQL 脚本将会执行特征抽取脚本，并且将生成的特征存储在一个文件中，供后续的模型训练使用。

```sql
> USE demo_db;
> SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```
### 2.5. SQL 方案上线
将探索好的SQL方案部署到线上，注意部署上线的 SQL 方案需要与对应的离线特征计算的 SQL 方案保持一致。
```sql
> USE demo_db;
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
上线后可以通过命令 `SHOW DEPLOYMENTS` 查看已部署的 SQL 方案；也可以使用 `DROP DEPLOYUMENT` 命令将已上线方案删除。

:bulb: 注意，在本例子中，我们使用了同一份数据做线下和线上特征计算。在实际部署中，往往需要倒入另一份最新的数据来做 SQL 方案上线。

### 2.6. 实时特征计算

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

### 2.7 Demo 

我们也提供了一个完整的应用开发 demo 来演示整个开发和上线流程，具体运行步骤可以在此[查看](https://github.com/4paradigm/OpenMLDB/tree/main/demo)。

