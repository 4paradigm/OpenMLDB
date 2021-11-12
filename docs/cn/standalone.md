# 快速上手（单机版）

## 部署
### 修改配置文件
1. 如果需要在其他节点执行命令和访问http, 需要把`127.0.0.1`替换成机器ip地址
2. 如果端口被占用需要改成其他端口

* conf/tablet.flags
   ```
   --endpoint=127.0.0.1:9921
   ```
* conf/nameserver.flags
   ```
   --endpoint=127.0.0.1:6527
   # 和conf/tablet.flags中endpoint保持一致
   --tablet=127.0.0.1:9921
   #--zk_cluster=127.0.0.1:7181
   #--zk_root_path=/openmldb_cluste
   ```
* conf/apiserver.flags
   ```
   --endpoint=127.0.0.1:8080
   # 和conf/nameserver.flags中endpoint保存一致
   --nameserver=127.0.0.1:6527
   ```
### 启动服务
```
sh bin/start-all.sh
```
如需停止, 执行如下命令 
```
sh bin/stop-all.sh
```
## 使用
### 启动CLI
```bash
# host为conf/nameserver.flags中配置endpoint的ip, port为对应的port
./openmldb --host 127.0.0.1 --port 6527
```
### 创建DB
```sql
> CREATE DATABASE demo_db;
```

### 创建表
```sql
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date, index(ts=c7));
```
**注**: 需要至少指定一个index并设置`ts`列。`ts`列是用来做ORDERBY的那一列
### 导入数据
只支持导入本地csv文件（示例csv文件可以在[这里](../../demo/standalone/data/data.csv)下载）
```sql
> USE demo_db;
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1;
```
可以通过option指定额外的配置
Name | Meaning | Type |  Default | Options
-- | -- | -- |  --  | --
delimiter | 分隔符| String | , | Any char
header | 是否有header| Boolean | true | true/false
null_value | null值 | String | null | Any String
```sql
> LOAD DATA INFILE '/tmp/data.csv' INTO TABLE demo_table1 OPTIONS (delimiter=',', header=false);
```
**注**: 分隔符只支持单个字符
### 分析数据
对数据集进行离线分析，为编写SQL语句进行特征抽取提供参考
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
### 生成方案SQL
```sql
SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
### 批量计算特征
```sql
> USE demo_db;
> SET performance_sensitive=false;
> SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```
可以通过option指定额外的配置
Name | Meaning | Type |  Default | Options
-- | -- | -- |  --  | --
delimiter | 分隔符| String | , | Any char
header | 是否有header| Boolean | true | true/false
null_value | null值 | String | null | Any String
mode | 模式 | String | error_if_exists | error_if_exists/overwrite/append
```sql
> SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv' OPTIONS (mode = 'overwrite', delimiter=',');
```
### SQL方案上线
将探索好的SQL方案Deploy到线上
```sql
> USE demo_db;
> DEPLOY demo_data_service SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
上线后可以查看和删除SQL方案
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
### 实时特征计算
url的格式为:
```
http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service
        \___________/      \____/              \_____________/
              |               |                        |
        APIServer地址     Database名字            Deployment名字
```
输入数据是一个json，把一行数据放到`input`的value中  
示例:
```bash
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{
"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]
}'
```
URL中ip和port是`conf/apiserver.flags`中配置的`endpoint`
