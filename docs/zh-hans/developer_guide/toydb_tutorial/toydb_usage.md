
## 编译
```shell
mkdir -p ./hybridse/build
cd ./hybridse/build/

cmake .. -DEXAMPLES_ENABLE=ON
make -j4
```

## 启动
```shell
cd ../examples/toydb/onebox
sh start_all.sh
sh start_cli.sh
```

### 启动命令说明

#### 启动dbms
```shell script
BUILD_DIR=../../../build/examples/toydb
${BUILD_DIR}/src/fesql --role=dbms  --fesql_port=9211  >dbms.log 2>&1 &
```

#### 启动tablet

```shell script
BUILD_DIR=../../../build/examples/toydb
${BUILD_DIR}/src/fesql --role=tablet --fesql_endpoint=127.0.0.1:9212 --fesql_port=9212 --dbms_endpoint=127.0.0.1:9211 >tablet.log 2>&1 &
```
#### 启动简易CLI客户端
```shell
BUILD_DIR=../../../build/examples/toydb
${BUILD_DIR}/src/fesql --role=client --tablet_endpoint=127.0.0.1:9212 --fesql_endpoint=127.0.0.1:9211
```


## ToyDB使用示例

### 创建数据库和表
#### 创建数据库

```mysql
CREATE DATABASE db_name
```

#### 进入数据库

```MYSQL
USE db_name;
```

#### 查看所有数据库列表信息

```mysql
 SHOW DATABASES;
```

#### 建表

```mysql
-- create table t1
create table IF NOT EXISTS t1(
    column1 int NOT NULL,
    column2 int NOT NULL,
    column3 float NOT NULL,
    column4 bigint NOT NULL,
    column5 int NOT NULL,
    column6 string,
    index(key=column1, ts=column4)
);
```

#### 查看表结构

```SQL
DESC t1;
+---------+---------+------+
| Field   | Type    | Null |
+---------+---------+------+
| column1 | kInt64  | NO   |
| col2    | kString | NO   |
| col3    | kFloat  | NO   |
+---------+---------+------+
```

#### 查看当前数据库下表信息

```mysql
SHOW TABLES;
```

#### 插入表数据

```SQL
-- prepare t1 data 
insert into t1 values(1, 2, 3.3, 1000, 5, "hello");
insert into t1 values(1, 3, 4.4, 2000, 6, "world");
insert into t1 values(11, 4, 5.5, 3000, 7, "string1");
insert into t1 values(11, 5, 6.6, 4000, 8, "string2");
insert into t1 values(11, 6, 7.7, 5000, 9, "string3");
insert into t1 values(1, 2, 3.3, 1000, 5, "hello");
insert into t1 values(1, 3, 4.4, 2000, 6, "world");
insert into t1 values(11, 4, 5.5, 3000, 7, "string1");
insert into t1 values(11, 5, 6.6, 4000, 8, "string2");
insert into t1 values(11, 6, 7.7, 5000, 9, "string3");
```



### 查询SQL

#### 简单的查询语句
```sql
-- simple query 
SELECT column1, column2, column1 + (2*column5) as f1 FROM t1 limit 10;
```

### 查询window聚合结果

```sql
-- window query t1
select
sum(column1) OVER w1 as w1_col1_sum, 
sum(column2) OVER w1 as w1_col2_sum, 
sum(column3) OVER w1 as w1_col3_sum, 
sum(column4) OVER w1 as w1_col4_sum, 
sum(column5) OVER w1 as w1_col5_sum 
FROM t1 WINDOW w1 AS (PARTITION BY column1 ORDER BY column4 ROWS BETWEEN 3000 PRECEDING AND CURRENT ROW) limit 100;

```
