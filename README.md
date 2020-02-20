# FeSQL

The first and fastest AI Native Database in the world

## build 

安装基础工具
* wget
* unzip
* texinfo
* gcc 8.3.1

Linux安装依赖库

```
sh tools/get_deps.sh
```

Mac下安装依赖库(缺RocksDB和zookeepr)

```shell
brew tap iveney/mocha
brew install realpath
brew install gettext
brew install gettext autoconf
brew install libtool
brew install pinfo
ln -s `brew ls gettext | grep bin/autopoint` /usr/local/bin

sh tools/get_deps.sh mac
```

编译

```
mkdir build && cd build && cmake .. && make -j4
```

运行测试

```
cd build && make test
```

运行覆盖统计

```
cd build && make coverage
```

## 添加测试

按照如下添加测试，方便make test能够运行
```
add_executable(flatbuf_ir_builder_test flatbuf_ir_builder_test.cc)
target_link_libraries(flatbuf_ir_builder_test gflags fesql_codegen fesql_proto ${llvm_libs} protobuf glog gtest pthread)
add_test(flatbuf_ir_builder_test flatbuf_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/flatbuf_ir_builder_test.xml)
```

## 添加覆盖率

将测试target append到test_list里面，方便make coverage能够运行
```
list(APPEND test_list flatbuf_ir_builder_test)
```

## 规范检查

请使用tools/cpplint.py 检查代码规范,
如果有一些格式问题，可以使用clang-format去格式化代码
```
tools/cpplint.py  filename
clang-format filename
```

## LocalRun环境

```
cd onebox
# 启动dbms 和 tablet
sh start_all.sh
# 使用 cli访问
sh start_cli.sh
```

## CMD使用说明

### 启动服务端

```shell script
./fesql --role=dbms --port=9111 --log_dir=./log/dbms &
```

### 启动客户端

```shell script
./fesql --role=client --endpoint=127.0.0.1:9111 --log_dir=./log/client
```


## FeSQL CMD 

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

#### 查看当前数据库下表信息

```mysql
SHOW TABLES;
```

#### 创建schema

```mysql
CREATE TABLE t1 (
column1 int,
col2 string,
col3 float
);
```

#### 查看表schema

```SQL
DESC table_name;
+---------+---------+------+
| Field   | Type    | Null |
+---------+---------+------+
| column1 | kInt32  | NO   |
| col2    | kString | NO   |
| col3    | kFloat  | NO   |
+---------+---------+------+
```


### 查询SQL

#### simple udf query
```sql
create table IF NOT EXISTS t1(
    column1 int NOT NULL,
    column2 int NOT NULL,
    column3 float NOT NULL,
    column4 bigint NOT NULL,
    column5 int NOT NULL,
    column6 string,
    index(key=column1, ts=column4)
);
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
%%fun
def test(a:i32,b:i32):i32
    c=a+b
    d=c+1
    return d
end
%%sql
SELECT column1, column2,test(column1,column5) as f1 FROM t1 limit 10;

+---------+---------+----+
| column1 | column2 | f1 |
+---------+---------+----+
| 1       | 3       | 8  |
| 1       | 3       | 8  |
| 1       | 2       | 7  |
| 1       | 2       | 7  |
| 11      | 6       | 21 |
| 11      | 6       | 21 |
| 11      | 5       | 20 |
| 11      | 5       | 20 |
| 11      | 4       | 19 |
| 11      | 4       | 19 |
+---------+---------+----+
```

### 查询window聚合结果
```sql
create table IF NOT EXISTS t2(
    column1 int NOT NULL,
    column2 int NOT NULL,
    column3 float NOT NULL,
    column4 bigint NOT NULL,
    column5 int NOT NULL,
    column6 string,
    index(key=column6, ts=column4)
);
insert into t2 values(1, 2, 3.3, 1000, 5, "hello");
insert into t2 values(1, 3, 4.4, 2000, 6, "world");
insert into t2 values(11, 4, 5.5, 3000, 7, "string1");
insert into t2 values(11, 5, 6.6, 4000, 8, "string2");
insert into t2 values(11, 6, 7.7, 5000, 9, "string3");
insert into t2 values(1, 2, 3.3, 1000, 5, "hello");
insert into t2 values(1, 3, 4.4, 2000, 6, "world");
insert into t2 values(11, 4, 5.5, 3000, 7, "string1");
insert into t2 values(11, 5, 6.6, 4000, 8, "string2");
insert into t2 values(11, 6, 7.7, 5000, 9, "string3");

select column1, column2, column3, column4, column5, column6 from t2 limit 100;
select
sum(column1) OVER w1 as w1_col1_sum, 
sum(column2) OVER w1 as w1_col2_sum, 
sum(column3) OVER w1 as w1_col3_sum, 
sum(column4) OVER w1 as w1_col4_sum, 
sum(column5) OVER w1 as w1_col5_sum 
FROM t2 WINDOW w1 AS (PARTITION BY column1 ORDER BY column4 ROWS BETWEEN 3000 PRECEDING AND CURRENT ROW) limit 100;


```


## UDF
### 算术表达式
```python
def test(int:a):int
    return a+1
end
```
### 逻辑和关系表达式
```python
def test(int:a):int
    return a>1
end
```
### 条件表达式
```python
def test(int:a):int
    if a > 1 
        return 1
    else
        return 0
end
```

### 循环表达式

```python
def test(list<int>:elements):int
    result = 0
    for x in elements
        result += x
end
```





