# fedb cli使用说明

## 下载命令工具

目前命令行工具支持linux 和 macos 两个操作系统
* linux 下载地址 http://pkg.4paradigm.com/fedb/linux/fedb
* mac 下载地址 http://pkg.4paradigm.com/fedb/mac/fedb

## 命令行

### 连接集群

```
./fedb --zk_cluster=xxxx --zk_root_path=xxxx --role=sql_client 2>/dev/null
  ______   _____  ___
 |  ____|  |  __ \|  _ \
 | |__ ___ | |  | | |_) |
 |  __/ _  \ |  | |  _ <
 | | |  __ / |__| | |_) |
 |_|  \___||_____/|____/

v2.0.0.0
172.27.128.81:5527/>
```

参数解释
* zk_cluster使用部署集群的zk地址
* zk_root_path使用部署集群zk的跟路径

### 创建 database

```
172.27.128.81:5527/> create database db1;
Create database success
172.27.128.81:5527/> show databases;
 -----------
  Databases
 -----------
  db1
 -----------
2 rows in set
```

### 进入database 

```
172.27.128.81:5527/> use db1;
Database changed
```

### 创建一个表

```
172.27.128.81:5527/db1> create table t1(
                     -> col1 string,
                     -> col2 timestamp,
                     -> col3 double,
                     -> index(key=col1, ts=col2));
172.27.128.81:5527/db1> show tables;
 --------
  Tables
 --------
  t1
 --------
1 row in set
172.27.128.81:5527/db1> desc t1;
 ------- ------------ ------
  Field   Type         Null
 ------- ------------ ------
  col1    kVarchar     YES
  col2    kTimestamp   YES
  col3    kDouble      YES
 ------- ------------ ------
3 rows in set
```

### 写入一条数据

```
172.27.128.81:5527/db1> insert into t1 values('hello world!', 1590738989000L, 10.0);
172.27.128.81:5527/db1> select * from t1;
 -------------- --------------- -----------
  col1           col2            col3
 -------------- --------------- -----------
  hello world!   1590738989000   10.000000
  hello world!   1590738988000   1.000000
 -------------- --------------- -----------

2 rows in set
```


