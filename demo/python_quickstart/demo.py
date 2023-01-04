#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


## 2. 使用OpenMLDB DBAPI

### 2.1 创建connection

import openmldb.dbapi

# 连接集群版OpenMLDB
db = openmldb.dbapi.connect(zk="127.0.0.1:2181", zkPath="/openmldb")

# 连接单机版OpenMLDB
# db = openmldb.dbapi.connect(host="$host", port="$port")

cursor = db.cursor()

### 2.2 创建数据库并使用
cursor.execute("CREATE DATABASE db1")
cursor.execute("USE db1")

### 2.3 创建表
cursor.execute(
    "CREATE TABLE t1 (col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1))"
)

### 2.4 插入数据到表中
cursor.execute("INSERT INTO t1 VALUES(1000, '2020-12-25', 'guangdon', 'shenzhen', 1)")

### 2.5 执行SQL查询
result = cursor.execute("SELECT * FROM t1")
print(result.fetchone())
print(result.fetchmany(10))
print(result.fetchall())


### 2.6 SQL批请求式查询
# Batch Request模式，接口入参依次为“SQL”, “Common_Columns”, “Request_Columns”
result = cursor.batch_row_request(
    "SELECT * FROM t1",
    ["col1", "col2"],
    (
        {
            "col1": 2000,
            "col2": "2020-12-22",
            "col3": "fujian",
            "col4": "xiamen",
            "col5": 2,
        }
    ),
)
print(result.fetchone())


### 2.7 删除表
cursor.execute("DROP TABLE t1")

### 2.8 删除数据库

cursor.execute("DROP DATABASE db1")

### 2.9 关闭连接

cursor.close()

## 3. 使用OpenMLDB SQLAlchemy

### 3.1 创建connection
import sqlalchemy as db

# 连接集群版OpenMLDB
engine = db.create_engine("openmldb:///?zk=127.0.0.1:2181&zkPath=/openmldb")

# 连接单机版OpenMLDB
# engine = db.create_engine('openmldb:///?host=127.0.0.1&port=6527')

connection = engine.connect()

### 3.2 创建数据库
try:
    connection.execute("CREATE DATABASE db1")
except Exception as e:
    print(e)

connection.execute("USE db1")

### 3.3 创建表
try:
    connection.execute(
        "CREATE TABLE t1 ( col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1))"
    )
except Exception as e:
    print(e)

### 3.4 插入数据到表中
try:
    connection.execute(
        "INSERT INTO t1 VALUES(1000, '2020-12-25', 'guangdon', 'shenzhen', 1);"
    )
except Exception as e:
    print(e)

# 使用`connection.execute(ddl, data)`接口执行带planceholder的SQL的插入语句，可以动态指定插入数据，也可插入多行：
try:
    insert = "INSERT INTO t1 VALUES(1002, '2020-12-27', ?, ?, 3);"
    connection.execute(insert, ({"col3": "fujian", "col4": "fuzhou"}))
    connection.execute(
        insert,
        [
            {"col3": "jiangsu", "col4": "nanjing"},
            {"col3": "zhejiang", "col4": "hangzhou"},
        ],
    )
except Exception as e:
    print(e)

### 3.5 执行SQL批式查询
try:
    rs = connection.execute("SELECT * FROM t1")
    for row in rs:
        print(row)
    rs = connection.execute("SELECT * FROM t1 WHERE col3 = ?;", ("hefei"))
    rs = connection.execute(
        "SELECT * FROM t1 WHERE col3 = ?;", [("hefei"), ("shanghai")]
    )
except Exception as e:
    print(e)


### 3.6 执行SQL请求式查询

# 使用`connection.execute(sql, request)`接口执行SQL批式查询语句:请求式查询，可以把输入数据放到execute的第二个参数中
try:
    rs = connection.execute(
        "SELECT * FROM t1",
        (
            {
                "col1": 9999,
                "col2": "2020-12-27",
                "col3": "zhejiang",
                "col4": "hangzhou",
                "col5": 100,
            }
        ),
    )
except Exception as e:
    print(e)

### 3.7 删除表
try:
    connection.execute("DROP TABLE t1")
except Exception as e:
    print(e)

### 3.8 删除数据库
try:
    connection.execute("DROP DATABASE db1")
except Exception as e:
    print(e)
