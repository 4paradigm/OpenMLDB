#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# test_query.py
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

#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
#

"""

"""

from test_base import *
import time
import fesql

def test_query():
    dbms = fesql.CreateDBMSSdk(dbms_endpoint)
    db1 = "name" + str(time.time())
    sql = "create table t1(col1 string, col2 bigint, col3 float, index(key=col1, ts=col2));"
    status = fesql.Status()
    dbms.CreateDatabase(db1, status)
    assert status.code == 0
    ok, msg, rs = exec_query(dbms, db1, sql)
    if not ok:
        print(msg)
    assert ok
    insert1 = "insert into t1 values('hello', 10, 1.2);"
    ok, msg, rs = exec_query(dbms, db1, insert1)
    assert ok
    query = "select col1, col2 from t1;"
    ok, msg, rs = exec_query(dbms, db1, query)
    assert ok
    assert rs.Next()
    assert rs.GetStringUnsafe(0) == 'hello'
    assert rs.GetInt64Unsafe(1) == 10

def test_request_query():
    dbms = fesql.CreateDBMSSdk(dbms_endpoint)
    db1 = "name2" + str(time.time())
    sql = "create table t1(col0 bigint, col1 string, col2 bigint, col3 float, index(key=col1, ts=col2));"
    status = fesql.Status()
    dbms.CreateDatabase(db1, status)
    assert status.code == 0
    ok, msg, rs = exec_query(dbms, db1, sql)
    assert ok
    query = "select col0, col1, col2 + 1 from t1;"
    row = dbms.GetRequestRow(db1, query, status) 
    assert status.code == 0
    assert row.Init(5)
    assert row.AppendInt64(64)
    assert row.AppendString("hello")
    assert row.AppendInt64(10)
    assert row.AppendFloat(1.0)
    assert row.Build()
    rs = dbms.ExecuteQuery(db1, query, row, status)
    assert status.code == 0
    assert rs
    assert rs.Next()
    assert rs.GetSchema().GetColumnCnt() == 3
    assert rs.GetInt64Unsafe(0) == 64
    assert rs.GetStringUnsafe(1) == "hello"
    assert rs.GetInt64Unsafe(2) == 11
