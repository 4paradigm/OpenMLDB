#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
#

"""

"""

from test_base import *
import fesql

def test_query():
    dbms = fesql.CreateDBMSSdk(dbms_endpoint)
    db1 = "name"
    sql = "create table t1(col1 string, col2 bigint, col3 float, index(key=col1, ts=col2));"
    status = fesql.Status()
    dbms.CreateDatabase(db1, status)
    assert status.code == 0
    ok, msg, rs = exec_query(dbms, db1, sql)
    assert ok
    insert1 = "insert into t1 values('hello', 10, 1.2);"
    ok, msg, rs = exec_query(dbms, db1, insert1)
    assert ok
    query = "select col1, col2 from t1;"
    ok, msg, rs = exec_query(dbms, db1, query)
    assert ok
    assert rs.Next()
