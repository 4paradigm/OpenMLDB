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

import os
from .case_conf import OpenMLDB_ZK_CLUSTER, OpenMLDB_ZK_PATH
import time

# fmt:off
import sys
from pathlib import Path

# add parent directory
sys.path.append(Path(__file__).parent.parent.as_posix())
from openmldb.sdk import sdk as sdk_module
import pytest
# fmt:on


def test_sdk_smoke():
    sdk = sdk_module.OpenMLDBSdk(
        zk=OpenMLDB_ZK_CLUSTER, zkPath=OpenMLDB_ZK_PATH)
    sdk.init()
    db_name = "pydb" + str(time.time_ns() % 100000)
    table_name = "pytable" + str(time.time_ns() % 100000)
    create_db = "create database " + db_name + ";"
    ok, error = sdk.executeSQL(db_name, create_db)
    assert ok
    ok, error = sdk.executeSQL(db_name, create_db)
    assert not ok

    ddl = "create table " + table_name + \
          "(col1 string, col2 int, col3 float, col4 bigint, index(key=col1, ts=col4));"
    ok, error = sdk.executeSQL(db_name, ddl)
    assert ok
    ok, error = sdk.executeSQL(db_name, ddl)
    assert not ok

    # insert table normal
    insert_normal = "insert into " + table_name + \
                    " values('hello', 123, 3.14, 1000);"
    ok, error = sdk.executeSQL(db_name, insert_normal)
    assert ok

    # insert table placeholder
    insert_placeholder = "insert into " + table_name + " values(?, ?, ?, ?);"
    ok, row_builder = sdk.getInsertBuilder(db_name, insert_placeholder)
    row_builder.Init(5)
    row_builder.AppendString("world")
    row_builder.AppendInt32(123)
    row_builder.AppendFloat(2.33)
    row_builder.AppendInt64(1001)
    ok, error = sdk.executeInsert(db_name, insert_placeholder, row_builder)
    assert ok

    # insert table placeholder batch
    ok, rows_builder = sdk.getInsertBatchBuilder(db_name, insert_placeholder)
    row_builder1 = rows_builder.NewRow()
    row_builder1.Init(2)
    row_builder1.AppendString("hi")
    row_builder1.AppendInt32(456)
    row_builder1.AppendFloat(2.8)
    row_builder1.AppendInt64(1002)

    row_builder2 = rows_builder.NewRow()
    row_builder2.Init(4)
    row_builder2.AppendString("word")
    row_builder2.AppendInt32(789)
    row_builder2.AppendFloat(6.6)
    row_builder2.AppendInt64(1003)
    ok, error = sdk.executeInsert(db_name, insert_placeholder, rows_builder)
    assert ok

    # select
    select = "select * from " + table_name + ";"
    ok, rs = sdk.executeSQL(db_name, select)
    assert ok
    assert rs.Size() == 4

    # reset the request timeout
    sdk = sdk_module.OpenMLDBSdk(zk=OpenMLDB_ZK_CLUSTER, zkPath=OpenMLDB_ZK_PATH,
                                 request_timeout=1)
    sdk.init()
    select = "select * from " + table_name + "where col1='world';"
    # request timeout 1ms, too fast, sending rpc request will reach timeout
    ok, _ = sdk.executeSQL(db_name, select)
    assert not ok

    # drop not empty db
    drop_db = "drop database " + db_name + ";"
    ok, error = sdk.executeSQL(db_name, drop_db)
    assert not ok

    # drop table
    ok, error = sdk.executeSQL(db_name, "drop table " + table_name + ";")
    assert ok

    # drop db
    ok, error = sdk.executeSQL(db_name, drop_db)
    assert ok


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", os.path.abspath(__file__)]))
