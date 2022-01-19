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

from openmldb_sdk import driver
import case_conf
import time

def test_smoke():
    print("hello")
    options = driver.DriverOptions(case_conf.FEDB_ZK_CLUSTER,
                                   case_conf.FEDB_ZK_PATH)
    sdk = driver.Driver(options)
    assert sdk.init()
    db_name = "pydb" + str(time.time_ns()%100000)
    table_name = "pytable" + str(time.time_ns()%100000)
    ok, error = sdk.createDB(db_name)
    assert ok == True
    ok, error = sdk.createDB(db_name)
    assert ok == False

    # create table
    ddl = "create table " + table_name + "(col1 string, col2 int, col3 float, col4 bigint, index(key=col1, ts=col4));"
    ok, error = sdk.executeDDL(db_name, ddl)
    assert ok == True
    ok, error = sdk.executeDDL(db_name, ddl)
    assert ok == False

    # insert table normal
    insert_normal = "insert into " + table_name + " values('hello', 123, 3.14, 1000);"
    ok, error = sdk.executeInsert(db_name, insert_normal)
    assert ok == True

    # insert table placeholder
    insert_placeholder = "insert into " + table_name + " values(?, ?, ?, ?);"
    ok, row_builder = sdk.getInsertBuilder(db_name, insert_placeholder)
    row_builder.Init(5)
    row_builder.AppendString("world")
    row_builder.AppendInt32(123)
    row_builder.AppendFloat(2.33)
    row_builder.AppendInt64(1001)
    ok, error = sdk.executeInsert(db_name, insert_placeholder, row_builder)
    assert ok == True

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
    assert ok == True

    # select
    select = "select * from " + table_name + ";"
    ok, rs = sdk.executeQuery(db_name, select)
    assert ok == True
    assert rs.Size() == 4

    # drop not empty db
    ok, error = sdk.dropDB(db_name)
    assert ok == False

    # drop table
    ok, error = sdk.executeDDL(db_name, "drop table " + table_name + ";")
    assert ok == True

    # drop db
    ok, error = sdk.dropDB(db_name)
    assert ok == True

if __name__ == "__main__":
    test_smoke()

