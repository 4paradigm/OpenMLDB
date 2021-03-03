/*
 * JDBCSmokeTest.java
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.fesql.jdbc;

import com._4paradigm.fesql.FeSqlLibrary;
import com._4paradigm.fesql.sdk.*;
import com._4paradigm.fesql_interface;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JDBCSmokeTest {
    static {
        FeSqlLibrary.initComplete();
    }

    @Test
    public void testCreateDB() {
        String dbname = "name" + System.nanoTime();
        DBMSSdk sdk = fesql_interface.CreateDBMSSdk(FESQLConfig.DBMS_ENDPOINT);
        Status status = new Status();
        sdk.CreateDatabase(dbname, status);
        Assert.assertEquals(0, status.getCode());
        sdk.CreateDatabase(dbname, status);
        Assert.assertFalse(status.getCode() == 0);
    }

    @Test
    public void testCreateTable() {
        String dbname = "name" + System.nanoTime();
        DBMSSdk sdk = fesql_interface.CreateDBMSSdk(FESQLConfig.DBMS_ENDPOINT);
        Status status = new Status();
        sdk.CreateDatabase(dbname, status);
        Assert.assertEquals(0, status.getCode());
        String createTable =  "create table t1 ( col1 bigint, col2 string, index(key=col2, ts=col1));";
        sdk.ExecuteQuery(dbname, createTable, status);
        Assert.assertEquals(0, status.getCode());
        TableSet ts = sdk.GetTables(dbname, status);
        Assert.assertEquals(1, ts.Size());
        Assert.assertTrue(ts.Next());
        Assert.assertEquals(ts.GetTable().GetName(), "t1");
    }

    @Test
    public void testInsertAndQuery() {
        String dbname = "name" + System.nanoTime();
        DBMSSdk sdk = fesql_interface.CreateDBMSSdk(FESQLConfig.DBMS_ENDPOINT);
        Status status = new Status();
        sdk.CreateDatabase(dbname, status);
        Assert.assertEquals(0, status.getCode());
        String createTable =  "create table t1 ( col1 bigint, col2 string, index(key=col2, ts=col1));";
        sdk.ExecuteQuery(dbname, createTable, status);
        String insert = "insert into t1 values(1000, 'hello');";
        sdk.ExecuteQuery(dbname, insert, status);
        Assert.assertEquals(0, status.getCode());
        String query = "select col1 + 1, col2 from t1;";
        ResultSet rs = sdk.ExecuteQuery(dbname, query, status);
        Assert.assertEquals(0, status.getCode());
        Assert.assertEquals(1, rs.Size());
        Assert.assertTrue(rs.Next());
        Assert.assertEquals(1001, rs.GetInt64Unsafe(0));
        Assert.assertEquals("hello", rs.GetStringUnsafe(1));
    }

    @Test
    public void testRequestMode() {
        String dbname = "name" + System.nanoTime();
        DBMSSdk sdk = fesql_interface.CreateDBMSSdk(FESQLConfig.DBMS_ENDPOINT);
        Status status = new Status();
        sdk.CreateDatabase(dbname, status);
        Assert.assertEquals(0, status.getCode());
        String createTable =  "create table t1 ( col1 bigint, col2 string, index(key=col2, ts=col1));";
        sdk.ExecuteQuery(dbname, createTable, status);
        Assert.assertEquals(0, status.getCode());
        String query = "select col1 + 1, col2 from t1;";
        RequestRow row = sdk.GetRequestRow(dbname, query, status);
        Assert.assertEquals(0, status.getCode());
        Assert.assertTrue(row.Init(5));
        Assert.assertTrue(row.AppendInt64(11));
        Assert.assertTrue(row.AppendString("hello"));
        Assert.assertTrue(row.Build());
        Assert.assertEquals(2, row.GetSchema().GetColumnCnt());
        ResultSet rs = sdk.ExecuteQuery(dbname, query, row, status);
        Assert.assertEquals(0, status.getCode());
        Assert.assertEquals(2,rs.GetSchema().GetColumnCnt());
        Assert.assertEquals("kTypeInt64", rs.GetSchema().GetColumnType(0).toString());
        Assert.assertEquals("kTypeString", rs.GetSchema().GetColumnType(1).toString());
        Assert.assertTrue(rs.Next());
        Assert.assertEquals("hello", rs.GetStringUnsafe(1));
        Assert.assertEquals(12, rs.GetInt64Unsafe(0));
    }
}
