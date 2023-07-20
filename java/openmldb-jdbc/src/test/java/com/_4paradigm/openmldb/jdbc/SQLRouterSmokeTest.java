/*
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

package com._4paradigm.openmldb.jdbc;

import com._4paradigm.openmldb.SQLInsertRow;
import com._4paradigm.openmldb.SQLInsertRows;
import com._4paradigm.openmldb.common.Pair;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Arrays;

public class SQLRouterSmokeTest {
    public static SqlExecutor clusterExecutor;
    public static SqlExecutor standaloneExecutor;

    static {
        try {
            SdkOption option = new SdkOption();
            option.setZkPath(TestConfig.ZK_PATH);
            option.setZkCluster(TestConfig.ZK_CLUSTER);
            option.setSessionTimeout(200000);
            clusterExecutor = new SqlClusterExecutor(option);
            java.sql.Statement state = clusterExecutor.getStatement();
            state.execute("SET @@execute_mode='online';");
            state.close();
            // create standalone router
            SdkOption standaloneOption = new SdkOption();
            standaloneOption.setHost(TestConfig.HOST);
            standaloneOption.setPort(TestConfig.PORT);
            standaloneOption.setClusterMode(false);
            standaloneOption.setSessionTimeout(20000);
            standaloneExecutor = new SqlClusterExecutor(standaloneOption);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testMoreOptions() throws Exception {
        SdkOption option = new SdkOption();
        option.setZkPath(TestConfig.ZK_PATH);
        option.setZkCluster(TestConfig.ZK_CLUSTER);
        option.setSessionTimeout(200000);
        option.setMaxSqlCacheSize(100);
        option.setZkLogLevel(2);
        SqlExecutor tmp = new SqlClusterExecutor(option);
    }

    @DataProvider(name = "executor")
    public Object[] executor() {
        return new Object[] { clusterExecutor, standaloneExecutor };
    }

    @Test(dataProvider = "executor")
    public void testSmoke(SqlExecutor router) {
        try {
            String dbname = "SQLRouterSmokeTest" + System.currentTimeMillis();
            String tableName = "tsql1010";

            // create db
            boolean ok = router.createDB(dbname);
            Assert.assertTrue(ok);
            String ddl = String.format("create table %s (col1 bigint, col2 string, index(key=col2, ts=col1));",
                    tableName);
            // create table
            ok = router.executeDDL(dbname, ddl);
            Assert.assertTrue(ok);
            NS.TableInfo info = router.getTableInfo(dbname, tableName);
            Assert.assertEquals(info.getName(), tableName);

            // insert normal (1000, 'hello')
            String insert = "insert into tsql1010 values(1000, 'hello');";
            ok = router.executeInsert(dbname, insert);
            Assert.assertTrue(ok);
            // insert placeholder (1001, 'world')
            String insertPlaceholder = "insert into tsql1010 values(?, ?);";
            PreparedStatement impl = router.getInsertPreparedStmt(dbname, insertPlaceholder);
            impl.setLong(1, 1001);
            impl.setString(2, "world");
            ok = impl.execute();
            Assert.assertTrue(ok);
            // insert placeholder batch (1002, 'hi'), (1003, 'word')
            SQLInsertRows insertRows = router.getInsertRows(dbname, insertPlaceholder);
            SQLInsertRow row1 = insertRows.NewRow();
            row1.Init(2);
            row1.AppendInt64(1002);
            row1.AppendString("hi");
            SQLInsertRow row2 = insertRows.NewRow();
            row2.Init(4);
            row2.AppendInt64(1003);
            row2.AppendString("word");
            ok = router.executeInsert(dbname, insertPlaceholder, insertRows);
            Assert.assertTrue(ok);

            // select
            String select1 = "select * from tsql1010;";
            com._4paradigm.openmldb.jdbc.SQLResultSet rs1 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router
                    .executeSQL(dbname, select1);

            Assert.assertEquals(2, rs1.GetInternalSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs1.GetInternalSchema().GetColumnType(0).toString());
            Assert.assertEquals("kTypeString", rs1.GetInternalSchema().GetColumnType(1).toString());

            List<Long> col1Insert = new ArrayList<>();
            List<String> col2Insert = new ArrayList<>();
            while (rs1.next()) {
                col1Insert.add(rs1.getLong(1));
                col2Insert.add(rs1.getString(2));
            }
            Collections.sort(col1Insert);
            Collections.sort(col2Insert);

            Assert.assertEquals(col1Insert,
                    Arrays.asList(Long.valueOf(1000), Long.valueOf(1001), Long.valueOf(1002), Long.valueOf(1003)));
            Assert.assertEquals(col2Insert, Arrays.asList("hello", "hi", "word", "world"));
            rs1.close();

            String select2 = "select col1 from tsql1010;";
            com._4paradigm.openmldb.jdbc.SQLResultSet rs2 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router
                    .executeSQL(dbname, select2);
            Assert.assertEquals(1, rs2.GetInternalSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs2.GetInternalSchema().GetColumnType(0).toString());

            List<Long> col1InsertRes = new ArrayList<>();
            while (rs2.next()) {
                col1InsertRes.add(rs2.getLong(1));
            }
            Collections.sort(col1InsertRes);
            Assert.assertEquals(col1InsertRes,
                    Arrays.asList(Long.valueOf(1000), Long.valueOf(1001), Long.valueOf(1002), Long.valueOf(1003)));
            rs2.close();

            String select3 = "select col2 from tsql1010;";
            com._4paradigm.openmldb.jdbc.SQLResultSet rs3 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router
                    .executeSQL(dbname, select3);
            Assert.assertEquals(1, rs3.GetInternalSchema().GetColumnCnt());
            Assert.assertEquals("kTypeString", rs3.GetInternalSchema().GetColumnType(0).toString());

            List<String> col2InsertRes = new ArrayList<>();
            while (rs3.next()) {
                col2InsertRes.add(rs3.getString(1));
            }
            Collections.sort(col2InsertRes);
            Assert.assertEquals(col2InsertRes, Arrays.asList("hello", "hi", "word", "world"));
            rs3.close();

            // parameterized query
            String parameterizedQuerySql = "select col1, col2 from tsql1010 where col2 = ? and col1 < ?;";
            PreparedStatement query_statement = router.getPreparedStatement(dbname, parameterizedQuerySql);
            // col2 = "hi" and col1 < 1003
            {
                query_statement.setString(1, "hi");
                query_statement.setLong(2, 1003);
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement
                        .executeQuery();
                Assert.assertEquals(2, rs4.GetInternalSchema().GetColumnCnt());
                Assert.assertEquals("kTypeInt64", rs4.GetInternalSchema().GetColumnType(0).toString());
                Assert.assertEquals("kTypeString", rs4.GetInternalSchema().GetColumnType(1).toString());
                Assert.assertTrue(rs4.next());
                Assert.assertEquals(1002, rs4.getLong(1));
                Assert.assertEquals("hi", rs4.getString(2));
                Assert.assertFalse(rs4.next());
                rs4.close();
            }
            // col2 = "hi" and col1 < 1002
            {
                query_statement.setString(1, "hi");
                query_statement.setLong(2, 1002);
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement
                        .executeQuery();
                Assert.assertEquals(2, rs4.GetInternalSchema().GetColumnCnt());
                Assert.assertEquals("kTypeInt64", rs4.GetInternalSchema().GetColumnType(0).toString());
                Assert.assertEquals("kTypeString", rs4.GetInternalSchema().GetColumnType(1).toString());
                Assert.assertFalse(rs4.next());
                rs4.close();
            }
            // col2 = "world" and col1 < 1003
            {
                query_statement.setString(1, "world");
                query_statement.setLong(2, 1003);
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement
                        .executeQuery();
                Assert.assertEquals(2, rs4.GetInternalSchema().GetColumnCnt());
                Assert.assertEquals("kTypeInt64", rs4.GetInternalSchema().GetColumnType(0).toString());
                Assert.assertEquals("kTypeString", rs4.GetInternalSchema().GetColumnType(1).toString());
                Assert.assertTrue(rs4.next());
                Assert.assertEquals(1001, rs4.getLong(1));
                Assert.assertEquals("world", rs4.getString(2));
                Assert.assertFalse(rs4.next());
                rs4.close();
            }
            // col2 = "hello" and col1 < 1003
            {
                query_statement.setString(1, "hello");
                query_statement.setLong(2, 1003);
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement
                        .executeQuery();
                Assert.assertEquals(2, rs4.GetInternalSchema().GetColumnCnt());
                Assert.assertEquals("kTypeInt64", rs4.GetInternalSchema().GetColumnType(0).toString());
                Assert.assertEquals("kTypeString", rs4.GetInternalSchema().GetColumnType(1).toString());
                Assert.assertTrue(rs4.next());
                Assert.assertEquals(1000, rs4.getLong(1));
                Assert.assertEquals("hello", rs4.getString(2));
                Assert.assertFalse(rs4.next());
                rs4.close();
            }
            // col2 = "word" and col1 < 1003
            {
                query_statement.setString(1, "word");
                query_statement.setLong(2, 1003);
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement
                        .executeQuery();
                Assert.assertEquals(2, rs4.GetInternalSchema().GetColumnCnt());
                Assert.assertEquals("kTypeInt64", rs4.GetInternalSchema().GetColumnType(0).toString());
                Assert.assertEquals("kTypeString", rs4.GetInternalSchema().GetColumnType(1).toString());
                Assert.assertFalse(rs4.next());
                rs4.close();
            }
            // drop table
            String drop = "drop table tsql1010;";
            ok = router.executeDDL(dbname, drop);
            Assert.assertTrue(ok);
            // insert into deleted table, can't get insert row
            SQLInsertRow insertRow = router.getInsertRow(dbname, insertPlaceholder);
            Assert.assertNull(insertRow);
            // drop database
            ok = router.dropDB(dbname);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test(dataProvider = "executor")
    public void testCreateFunction(SqlExecutor router) {
        java.sql.Statement statement = router.getStatement();

        // create function ok
        Assert.assertTrue(statement.execute("CREATE FUNCTION cut2(x STRING) RETURNS STRING OPTIONS (FILE='/tmp/libtest_udf.so')"));
        Assert.assertTrue(statement.execute("SHOW FUNCTIONS"));

        // queryable
        Assert.assertTrue(statement.execute("set @@execute_mode='online'"));
        Assert.assertTrue(statement.execute("select cut2('hello')"));
        java.sql.ResultSet resultset = statement.getResultSet();
        resultset.next();
        String result = resultset.getString(1);
        Assert.assertEqualsDeep(result, "he");

        // dropable
        Assert.assertTrue(statement.execute("DROP FUNCTION cut2"));
    }

    @Test(dataProvider = "executor")
    public void testParameterizedQueryFail(SqlExecutor router) {
        try {
            String dbname = "SQLRouterSmokeTest" + System.currentTimeMillis();
            // create db
            router.dropDB(dbname);
            boolean ok = router.createDB(dbname);
            Assert.assertTrue(ok);
            String ddl = "create table tsql1010 ( col1 bigint, col2 string, index(key=col2, ts=col1));";
            // create table
            ok = router.executeDDL(dbname, ddl);
            Assert.assertTrue(ok);
            // parameterized query
            String parameterizedQuerySql = "select col1, col2 from tsql1010 where col2 = ? and col1 < ?;";
            PreparedStatement query_statement = router.getPreparedStatement(dbname, parameterizedQuerySql);
            // missing 2nd parameter
            {
                query_statement.setString(1, "hi");
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement
                        .executeQuery();
                Assert.fail("executeQuery is expected to throw exception");
                rs4.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(dataProvider = "executor")
    public void testInsertMeta(SqlExecutor router) {
        String dbname = "SQLRouterSmokeTest" + System.currentTimeMillis();
        // create db
        router.dropDB(dbname);
        boolean ok = router.createDB(dbname);
        Assert.assertTrue(ok);
        String ddl = "create table tsql1010 ( col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1));";
        // create table
        ok = router.executeDDL(dbname, ddl);
        Assert.assertTrue(ok);
        java.sql.Date d1 = new java.sql.Date(2019 - 1900, 1 - 1, 1);
        java.sql.Date d2 = new java.sql.Date(2019 - 1900, 2 - 1, 20);
        java.sql.Date d3 = new java.sql.Date(2019 - 1900, 3 - 1, 3);
        java.sql.Date d4 = new java.sql.Date(2019 - 1900, 4 - 1, 24);
        java.sql.Date d5 = new java.sql.Date(2019 - 1900, 5 - 1, 5);
        String date1 = String.format("%s-%02d-%02d", d1.getYear() + 1900, d1.getMonth() + 1, d1.getDate());
        String fullInsert = String.format("insert into tsql1010 values(1000, '%s', 'guangdong', '广州', 1);", date1);
        try {

            PreparedStatement ps = router.getInsertPreparedStmt(dbname, fullInsert);
            Assert.assertEquals(ps.getMetaData().getColumnCount(), 0);
            ps.close();
        } catch (Exception e) {
            Assert.fail();
        }

        String insert1 = "insert into tsql1010 values(?, '2019-01-12', 'xx', 'xx', 1);";
        try {
            PreparedStatement ps = router.getInsertPreparedStmt(dbname, insert1);
            Assert.assertEquals(ps.getMetaData().getColumnCount(), 1);
            String col = ps.getMetaData().getColumnName(1);
            Assert.assertEquals(col, "col1");
            int type = ps.getMetaData().getColumnType(1);
            Assert.assertEquals(Types.BIGINT, type);
        } catch (Exception e) {
            Assert.fail();
        }

    }

    @Test(dataProvider = "executor")
    public void testInsertPreparedState(SqlExecutor router) {
        try {
            String dbname = "SQLRouterSmokeTest" + System.currentTimeMillis();
            // create db
            router.dropDB(dbname);
            boolean ok = router.createDB(dbname);
            Assert.assertTrue(ok);
            String ddl = "create table tsql1010 ( col1 bigint, col2 date, col3 string, col4 string, col5 int," +
                    " index(key=col3, ts=col1));";
            // create table
            ok = router.executeDDL(dbname, ddl);
            Assert.assertTrue(ok);
            // insert normal
            java.sql.Date d1 = new java.sql.Date(2019 - 1900, 1 - 1, 1);
            java.sql.Date d2 = new java.sql.Date(2019 - 1900, 2 - 1, 2);
            java.sql.Date d3 = new java.sql.Date(2019 - 1900, 3 - 1, 3);
            java.sql.Date d4 = new java.sql.Date(2019 - 1900, 4 - 1, 4);
            java.sql.Date d5 = new java.sql.Date(2019 - 1900, 5 - 1, 5);
            String date1 = String.format("%s-%02d-%02d", d1.getYear() + 1900, d1.getMonth() + 1, d1.getDate());
            String fullInsert = String.format("insert into tsql1010 values(1000, '%s', 'guangdong', '广州', 1);", date1);
            ok = router.executeInsert(dbname, fullInsert);
            Assert.assertTrue(ok);
            Object[][] datas = new Object[][] {
                    { 1000L, d1, "guangdong", "广州", 1 },
                    { 1001L, d2, "jiangsu", "nanjing", 2 },
                    { 1002L, d3, "sandong", "jinan", 3 },
                    { 1003L, d4, "zhejiang", "hangzhou", 4 },
                    { 1004L, d5, "henan", "zhenzhou", 5 },
            };
            // insert placeholder
            String date2 = String.format("%s-%s-%s", d2.getYear() + 1900, d2.getMonth() + 1, d2.getDate());
            String insert = String.format("insert into tsql1010 values(?, '%s', 'jiangsu', 'nanjing', 2);", date2);
            PreparedStatement impl = router.getInsertPreparedStmt(dbname, insert);
            impl.setLong(1, 1001);
            try {
                impl.setInt(2, 1002);
            } catch (Exception e) {
                Assert.assertEquals("out of data range", e.getMessage());
            }
            ok = impl.execute();
            Assert.assertTrue(ok);

            // custom insert order
            insert = "insert into tsql1010 (col1, col3, col2, col4, col5) values (1002, ?, ?, 'jinan', 3);";
            PreparedStatement impl2 = router.getInsertPreparedStmt(dbname, insert);
            ResultSetMetaData metaData = impl2.getMetaData();
            Assert.assertEquals(metaData.getColumnCount(), 2);
            Assert.assertEquals(metaData.getColumnName(1), "col3");
            Assert.assertEquals(metaData.getColumnType(1), Types.VARCHAR);
            Assert.assertEquals(metaData.getColumnTypeName(1), "string");
            try {
                impl2.setString(2, "c");
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage().contains("data type not match"));
            }
            impl2.setString(1, "sandong");
            impl2.setDate(2, d3);
            ok = impl2.execute();
            Assert.assertTrue(ok);

            insert = "insert into tsql1010 values(?, ?, ?, ?, ?);";
            PreparedStatement impl3 = router.getInsertPreparedStmt(dbname, insert);
            impl3.setLong(1, 1003);
            impl3.setString(3, "zhejiangxx");
            impl3.setString(3, "zhejiang");
            impl3.setString(4, "xxhangzhou");
            impl3.setString(4, "hangzhou");
            impl3.setDate(2, d4);
            impl3.setInt(5, 4);
            impl3.closeOnCompletion();
            Assert.assertTrue(impl3.isCloseOnCompletion());
            ok = impl3.execute();
            Assert.assertTrue(ok);
            try {
                impl3.execute();
            } catch (Exception e) {
                Assert.assertEquals("InsertPreparedStatement closed", e.getMessage());
            }
            insert = "insert into tsql1010 values(?, ?, ?, 'zhenzhou', 5);";
            PreparedStatement impl4 = router.getInsertPreparedStmt(dbname, insert);
            impl4.close();
            Assert.assertTrue(impl4.isClosed());
            PreparedStatement impl5 = router.getInsertPreparedStmt(dbname, insert);
            impl5.setLong(1, 1004);
            impl5.setDate(2, d5);
            impl5.setString(3, "henan");
            ok = impl5.execute();
            Assert.assertTrue(ok);
            // select
            String select1 = "select * from tsql1010;";
            com._4paradigm.openmldb.jdbc.SQLResultSet rs1 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router
                    .executeSQL(dbname, select1);
            Assert.assertEquals(5, rs1.GetInternalSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs1.GetInternalSchema().GetColumnType(0).toString());
            Assert.assertEquals("kTypeDate", rs1.GetInternalSchema().GetColumnType(1).toString());
            Assert.assertEquals("kTypeString", rs1.GetInternalSchema().GetColumnType(2).toString());
            Assert.assertEquals("kTypeString", rs1.GetInternalSchema().GetColumnType(3).toString());
            Assert.assertEquals("kTypeInt32", rs1.GetInternalSchema().GetColumnType(4).toString());
            while (rs1.next()) {
                int idx = rs1.getInt(5);
                int suffix = idx - 1;
                Assert.assertTrue(suffix < datas.length);
                Assert.assertEquals(datas[suffix][0], rs1.getLong(1));
                Assert.assertEquals(datas[suffix][4], idx);
                Assert.assertEquals(datas[suffix][2], rs1.getString(3));
                Assert.assertEquals(datas[suffix][3], rs1.getString(4));
                java.sql.Date tmpDate = (java.sql.Date) datas[suffix][1];
                java.sql.Date getData = rs1.getDate(2);
                Assert.assertEquals(tmpDate.getYear(), getData.getYear());
                Assert.assertEquals(tmpDate.getDate(), getData.getDate());
                Assert.assertEquals(tmpDate.getMonth(), getData.getMonth());
            }
            rs1.close();

            String select2 = "select col1 from tsql1010;";
            com._4paradigm.openmldb.jdbc.SQLResultSet rs2 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router
                    .executeSQL(dbname, select2);
            Assert.assertEquals(1, rs2.GetInternalSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs2.GetInternalSchema().GetColumnType(0).toString());
            rs2.close();
            // drop table
            String drop = "drop table tsql1010;";
            ok = router.executeDDL(dbname, drop);
            Assert.assertTrue(ok);
            // insert into deleted table
            ok = router.executeInsert(dbname, fullInsert);
            Assert.assertFalse(ok);
            // drop database
            ok = router.dropDB(dbname);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test(dataProvider = "executor")
    public void testInsertPreparedStateBatch(SqlExecutor router) {
        Object[][] batchData = new Object[][] {
                {
                        "insert into tsql1010 values(?, ?, 'zhao', 1.0, null, 'z');",
                        new Object[][] {
                                { 1000l, 1l }, { 1001l, 2l }, { 1002l, 3l }, { 1003l, 4l }, }
                },
                {
                        "insert into tsql1010 values(?, ?, 'zhao', 1.0, null, 'z');",
                        new Object[][] {
                                { 1004l, 5l }, { 1005l, 6l }, { 1006l, 7l }, { 1007l, 8l }, }
                },
                {
                        "insert into tsql1010 values(?, ?, ?, 2.0, null, ?);",
                        "insert into tsql1010 values(1008, 9, 'zhao', 2.0, null, 'z');",
                        "insert into tsql1010 values(1009, 10, 'zhao', 2.0, null, 'z');",
                        "insert into tsql1010 values(1010, 11, 'zhao', 2.0, null, 'z');",
                }
        };
        try {
            String dbname = "SQLRouterSmokeTest" + System.currentTimeMillis();
            // create db
            router.dropDB(dbname);
            boolean ok = router.createDB(dbname);
            Assert.assertTrue(ok);
            String ddl = "create table tsql1010 ( col1 bigint, col2 bigint, col3 string, col4 float, col5 date, col6 string, index(key=col2, ts=col1));";
            // create table
            ok = router.executeDDL(dbname, ddl);
            Assert.assertTrue(ok);
            // insert normal
            int i = 0;
            String insertPlaceholder = "insert into tsql1010 values(?, 2, 'taiyuan', 2.0);";
            PreparedStatement impl = router.getInsertPreparedStmt(dbname, (String) batchData[i][0]);
            Object[][] datas1 = (Object[][]) batchData[i][1];
            for (int j = 0; j < datas1.length; j++) {
                try {
                    impl.setInt(2, 1002);
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("data type not match"));
                }
                try {
                    // set failed, so the row is uncompleted, appending row will be failed
                    impl.execute();
                } catch (Exception e) {
                    if (j > 0) {
                        // j > 0, addBatch has been called
                        Assert.assertEquals(e.getMessage(), "please use executeBatch");
                    } else {
                        Assert.assertTrue(e.getMessage().contains("append failed"));
                    }
                }
                impl.setLong(1, (Long) datas1[j][0]);
                impl.setLong(2, (Long) datas1[j][1]);
                impl.addBatch();
            }
            try {
                ok = impl.execute();
            } catch (Exception e) {
                Assert.assertEquals(e.getMessage(), "please use executeBatch");
            }
            impl.executeBatch();
            Assert.assertTrue(ok);
            String select1 = "select * from tsql1010;";
            com._4paradigm.openmldb.jdbc.SQLResultSet rs1 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router
                    .executeSQL(dbname, select1);
            Assert.assertEquals(6, rs1.GetInternalSchema().GetColumnCnt());
            rs1.close();
            i++;
            PreparedStatement impl2 = router.getInsertPreparedStmt(dbname, (String) batchData[i][0]);
            datas1 = (Object[][]) batchData[i][1];
            // value setting error won't break anything
            for (int j = 0; j < datas1.length; j++) {
                try {
                    impl2.setInt(2, 1002);
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("data type not match"));
                }
                try {
                    impl2.execute();
                } catch (Exception e) {
                    if (j > 0) {
                        Assert.assertEquals(e.getMessage(), "please use executeBatch");
                    } else {
                        Assert.assertTrue(e.getMessage().contains("append failed"));
                    }
                }
                impl2.setLong(1, (Long) datas1[j][0]);
                impl2.setLong(2, (Long) datas1[j][1]);
                impl2.addBatch();
            }

            try {
                ok = impl2.execute();
            } catch (Exception e) {
                Assert.assertEquals(e.getMessage(), "please use executeBatch");
            }
            i++;
            // can't use addBatch(String sql)
            Object[] datas2 = batchData[i];
            try {
                impl2.addBatch((String) datas2[0]);
            } catch (Exception e) {
                Assert.assertEquals(e.getMessage(), "cannot take arguments in PreparedStatement");
            }

            int[] result = impl.executeBatch();
            int[] expected = new int[result.length];
            Assert.assertEquals(result, expected);

            String select2 = "select * from tsql1010;";
            com._4paradigm.openmldb.jdbc.SQLResultSet rs2 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router
                    .executeSQL(dbname, select1);
            Assert.assertEquals(6, rs2.GetInternalSchema().GetColumnCnt());
            int recordCnt = 0;
            while (rs2.next()) {
                recordCnt++;
            }
            Assert.assertEquals(datas1.length, recordCnt);
            rs2.close();
            // drop table
            String drop = "drop table tsql1010;";
            ok = router.executeDDL(dbname, drop);
            Assert.assertTrue(ok);
            // insert into deleted table
            ok = router.executeInsert(dbname, "insert into tsql1010 values(1009, 10, 'zhao', 2.0, null, 'z')");
            Assert.assertFalse(ok);
            // drop database
            ok = router.dropDB(dbname);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testDDLParseMethods() throws SQLException {
        Map<String, Map<String, Schema>> schemaMaps = new HashMap<>();
        Schema sch = new Schema(Collections.singletonList(new Column("c1", Types.VARCHAR)));
        Map<String, Schema> dbSchema = new HashMap<>();
        dbSchema.put("t1", sch);
        schemaMaps.put("db1", dbSchema);
        List<String> ddls = SqlClusterExecutor.genDDL("select c1 from t1;", schemaMaps);
        Assert.assertEquals(ddls.size(), 1);
        List<Column> schema = SqlClusterExecutor.genOutputSchema("select c1 from t1;", schemaMaps).getColumnList();
        Assert.assertEquals(schema.size(), 1);
        Assert.assertEquals(schema.get(0).getColumnName(), "c1");
        Assert.assertEquals(schema.get(0).getSqlType(), Types.VARCHAR);

        // if input schema is null or empty
        try {
            SqlClusterExecutor.genDDL("", null);
            Assert.fail("null input schema will throw an exception");
        } catch (SQLException ignored) {
        }
        try {
            SqlClusterExecutor.genOutputSchema("", null);
            Assert.fail("null input schema will throw an exception");
        } catch (NullPointerException ignored) {
        }
        try {
            SqlClusterExecutor.genDDL("", Maps.<String, Map<String, Schema>>newHashMap());
            Assert.fail("null input schema will throw an exception");
        } catch (SQLException ignored) {
        }
        try {
            SqlClusterExecutor.genOutputSchema("", Maps.<String, Map<String, Schema>>newHashMap());
            Assert.fail("empty input schema will throw an exception");
        } catch (NoSuchElementException ignored) {
        }

        // if parse fails, genDDL will create table without index
        List<String> res1 = SqlClusterExecutor.genDDL("select not_exist from t1;", schemaMaps);
        Assert.assertEquals(res1.size(), 1);
        Assert.assertFalse(res1.get(0).contains("index"));
        // if parse fails, the output schema result is empty, can't convert to
        // sdk.Schema
        try {
            SqlClusterExecutor.genOutputSchema("select not_exist from t1;", schemaMaps);
        } catch (SQLException ignored) {
        }

        // multi db genOutputSchema
        schemaMaps.put("db2", dbSchema);
        schema = SqlClusterExecutor
                .genOutputSchema("select t11.c1 from db1.t1 t1 last join db2.t1 t11 on t1.c1==t11.c1;", schemaMaps)
                .getColumnList();
        Assert.assertEquals(schema.size(), 1);

        // get dependence tables
        List<Pair<String, String>> tables = SqlClusterExecutor.getDependentTables(
                "select t11.c1 from db1.t1 t1 last join db2.t1 t11 on t1.c1==t11.c1;", "",
                schemaMaps);
        Assert.assertEquals(tables.size(), 2);
    }

    @Test
    public void testValidateSQL() throws SQLException {
        Map<String, Map<String, Schema>> schemaMaps = new HashMap<>();
        Schema sch = new Schema(Collections.singletonList(new Column("c1", Types.VARCHAR)));
        Map<String, Schema> dbSchema = new HashMap<>();
        dbSchema.put("t1", sch);
        schemaMaps.put("db1", dbSchema);
        dbSchema = new HashMap<>();
        dbSchema.put("t2", sch);
        // one more db, if no used db, will use the first one db1
        schemaMaps.put("db2", dbSchema);

        List<String> ret = SqlClusterExecutor.validateSQLInBatch("select c1 from t1;", schemaMaps);
        Assert.assertEquals(ret.size(), 0);
        // t2 is in db2, db1 is the used db
        ret = SqlClusterExecutor.validateSQLInBatch("select c1 from t2;", schemaMaps);
        Assert.assertEquals(ret.size(), 2);
        // used db won't effect <db>.<table> style
        ret = SqlClusterExecutor.validateSQLInBatch("select c1 from db1.t1;", schemaMaps);
        Assert.assertEquals(ret.size(), 0);

        ret = SqlClusterExecutor.validateSQLInBatch("swlect c1 from t1;", schemaMaps);
        Assert.assertEquals(ret.size(), 2);
        Assert.assertTrue(ret.get(0).contains("Syntax error"));

        ret = SqlClusterExecutor.validateSQLInBatch("select foo(c1) from t1;", schemaMaps);
        Assert.assertEquals(ret.size(), 2);
        Assert.assertTrue(ret.get(0).contains("Fail to resolve expression"));

        // if has the same name tables, the first one will be used
        schemaMaps = new HashMap<>();
        Schema sch2 = new Schema(Collections.singletonList(new Column("c2", Types.VARCHAR)));
        dbSchema = new HashMap<>();
        dbSchema.put("t1", sch);
        schemaMaps.put("db1", dbSchema);
        dbSchema = new HashMap<>();
        dbSchema.put("t1", sch2);
        schemaMaps.put("db2", dbSchema);

        ret = SqlClusterExecutor.validateSQLInBatch("select c1 from t1;", schemaMaps);
        Assert.assertEquals(ret.size(), 0);
        // t1 is db1.t1
        ret = SqlClusterExecutor.validateSQLInBatch("select c2 from t1;", schemaMaps);
        Assert.assertEquals(ret.size(), 2);

        // if input schema is null or empty
        try {
            SqlClusterExecutor.validateSQLInBatch("", null);
            Assert.fail("null input schema will throw an exception");
        } catch (NullPointerException ignored) {
        }

        try {
            SqlClusterExecutor.validateSQLInBatch("", Maps.<String, Map<String, Schema>>newHashMap());
            Assert.fail("null input schema will throw an exception");
        } catch (NoSuchElementException ignored) {
        }

        ret = SqlClusterExecutor.validateSQLInRequest("select count(c1) from t1;", schemaMaps);
        Assert.assertEquals(ret.size(), 2);
        Assert.assertTrue(ret.get(0).contains("Aggregate over a table cannot be supported in online serving"));
        dbSchema = new HashMap<>();
        dbSchema.put("t3", new Schema(Arrays.asList(new Column("c1", Types.VARCHAR),
                new Column("c2", Types.BIGINT))));
        schemaMaps.put("db3", dbSchema);
        // t3 is in db3, <db>.<table> style is ok
        ret = SqlClusterExecutor.validateSQLInRequest("select count(c1) over w1 from db3.t3 window " +
                "w1 as(partition by c1 order by c2 rows between unbounded preceding and current row);", schemaMaps);
        Assert.assertEquals(ret.size(), 0);
    }

    @Test
    public void testMergeSQL() throws SQLException {
        // full table pattern
        List<String> sqls = Arrays.asList(
                // some features in current row
                "select c1 from main",
                // window
                "select sum(c1) over w1 of2 from main window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row);",
                // join
                "select t1.c2 of3 from main last join t1 on main.c1==t1.c1;",
                // join in order
                "select t1.c2 of4 from main last join t1 order by t1.c2 on main.c1==t1.c1;",
                // window union
                "select sum(c2) over w1 from main window w1 as (union (select \"\" as id, * from t1) partition by c1 order by c2 rows between unbounded preceding and current row)");

        // validate merged sql
        Map<String, Map<String, Schema>> schemaMaps = new HashMap<>();
        HashMap<String, Schema> dbSchema = new HashMap<>();
        dbSchema.put("main", new Schema(Arrays.asList(new Column("id", Types.VARCHAR), new Column("c1", Types.BIGINT),
                new Column("c2", Types.BIGINT))));
        dbSchema.put("t1", new Schema(Arrays.asList(new Column("c1", Types.BIGINT), new Column("c2", Types.BIGINT))));
        schemaMaps.put("foo", dbSchema);

        String filtered = SqlClusterExecutor.mergeSQL(sqls, "foo", Arrays.asList("id"), schemaMaps);
        Assert.assertEquals(filtered,
                "select `c1`, `of2`, `of3`, `of4`, `sum(c2)over w1` from (select foo.main.id as merge_id_0, c1 from main) as out0 "
                        + "last join "
                        + "(select foo.main.id as merge_id_1, sum(c1) over w1 of2 from main window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row)) as out1 on out0.merge_id_0 = out1.merge_id_1 "
                        + "last join "
                        + "(select foo.main.id as merge_id_2, t1.c2 of3 from main last join t1 on main.c1==t1.c1) as out2 "
                        + "on out0.merge_id_0 = out2.merge_id_2" + " last join "
                        + "(select foo.main.id as merge_id_3, t1.c2 of4 from main last join t1 order by t1.c2 on main.c1==t1.c1) as out3 "
                        + "on out0.merge_id_0 = out3.merge_id_3 " + "last join"
                        + " (select foo.main.id as merge_id_4, sum(c2) over w1 from main window w1 as (union (select \"\" as id, * from t1) partition by c1 order by c2 rows between unbounded preceding and current row)) as out4 "
                        + "on out0.merge_id_0 = out4.merge_id_4;");

        // add a function col without rename
        List<String> sqls2 = new ArrayList<>(sqls);
        sqls2.add("select int(`c1`) from main");
        String merged1 = SqlClusterExecutor.mergeSQL(sqls2, "foo", Arrays.asList("id"), schemaMaps);
        Assert.assertFalse(merged1.startsWith("select * from "));
        // add a ambiguous col-int(c1), throw exception
        try {
            sqls2.add("select int(`c1`) from main");
            SqlClusterExecutor.mergeSQL(sqls2, "foo", Arrays.asList("id"), schemaMaps);
            Assert.fail("ambiguous col should throw exception");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("ambiguous"));
        }

        // join keys
        String mergedKeys = SqlClusterExecutor.mergeSQL(sqls, "foo", Arrays.asList("id", "c1", "c2"), schemaMaps);
        Assert.assertEquals(mergedKeys,
                "select `c1`, `of2`, `of3`, `of4`, `sum(c2)over w1` from "
                        + "(select foo.main.id as merge_id_0, foo.main.c1 as merge_c1_0, foo.main.c2 as merge_c2_0, c1 from main) as out0 "
                        + "last join "
                        + "(select foo.main.id as merge_id_1, foo.main.c1 as merge_c1_1, foo.main.c2 as merge_c2_1, sum(c1) over w1 of2 from main window w1 as "
                        + "(partition by c1 order by c2 rows between unbounded preceding and current row)) as out1 "
                        + "on out0.merge_id_0 = out1.merge_id_1 and out0.merge_c1_0 = out1.merge_c1_1 and out0.merge_c2_0 = out1.merge_c2_1 "
                        + "last join "
                        + "(select foo.main.id as merge_id_2, foo.main.c1 as merge_c1_2, foo.main.c2 as merge_c2_2, t1.c2 of3 from main last join t1 on main.c1==t1.c1) as out2 "
                        + "on out0.merge_id_0 = out2.merge_id_2 and out0.merge_c1_0 = out2.merge_c1_2 and out0.merge_c2_0 = out2.merge_c2_2 "
                        + "last join "
                        + "(select foo.main.id as merge_id_3, foo.main.c1 as merge_c1_3, foo.main.c2 as merge_c2_3, t1.c2 of4 from main last join t1 order by t1.c2 on main.c1==t1.c1) as out3 "
                        + "on out0.merge_id_0 = out3.merge_id_3 and out0.merge_c1_0 = out3.merge_c1_3 and out0.merge_c2_0 = out3.merge_c2_3 "
                        + "last join "
                        + "(select foo.main.id as merge_id_4, foo.main.c1 as merge_c1_4, foo.main.c2 as merge_c2_4, sum(c2) over w1 from main "
                        + "window w1 as (union (select \"\" as id, * from t1) partition by c1 order by c2 rows between unbounded preceding and current row)) as out4 "
                        + "on out0.merge_id_0 = out4.merge_id_4 and out0.merge_c1_0 = out4.merge_c1_4 and out0.merge_c2_0 = out4.merge_c2_4;");

        // main table patterns
        sqls = Arrays.asList(
                "select c1 from main",
                "select c2 from foo.main",
                "select bar.id from main as bar");
        String mainRenamed = SqlClusterExecutor.mergeSQL(sqls, "foo", Arrays.asList("id", "c1", "c2"), schemaMaps);
        Assert.assertEquals(mainRenamed,
                "select `c1`, `c2`, `id` from (select foo.main.id as merge_id_0, foo.main.c1 as merge_c1_0, foo.main.c2 as merge_c2_0, c1 from main) as out0 "
                        + "last join "
                        + "(select foo.main.id as merge_id_1, foo.main.c1 as merge_c1_1, foo.main.c2 as merge_c2_1, c2 from foo.main) as out1 "
                        + "on out0.merge_id_0 = out1.merge_id_1 and out0.merge_c1_0 = out1.merge_c1_1 and out0.merge_c2_0 = out1.merge_c2_1 "
                        + "last join "
                        + "(select foo.main.id as merge_id_2, foo.main.c1 as merge_c1_2, foo.main.c2 as merge_c2_2, bar.id from main as bar) as out2 "
                        + "on out0.merge_id_0 = out2.merge_id_2 and out0.merge_c1_0 = out2.merge_c1_2 and out0.merge_c2_0 = out2.merge_c2_2;");
        // add one more db aaa, mergeSQL won't use the unrelated db
        schemaMaps.put("aaa", new HashMap<String, Schema>());
        String twoDB = SqlClusterExecutor.mergeSQL(sqls, "foo", Arrays.asList("id", "c1", "c2"), schemaMaps);

        // no used db, all tables are <db>.<table>
        sqls = Arrays.asList("select c1 from foo.main",
                "select t1.c2 from foo.main as main last join foo.t1 as t1 on main.c1==t1.c1",
                "select id from foo.main");
        String noUsedDB = SqlClusterExecutor.mergeSQL(sqls, "", Arrays.asList("id", "c1", "c2"), schemaMaps);
        Assert.assertEquals(noUsedDB,
                "select `c1`, `c2`, `id` from "
                        + "(select foo.main.id as merge_id_0, foo.main.c1 as merge_c1_0, foo.main.c2 as merge_c2_0, c1 from foo.main) as out0 "
                        + "last join "
                        + "(select foo.main.id as merge_id_1, foo.main.c1 as merge_c1_1, foo.main.c2 as merge_c2_1, t1.c2 from foo.main as main "
                        + "last join foo.t1 as t1 on main.c1==t1.c1) as out1 "
                        + "on out0.merge_id_0 = out1.merge_id_1 and out0.merge_c1_0 = out1.merge_c1_1 and out0.merge_c2_0 = out1.merge_c2_1 "
                        + "last join "
                        + "(select foo.main.id as merge_id_2, foo.main.c1 as merge_c1_2, foo.main.c2 as merge_c2_2, id from foo.main) as out2 "
                        + "on out0.merge_id_0 = out2.merge_id_2 and out0.merge_c1_0 = out2.merge_c1_2 and out0.merge_c2_0 = out2.merge_c2_2;");


        // case in java quickstart
        String demoResult = SqlClusterExecutor.mergeSQL(Arrays.asList(
                // 单表直出特征
                "select c1 from main;",
                // 单表聚合特征
                "select sum(c1) over w1 of2 from main window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row);",
                // 多表特征
                "select t1.c2 of4 from main last join t1 order by t1.c2 on main.c1==t1.c1;",
                // 多表聚合特征
                "select sum(c2) over w1 from main window w1 as (union (select \"\" as id, * from t1) partition by c1 order by c2 rows between unbounded preceding and current row);"),
                "db", Arrays.asList("id", "c1"), Collections.singletonMap("db", dbSchema));
        Assert.assertEquals(demoResult, "select `c1`, `of2`, `of4`, `sum(c2)over w1` from "
                + "(select db.main.id as merge_id_0, db.main.c1 as merge_c1_0, c1 from main) as out0 " + "last join "
                + "(select db.main.id as merge_id_1, db.main.c1 as merge_c1_1, sum(c1) over w1 of2 from main window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row)) as out1 "
                + "on out0.merge_id_0 = out1.merge_id_1 and out0.merge_c1_0 = out1.merge_c1_1 " + "last join "
                + "(select db.main.id as merge_id_2, db.main.c1 as merge_c1_2, t1.c2 of4 from main last join t1 order by t1.c2 on main.c1==t1.c1) as out2 "
                + "on out0.merge_id_0 = out2.merge_id_2 and out0.merge_c1_0 = out2.merge_c1_2 last join "
                + "(select db.main.id as merge_id_3, db.main.c1 as merge_c1_3, sum(c2) over w1 from main window w1 as (union (select \"\" as id, * from t1) partition by c1 order by c2 rows between unbounded preceding and current row)) as out3 "
                + "on out0.merge_id_0 = out3.merge_id_3 and out0.merge_c1_0 = out3.merge_c1_3;");
    }
}
