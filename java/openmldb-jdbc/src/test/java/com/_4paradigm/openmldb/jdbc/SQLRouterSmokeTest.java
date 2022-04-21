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
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SQLRouterSmokeTest {

    private final Random random = new Random(System.currentTimeMillis());
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

    @DataProvider(name = "executor")
    public Object[] executor() {
        return new Object[] {clusterExecutor, standaloneExecutor};
    }

    @Test(dataProvider = "executor")
    public void testSmoke(SqlExecutor router) {
        try {
            String dbname = "db" + random.nextInt(100000);
            // create db
            router.dropDB(dbname);
            boolean ok = router.createDB(dbname);
            Assert.assertTrue(ok);
            String ddl = "create table tsql1010(col1 bigint, col2 string, index(key=col2, ts=col1));";
            // create table
            ok = router.executeDDL(dbname, ddl);
            Assert.assertTrue(ok);
            NS.TableInfo info = router.getTableInfo(dbname, "tsql1010");
            Assert.assertEquals(info.getName(), "tsql1010");

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
            com._4paradigm.openmldb.jdbc.SQLResultSet rs1 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router.executeSQL(dbname, select1);

            Assert.assertEquals(2, rs1.GetInternalSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs1.GetInternalSchema().GetColumnType(0).toString());
            Assert.assertEquals("kTypeString", rs1.GetInternalSchema().GetColumnType(1).toString());
            Assert.assertTrue(rs1.next());
            Assert.assertEquals("hello", rs1.getString(2));
            Assert.assertEquals(1000, rs1.getLong(1));

            Assert.assertTrue(rs1.next());
            Assert.assertEquals("world", rs1.getString(2));
            Assert.assertEquals(1001, rs1.getLong(1));

            Assert.assertTrue(rs1.next());
            Assert.assertEquals("hi", rs1.getString(2));
            Assert.assertEquals(1002, rs1.getLong(1));

            Assert.assertTrue(rs1.next());
            Assert.assertEquals("word", rs1.getString(2));
            Assert.assertEquals(1003, rs1.getLong(1));
            rs1.close();

            String select2 = "select col1 from tsql1010;";
            com._4paradigm.openmldb.jdbc.SQLResultSet rs2 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router.executeSQL(dbname, select2);
            Assert.assertEquals(1, rs2.GetInternalSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs2.GetInternalSchema().GetColumnType(0).toString());
            Assert.assertTrue(rs2.next());
            Assert.assertEquals(1000, rs2.getLong(1));
            Assert.assertTrue(rs2.next());
            Assert.assertEquals(1001, rs2.getLong(1));
            Assert.assertTrue(rs2.next());
            Assert.assertEquals(1002, rs2.getLong(1));
            Assert.assertTrue(rs2.next());
            Assert.assertEquals(1003, rs2.getLong(1));
            rs2.close();

            String select3 = "select col2 from tsql1010;";

            com._4paradigm.openmldb.jdbc.SQLResultSet rs3 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router.executeSQL(dbname, select3);
            Assert.assertEquals(1, rs3.GetInternalSchema().GetColumnCnt());
            Assert.assertEquals("kTypeString", rs3.GetInternalSchema().GetColumnType(0).toString());
            Assert.assertTrue(rs3.next());
            Assert.assertEquals("hello", rs3.getString(1));
            Assert.assertTrue(rs3.next());
            Assert.assertEquals("world", rs3.getString(1));
            Assert.assertTrue(rs3.next());
            Assert.assertEquals("hi", rs3.getString(1));
            Assert.assertTrue(rs3.next());
            Assert.assertEquals("word", rs3.getString(1));
            rs3.close();

            // parameterized query
            String parameterizedQuerySql = "select col1, col2 from tsql1010 where col2 = ? and col1 < ?;";
            PreparedStatement query_statement = router.getPreparedStatement(dbname, parameterizedQuerySql);
            // col2 = "hi" and col1 < 1003
            {
                query_statement.setString(1, "hi");
                query_statement.setLong(2, 1003);
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement.executeQuery();
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
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement.executeQuery();
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
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement.executeQuery();
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
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement.executeQuery();
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
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement.executeQuery();
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
    public void testParameterizedQueryFail(SqlExecutor router) {
        try {
            String dbname = "db" + random.nextInt(100000);
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
                com._4paradigm.openmldb.jdbc.SQLResultSet rs4 = (com._4paradigm.openmldb.jdbc.SQLResultSet) query_statement.executeQuery();
                Assert.fail("executeQuery is expected to throw exception");
                rs4.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(dataProvider = "executor")
    public void testInsertMeta(SqlExecutor router) {
        String dbname = "db" + random.nextInt(100000);
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
            String dbname = "db" + random.nextInt(100000);
            // create db
            router.dropDB(dbname);
            boolean ok = router.createDB(dbname);
            Assert.assertTrue(ok);
            String ddl = "create table tsql1010 ( col1 bigint, col2 date, col3 string, col4 string, col5 int, index(key=col3, ts=col1));";
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
            Object[][] datas = new Object[][]{
                    {1000l, d1, "guangdong", "广州", 1},
                    {1001l, d2, "jiangsu", "nanjing", 2},
                    {1002l, d3, "sandong", "jinan", 3},
                    {1003l, d4, "zhejiang", "hangzhou", 4},
                    {1004l, d5, "henan", "zhenzhou", 5},
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
            insert = "insert into tsql1010 values(1002, ?, ?, 'jinan', 3);";
            PreparedStatement impl2 = router.getInsertPreparedStmt(dbname, insert);
            try {
                impl2.setString(1, "c");
            } catch (Exception e) {
                Assert.assertEquals("data type not match", e.getMessage());
            }
            impl2.setDate(1, d3);
            impl2.setString(2, "sandong");
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
            com._4paradigm.openmldb.jdbc.SQLResultSet rs1 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router.executeSQL(dbname, select1);
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
            com._4paradigm.openmldb.jdbc.SQLResultSet rs2 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router.executeSQL(dbname, select2);
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
        Object[][] batchData = new Object[][]{
                {
                        "insert into tsql1010 values(?, ?, 'zhao', 1.0, null, 'z');",
                        new Object[][]{
                                {1000l, 1l}, {1001l, 2l}, {1002l, 3l}, {1003l, 4l},}
                },
                {
                        "insert into tsql1010 values(?, ?, 'zhao', 1.0, null, 'z');",
                        new Object[][]{
                                {1004l, 5l}, {1005l, 6l}, {1006l, 7l}, {1007l, 8l},}
                },
                {
                        "insert into tsql1010 values(?, ?, ?, 2.0, null, ?);",
                        "insert into tsql1010 values(1008, 9, 'zhao', 2.0, null, 'z');",
                        "insert into tsql1010 values(1009, 10, 'zhao', 2.0, null, 'z');",
                        "insert into tsql1010 values(1010, 11, 'zhao', 2.0, null, 'z');",
                }
        };
        try {
            String dbname = "db" + random.nextInt(100000);
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
                    Assert.assertEquals(e.getMessage(), "data type not match");
                }
                try {
                    // set failed, so the row is uncompleted, appending row will be failed
                    impl.execute();
                } catch (Exception e) {
                    if (j > 0) {
                        // j > 0, addBatch has been called
                        Assert.assertEquals(e.getMessage(), "please use executeBatch");
                    } else {
                        Assert.assertEquals(e.getMessage(), "append failed");
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
            com._4paradigm.openmldb.jdbc.SQLResultSet rs1 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router.executeSQL(dbname, select1);
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
                    Assert.assertEquals(e.getMessage(), "data type not match");
                }
                try {
                    impl2.execute();
                } catch (Exception e) {
                    if (j > 0) {
                        Assert.assertEquals(e.getMessage(), "please use executeBatch");
                    } else {
                        Assert.assertEquals(e.getMessage(), "append failed");
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
            com._4paradigm.openmldb.jdbc.SQLResultSet rs2 = (com._4paradigm.openmldb.jdbc.SQLResultSet) router.executeSQL(dbname, select1);
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

    @Test(dataProvider = "executor")
    public void testDDLParseMethods(SqlExecutor router) throws SQLException {
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
        } catch (SQLException ignored) {
        }
        try {
            SqlClusterExecutor.genDDL("", Maps.<String, Map<String, Schema>>newHashMap());
            Assert.fail("null input schema will throw an exception");
        } catch (SQLException ignored) {
        }
        try {
            SqlClusterExecutor.genOutputSchema("", Maps.<String, Map<String, Schema>>newHashMap());
            Assert.fail("null input schema will throw an exception");
        } catch (SQLException ignored) {
        }

        // if parse fails, genDDL will create table without index
        List<String> res1 = SqlClusterExecutor.genDDL("select not_exist from t1;", schemaMaps);
        Assert.assertEquals(res1.size(), 1);
        Assert.assertFalse(res1.get(0).contains("index"));
        // if parse fails, the output schema result is empty, can't convert to sdk.Schema
        try {
            SqlClusterExecutor.genOutputSchema("select not_exist from t1;", schemaMaps);
        } catch (SQLException ignored) {
        }
    }
}
