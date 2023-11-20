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

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import java.sql.*;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RequestPreparedStatementTest {
    private final Random random = new Random(System.currentTimeMillis());
    public static SqlExecutor executor;

    static {
        try {
            SdkOption option = new SdkOption();
            option.setZkPath(TestConfig.ZK_PATH);
            option.setZkCluster(TestConfig.ZK_CLUSTER);
            option.setSessionTimeout(200000);
            executor = new SqlClusterExecutor(option);
            java.sql.Statement state = executor.getStatement();
            state.execute("SET @@execute_mode='online';");
            state.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @DataProvider(name = "createOption")
    Object[][] getCreateParm() {
        return new Object[][] { {"NoCompress", "Memory"},
                                {"NoCompress", "HDD"},
                                {"Snappy", "Memory"},
                                {"Snappy", "HDD"} };
    }

    @Test(dataProvider = "createOption")
    public void testRequest(String compressType, String storageMode) {
        String dbname = "db" + random.nextInt(100000);
        executor.dropDB(dbname);
        boolean ok = executor.createDB(dbname);
        Assert.assertTrue(ok);
        String baseSql = "create table trans(c1 string,\n" +
                "                   c3 int,\n" +
                "                   c4 bigint,\n" +
                "                   c5 float,\n" +
                "                   c6 double,\n" +
                "                   c7 timestamp,\n" +
                "                   c8 date,\n" +
                "                   index(key=c1, ts=c7))\n ";
        String createTableSql = String.format("%s OPTIONS (compress_type='%s', storage_mode='%s');",
                baseSql, compressType, storageMode);
        executor.executeDDL(dbname, createTableSql);
        String insertSql = "insert into trans values(\"aa\",23,33,1.4,2.4,1590738993000,\"2020-05-04\");";
        PreparedStatement pstmt = null;
        try {
            pstmt = executor.getInsertPreparedStmt(dbname, insertSql);
            Assert.assertTrue(pstmt.execute());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        ResultSet resultSet = null;
        try {
            pstmt = executor.getRequestPreparedStmt(dbname, selectSql);

            pstmt.setString(1, "bb");
            pstmt.setInt(2, 24);
            pstmt.setLong(3, 34l);
            pstmt.setFloat(4, 1.5f);
            pstmt.setDouble(5, 2.5);
            pstmt.setTimestamp(6, new Timestamp(1590738994000l));
            pstmt.setDate(7, Date.valueOf("2020-05-05"));

            resultSet = pstmt.executeQuery();

            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertFalse(resultSet.next());

            String drop = "drop table trans;";
            ok = executor.executeDDL(dbname, drop);
            Assert.assertTrue(ok);
            ok = executor.dropDB(dbname);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        }
    }

    @Test(dataProvider = "createOption")
    public void testDeploymentRequest(String compressType, String storageMode) {
        java.sql.Statement state = executor.getStatement();
        String dbname = "db" + random.nextInt(100000);
        String deploymentName = "dp_test1";
        try {
            state.execute("drop database if exists " + dbname + ";");
            state.execute("create database " + dbname + ";");
            state.execute("use " + dbname + ";");
            String baseSql = "create table trans(c1 string,\n" +
                    "                   c3 int,\n" +
                    "                   c4 bigint,\n" +
                    "                   c5 float,\n" +
                    "                   c6 double,\n" +
                    "                   c7 timestamp,\n" +
                    "                   c8 date,\n" +
                    "                   index(key=c1, ts=c7))";
            String createTableSql = String.format(" %s OPTIONS (compress_type='%s', storage_mode='%s');",
                    baseSql, compressType, storageMode);
            state.execute(createTableSql);
            String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                    "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
            String deploySql = "DEPLOY " + deploymentName + " " + selectSql;
            state.execute(deploySql);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
        String insertSql = "insert into trans values(\"aa\",23,33,1.4,2.4,1590738993000,\"2020-05-04\");";
        PreparedStatement pstmt = null;
        try {
            pstmt = executor.getInsertPreparedStmt(dbname, insertSql);
            Assert.assertTrue(pstmt.execute());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        ResultSet resultSet = null;
        try {
            Thread.sleep(1000);
            pstmt = executor.getCallablePreparedStmt(dbname, deploymentName);

            pstmt.setString(1, "bb");
            pstmt.setInt(2, 24);
            pstmt.setLong(3, 34l);
            pstmt.setFloat(4, 1.5f);
            pstmt.setDouble(5, 2.5);
            pstmt.setTimestamp(6, new Timestamp(1590738994000l));
            pstmt.setDate(7, Date.valueOf("2020-05-05"));

            resultSet = pstmt.executeQuery();

            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertFalse(resultSet.next());

            state.execute("drop deployment " + deploymentName + ";");
            String drop = "drop table trans;";
            boolean ok = executor.executeDDL(dbname, drop);
            Assert.assertTrue(ok);
            ok = executor.dropDB(dbname);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                state.close();
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        }
    }

    @Test(dataProvider = "createOption")
    public void testBatchRequest(String compressType, String storageMode) {
        String dbname = "db" + random.nextInt(100000);
        executor.dropDB(dbname);
        boolean ok = executor.createDB(dbname);
        Assert.assertTrue(ok);
        String baseSql = "create table trans(c1 string,\n" +
                "                   c3 int,\n" +
                "                   c4 bigint,\n" +
                "                   c5 float,\n" +
                "                   c6 double,\n" +
                "                   c7 timestamp,\n" +
                "                   c8 date,\n" +
                "                   index(key=c1, ts=c7))";
        String createTableSql = String.format(" %s OPTIONS (compress_type='%s', storage_mode='%s');",
                baseSql, compressType, storageMode);
        executor.executeDDL(dbname, createTableSql);
        String insertSql = "insert into trans values(\"aa\",23,33,1.4,2.4,1590738993000,\"2020-05-04\");";
        PreparedStatement pstmt = null;
        try {
            pstmt = executor.getInsertPreparedStmt(dbname, insertSql);
            Assert.assertTrue(pstmt.execute());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        ResultSet resultSet = null;
        try {
            List<Integer> list = new ArrayList<Integer>();
            pstmt = executor.getBatchRequestPreparedStmt(dbname, selectSql, list);
            int batchSize = 5;
            for (int idx = 0; idx < batchSize; idx++) {
                pstmt.setString(1, "bb");
                pstmt.setInt(2, 24);
                pstmt.setLong(3, 34l);
                pstmt.setFloat(4, 1.5f);
                pstmt.setDouble(5, 2.5);
                pstmt.setTimestamp(6, new Timestamp(1590738994000l + idx));
                pstmt.setDate(7, Date.valueOf("2020-05-05"));
                pstmt.addBatch();
            }

            resultSet = pstmt.executeQuery();

            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            int resultNum = 0;
            while (resultSet.next()) {
                Assert.assertEquals(resultSet.getString(1), "bb");
                Assert.assertEquals(resultSet.getInt(2), 24);
                Assert.assertEquals(resultSet.getLong(3), 34);
                resultNum++;
            }
            Assert.assertEquals(resultNum, batchSize);

            String drop = "drop table trans;";
            ok = executor.executeDDL(dbname, drop);
            Assert.assertTrue(ok);
            ok = executor.dropDB(dbname);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        }
    }

    @Test(dataProvider = "createOption")
    public void testDeploymentBatchRequest(String compressType, String storageMode) {
        java.sql.Statement state = executor.getStatement();
        String dbname = "db" + random.nextInt(100000);
        String deploymentName = "dp_test1";
        try {
            state.execute("drop database if exists " + dbname + ";");
            state.execute("create database " + dbname + ";");
            state.execute("use " + dbname + ";");
            String baseSql = "create table trans(c1 string,\n" +
                    "                   c3 int,\n" +
                    "                   c4 bigint,\n" +
                    "                   c5 float,\n" +
                    "                   c6 double,\n" +
                    "                   c7 timestamp,\n" +
                    "                   c8 date,\n" +
                    "                   index(key=c1, ts=c7))";
            String createTableSql = String.format(" %s OPTIONS (compress_type='%s', storage_mode='%s');",
                    baseSql, compressType, storageMode);
            state.execute(createTableSql);
            String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                    "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
            String deploySql = "DEPLOY " + deploymentName + " " + selectSql;
            state.execute(deploySql);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
        String insertSql = "insert into trans values(\"aa\",23,33,1.4,2.4,1590738993000,\"2020-05-04\");";
        PreparedStatement pstmt = null;
        try {
            pstmt = executor.getInsertPreparedStmt(dbname, insertSql);
            Assert.assertTrue(pstmt.execute());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception throwables) {
                    throwables.printStackTrace();
                }
            }
        }

        ResultSet resultSet = null;
        try {
            Thread.sleep(1000);
            pstmt = executor.getCallablePreparedStmtBatch(dbname, deploymentName);

            int batchSize = 5;
            for (int idx = 0; idx < batchSize; idx++) {
                pstmt.setString(1, "bb");
                pstmt.setInt(2, 24);
                pstmt.setLong(3, 34l);
                pstmt.setFloat(4, 1.5f);
                pstmt.setDouble(5, 2.5);
                pstmt.setTimestamp(6, new Timestamp(1590738994000l + idx));
                pstmt.setDate(7, Date.valueOf("2020-05-05"));
                pstmt.addBatch();
            }

            resultSet = pstmt.executeQuery();

            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            int resultNum = 0;
            while (resultSet.next()) {
                Assert.assertEquals(resultSet.getString(1), "bb");
                Assert.assertEquals(resultSet.getInt(2), 24);
                Assert.assertEquals(resultSet.getLong(3), 34);
                resultNum++;
            }
            Assert.assertEquals(resultNum, batchSize);

            state.execute("drop deployment " + deploymentName + ";");
            String drop = "drop table trans;";
            boolean ok = executor.executeDDL(dbname, drop);
            Assert.assertTrue(ok);
            ok = executor.dropDB(dbname);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                state.close();
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        }
    }

    @Test
    public void testResultSetNull() {
        java.sql.Statement state = executor.getStatement();
        String dbname = "db" + random.nextInt(100000);
        String deploymentName = "dp_test1";
        try {
            state.execute("drop database if exists " + dbname + ";");
            state.execute("create database " + dbname + ";");
            state.execute("use " + dbname + ";");
            String baseSql = "create table trans(c1 string,\n" +
                    "                   c3 int,\n" +
                    "                   c4 bigint,\n" +
                    "                   c5 float,\n" +
                    "                   c6 double,\n" +
                    "                   c7 timestamp,\n" +
                    "                   c8 date,\n" +
                    "                   index(key=c1, ts=c7));";
            state.execute(baseSql);
            String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                    "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS_RANGE BETWEEN 2s PRECEDING AND 0s OPEN PRECEDING EXCLUDE CURRENT_TIME);";
            String deploySql = "DEPLOY " + deploymentName + " " + selectSql;
            state.execute(deploySql);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            Thread.sleep(100);
            pstmt = executor.getCallablePreparedStmt(dbname, deploymentName);

            pstmt.setString(1, "aa");
            pstmt.setInt(2, 20);
            pstmt.setNull(3, Types.BIGINT);
            pstmt.setFloat(4, 1.1f);
            pstmt.setDouble(5, 2.1);
            pstmt.setTimestamp(6, new Timestamp(0));
            pstmt.setDate(7, Date.valueOf("2020-05-01"));

            resultSet = pstmt.executeQuery();

            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            while (resultSet.next()) {
                Assert.assertEquals(resultSet.getString(1), "aa");
                Assert.assertEquals(resultSet.getNString(1), "aa");
                Assert.assertEquals(resultSet.getInt(2), 20);
                Assert.assertEquals(resultSet.getNString(2), "20");
                Assert.assertTrue(resultSet.getNString(3) == null);
            }

            state.execute("drop deployment " + deploymentName + ";");
            String drop = "drop table trans;";
            boolean ok = executor.executeDDL(dbname, drop);
            Assert.assertTrue(ok);
            ok = executor.dropDB(dbname);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                state.close();
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (Exception throwables) {
                throwables.printStackTrace();
            }
        }
    }

}
