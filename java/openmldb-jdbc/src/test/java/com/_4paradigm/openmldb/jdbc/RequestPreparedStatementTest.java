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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Date;
import java.sql.Timestamp;

import org.testng.Assert;
import org.testng.annotations.Test;

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

    @Test
    public void testRequest() {
        String dbname = "db" + random.nextInt(100000);
        executor.dropDB(dbname);
        boolean ok = executor.createDB(dbname);
        Assert.assertTrue(ok);
        String createTableSql = "create table trans(c1 string,\n" +
                "                   c3 int,\n" +
                "                   c4 bigint,\n" +
                "                   c5 float,\n" +
                "                   c6 double,\n" +
                "                   c7 timestamp,\n" +
                "                   c8 date,\n" +
                "                   index(key=c1, ts=c7));";
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
            Assert.fail("catched exception", e);
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

    @Test
    public void testBatchRequest() {
        String dbname = "db" + random.nextInt(100000);
        executor.dropDB(dbname);
        boolean ok = executor.createDB(dbname);
        Assert.assertTrue(ok);
        String createTableSql = "create table trans(c1 string,\n" +
                "                   c3 int,\n" +
                "                   c4 bigint,\n" +
                "                   c5 float,\n" +
                "                   c6 double,\n" +
                "                   c7 timestamp,\n" +
                "                   c8 date,\n" +
                "                   index(key=c1, ts=c7));";
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
}
