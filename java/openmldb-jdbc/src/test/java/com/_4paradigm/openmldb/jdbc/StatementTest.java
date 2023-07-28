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
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import lombok.extern.slf4j.Slf4j;

import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;
import java.util.Map;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

@Slf4j
public class StatementTest {
    static SqlClusterExecutor router;
    static String testDB = "statement_test" + System.currentTimeMillis();
    static {
        SdkOption option = new SdkOption();
        option.setZkPath(TestConfig.ZK_PATH);
        option.setZkCluster(TestConfig.ZK_CLUSTER);
        option.setSessionTimeout(200000);
        try {
            router = new SqlClusterExecutor(option);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeSuite
    public void before() {
        // ensure test database does not exist
        java.sql.Statement state = router.getStatement();
        try {
            state.execute("create database " + testDB);
        } catch (SQLException e) {
            Assert.fail(testDB + " database already exists", e);
        }
    }

    @AfterSuite
    public void after() throws SQLException {
        log.info("after, clean up test database " + testDB);

        router.refreshCatalog();
        // can't use statement to cleanup, got SIGSEGV, check later
        ResultSet rs = router.executeSQL(testDB, "show deployments");
        while (rs.next()) {
            String deployName = rs.getString(1);
            log.info("drop deployment " + deployName);
            router.executeSQL(testDB, "drop deployment " + deployName);
        }
        rs = router.executeSQL(testDB, "show tables");
        while (rs.next()) {
            String tableName = rs.getString(1);
            log.info("drop table " + tableName);
            router.executeSQL(testDB, "drop table " + tableName);
        }
        rs = router.executeSQL(testDB, "drop database " + testDB);
        try {
            rs.next();
            Assert.fail("shouldn't get non-nullable result from ddl");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "resultset is null");
        }
        log.info("cleanup done");
    }

    // query success -> result != null -> true
    // but ddl won't return result, if ddl ok, return false, if ddl failed, throw
    // exception
    static void checkDDL(boolean ret) {
        Assert.assertFalse(ret);
    }

    @Test
    public void testExecute() {
        java.sql.Statement state = router.getStatement();

        try {
            boolean ret = state.execute("SET @@execute_mode='online';");
            checkDDL(ret);
            ret = state.execute("use " + testDB);
            checkDDL(ret);
            ret = state.execute("create table testtable111(col1 bigint, col2 string, index(key=col2, " +
                    "ts=col1));");
            checkDDL(ret);
            int r = state.executeUpdate("insert into testtable111 values(1000, 'hello');");
            // update insert stmt, is not dml, return nothing
            Assert.assertEquals(r, 0);
            r = state.executeUpdate("insert into testtable111 values(1001, 'xxxx');");
            Assert.assertEquals(r, 0);
            ret = state.execute("select * from testtable111");
            Assert.assertTrue(ret);
            java.sql.ResultSet rs = state.getResultSet();
            Assert.assertTrue(rs.next());
            Map<Long, String> result = new HashMap<>();
            result.put(rs.getLong(1), rs.getString(2));
            Assert.assertTrue(rs.next());
            result.put(rs.getLong(1), rs.getString(2));
            Assert.assertEquals(result.size(), 2);
            Assert.assertEquals(result.get(1000L), "hello");
            Assert.assertEquals(result.get(1001L), "xxxx");
        } catch (Exception e) {
            Assert.fail("catched exception", e);
        } finally {
            try {
                state.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testDeploy() throws SQLException {
        java.sql.Statement state = router.getStatement();
        try {
            // should be a new database
            boolean ret = state.execute("SET @@execute_mode='online';");
            checkDDL(ret);
            ret = state.execute("use " + testDB);
            checkDDL(ret);
            String createSql = "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp," +
                    "c8 date, index(key=c1, ts=c4, abs_ttl=0, ttl_type=absolute));";
            ret = state.execute(createSql);
            checkDDL(ret);
            String deploySql = "deploy demo SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans " +
                    " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
            ret = state.execute(deploySql);
            checkDDL(ret);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            state.close();
        }
    }

    private void checkDataCount(String tableName, int expectCnt) {
        java.sql.Statement state = router.getStatement();
        try {
            state.execute("select * from " + tableName);
            java.sql.ResultSet rs = state.getResultSet();
            int cnt = 0;
            while (rs.next()) {
                cnt++;
            }
            Assert.assertEquals(expectCnt, cnt);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                state.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testDelete() {
        java.sql.Statement state = router.getStatement();
        try {
            boolean ret = state.execute("SET @@execute_mode='online';");
            checkDDL(ret);
            ret = state.execute("use " + testDB);
            checkDDL(ret);
            ret = state.execute("create table t1(col1 bigint, col2 string, index(key=col2, " +
                    "ts=col1));");
            checkDDL(ret);
            state.executeUpdate("insert into t1 values(1000, 'key1');");
            state.executeUpdate("insert into t1 values(1002, 'key1');");
            state.executeUpdate("insert into t1 values(1001, 'key2');");
            state.executeUpdate("insert into t1 values(1003, 'key3');");
            state.executeUpdate("insert into t1 values(1004, 'key4');");
            state.executeUpdate("insert into t1 values(1001, 'key5');");
            state.executeUpdate("insert into t1 values(1003, NULL);");
            state.executeUpdate("insert into t1 values(1003, '');");
            state.execute("select * from t1");
            checkDataCount("t1", 8);
            String sql = "DELETE FROM t1 WHERE col2 = 'key1';";
            state.execute(sql);
            checkDataCount("t1", 6);
            state.execute("DELETE FROM t1 WHERE col2 = NULL;");
            checkDataCount("t1", 5);
            state.execute("DELETE FROM t1 WHERE col2 = '';");
            checkDataCount("t1", 4);
            sql = "DELETE FROM t1 WHERE col2 = ?;";
            java.sql.PreparedStatement p1 = router.getDeletePreparedStmt(testDB, sql);
            p1.setString(1, "key2");
            p1.executeUpdate();
            p1.setString(1, "keynoexist");
            p1.executeUpdate();
            checkDataCount("t1", 3);
            p1.setString(1, "key3");
            p1.addBatch();
            p1.setString(1, "key4");
            p1.addBatch();
            p1.setString(1, "key2");
            p1.addBatch();
            p1.executeBatch();
            checkDataCount("t1", 1);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                state.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
