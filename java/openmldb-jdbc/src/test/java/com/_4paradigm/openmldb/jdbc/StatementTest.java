package com._4paradigm.openmldb.jdbc;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Map;
import java.util.HashMap;

public class StatementTest {
    static SqlExecutor router;

    static {
        SdkOption option = new SdkOption();
        option.setZkPath(TestConfig.ZK_PATH);
        option.setZkCluster(TestConfig.ZK_CLUSTER);
        option.setZkSessionTimeout(200000);
        try {
            router = new SqlClusterExecutor(option);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testExecute() {
        java.sql.Statement state = router.getStatement();

        try {
            boolean ret = state.execute("SET @@execute_mode='online';");
            ret = state.execute("create database test");
            Assert.assertFalse(ret);
            ret = state.execute("use test");
            Assert.assertFalse(ret);
            ret = state.execute("create table testtable111(col1 bigint, col2 string, index(key=col2, ts=col1));");
            Assert.assertFalse(ret);
            state.executeUpdate("insert into testtable111 values(1000, 'hello');");
            state.executeUpdate("insert into testtable111 values(1001, 'xxxx');");
            ret = state.execute("select * from testtable111");
            Assert.assertTrue(ret);
            java.sql.ResultSet rs = state.getResultSet();
            Assert.assertTrue(rs.next());
            Map<Long, String> result = new HashMap<>();
            result.put(rs.getLong(1), rs.getString(2));
            Assert.assertTrue(rs.next());
            result.put(rs.getLong(1), rs.getString(2));
            Assert.assertEquals(2, result.size());
            Assert.assertEquals("hello", result.get(1000L));
            Assert.assertEquals("xxxx", result.get(1001L));

            ret = state.execute("drop table testtable111");
            Assert.assertFalse(ret);
            ret = state.execute("drop database test");
            Assert.assertFalse(ret);
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

    @Test
    public void testDeploy() {
        java.sql.Statement state = router.getStatement();
        try {
            boolean ret = state.execute("SET @@execute_mode='online';");
            ret = state.execute("create database testxx");
            Assert.assertFalse(ret);
            ret = state.execute("use testxx");
            Assert.assertFalse(ret);
            String createSql = "create table trans (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp," +
                    "c8 date, index(key=c1, ts=c4, abs_ttl=0, ttl_type=absolute));";
            ret = state.execute(createSql);
            Assert.assertFalse(ret);
            String deploySql = "deploy demo SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans " +
                    " WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
            ret = state.execute(deploySql);
            Assert.assertFalse(ret);
            ret = state.execute("drop deployment demo;");
            Assert.assertFalse(ret);
            ret = state.execute("drop table trans");
            Assert.assertFalse(ret);
            ret = state.execute("drop database testxx");
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
