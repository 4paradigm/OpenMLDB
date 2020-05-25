package com._4paradigm.sql.jdbc;

import com._4paradigm.sql.SQLRouter;
import com._4paradigm.sql.SQLRouterOptions;
import com._4paradigm.sql.Status;
import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import com._4paradigm.sql.sql_router_sdk;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;

public class SQLRouterSmokeTest {
    static {
        String os = System.getProperty("os.name").toLowerCase();
        String path = SQLRouterSmokeTest.class.getResource("/libsql_jsdk.so").getPath();
        System.load(path);
    }
    private Random random = new Random(System.currentTimeMillis());

    @Test
    public void testCreateDB() {
        SdkOption option = new SdkOption();
        option.setZkPath(TestConfig.ZK_PATH);
        option.setZkCluster(TestConfig.ZK_CLUSTER);
        option.setSessionTimeout(200000);
        try {
            SqlExecutor router = new SqlClusterExecutor(option);
            String dbname = "db" + random.nextInt(100000);
            boolean ok = router.createDB(dbname);
            Assert.assertTrue(ok);
            String ddl = "create table tsql1010 ( col1 bigint, col2 string, index(key=col2, ts=col1));";
            ok = router.executeDDL(dbname, ddl);
            Assert.assertTrue(ok);
            String insert = "insert into tsql1010 values(1000, 'hello');";
            ok = router.executeInsert(dbname, insert);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }
}
