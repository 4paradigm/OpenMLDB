package com._4paradigm.sql.jdbc;

import com._4paradigm.sql.SQLRouter;
import com._4paradigm.sql.SQLRouterOptions;
import com._4paradigm.sql.Status;
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
        SQLRouterOptions options = new SQLRouterOptions();
        options.setZk_path(TestConfig.ZK_PATH);
        options.setZk_cluster(TestConfig.ZK_CLUSTER);
        options.setSession_timeout(3000);
        SQLRouter router = sql_router_sdk.NewClusterSQLRouter(options);
        Assert.assertNotNull(router);
        Status status = new Status();
        String dbname = "db" + random.nextInt(100000);
        boolean ok = router.CreateDB(dbname, status);
        Assert.assertTrue(ok);
        String ddl = "create table tsql1010 ( col1 bigint, col2 string, index(key=col2, ts=col1));";
        ok = router.ExecuteDDL(dbname, ddl, status);
        Assert.assertTrue(ok);
        Assert.assertTrue(router.RefreshCatalog());
        String insert = "insert into tsql1010 values(1000, 'hello');";
        ok = router.ExecuteInsert(dbname, insert, status);
        Assert.assertTrue(ok);
    }
}
