package com._4paradigm.fesql.jdbc;

import com._4paradigm.fesql.sdk.DBMSSdk;
import com._4paradigm.fesql.sdk.ResultSet;
import com._4paradigm.fesql.sdk.Status;
import com._4paradigm.fesql.sdk.TableSet;
import com._4paradigm.fesql_interface;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JDBCSmokeTest {
    static {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("mac")) {
            String path = JDBCSmokeTest.class.getResource("/libfesql_jsdk.dylib").getPath();
            System.load(path);
        }else {
            String path = JDBCSmokeTest.class.getResource("/libfesql_jsdk.so").getPath();
            System.load(path);
        }
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
        String createTable =  "create table t1 ( col1 bigint, col2 string, index(key=col1, ts=col2));";
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
        String createTable =  "create table t1 ( col1 bigint, col2 string, index(key=col1, ts=col2));";
        sdk.ExecuteQuery(dbname, createTable, status);
        String insert = "insert into t1 values(1000, 'hello');";
        sdk.ExecuteQuery(dbname, insert, status);
        System.out.println(status.getMsg());
        Assert.assertEquals(0, status.getCode());
        String query = "select col1 + 1, col2 from t1;";
        ResultSet rs = sdk.ExecuteQuery(dbname, query, status);
        Assert.assertEquals(0, status.getCode());
        Assert.assertEquals(1, rs.Size());
        Assert.assertTrue(rs.Next());
        Assert.assertEquals(1001, rs.GetInt64Unsafe(0));
        Assert.assertEquals("hello", rs.GetStringUnsafe(1));
    }
}
