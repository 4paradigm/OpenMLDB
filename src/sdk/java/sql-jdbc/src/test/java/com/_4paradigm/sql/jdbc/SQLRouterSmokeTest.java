package com._4paradigm.sql.jdbc;

import com._4paradigm.sql.*;
import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;

public class SQLRouterSmokeTest {

    private Random random = new Random(System.currentTimeMillis());

    @Test
    public void testSmoke() {
        SdkOption option = new SdkOption();
        option.setZkPath(TestConfig.ZK_PATH);
        option.setZkCluster(TestConfig.ZK_CLUSTER);
        option.setSessionTimeout(200000);
        try {
            SqlExecutor router = new SqlClusterExecutor(option);
            String dbname = "db" + random.nextInt(100000);
            // create db
            boolean ok = router.createDB(dbname);
            Assert.assertTrue(ok);
            String ddl = "create table tsql1010 ( col1 bigint, col2 string, index(key=col2, ts=col1));";
            // create table
            ok = router.executeDDL(dbname, ddl);
            Assert.assertTrue(ok);
            // insert normal
            String insert = "insert into tsql1010 values(1000, 'hello');";
            ok = router.executeInsert(dbname, insert);
            Assert.assertTrue(ok);
            // insert placeholder
            String insertPlaceholder = "insert into tsql1010 values(?, ?);";
            SQLInsertRow insertRow = router.getInsertRow(dbname, insertPlaceholder);
            insertRow.Init(5);
            insertRow.AppendInt64(1001);
            insertRow.AppendString("world");
            ok = router.executeInsert(dbname, insertPlaceholder, insertRow);
            Assert.assertTrue(ok);
            // insert placeholder batch
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
            ResultSet rs1 = router.executeSQL(dbname, select1);
            Assert.assertEquals(4, rs1.Size());
            Assert.assertEquals(2, rs1.GetSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs1.GetSchema().GetColumnType(0).toString());
            Assert.assertEquals("kTypeString", rs1.GetSchema().GetColumnType(1).toString());
            Assert.assertTrue(rs1.Next());
            Assert.assertEquals("hello", rs1.GetStringUnsafe(1));
            Assert.assertEquals(1000, rs1.GetInt64Unsafe(0));
            Assert.assertTrue(rs1.Next());
            Assert.assertEquals("world", rs1.GetStringUnsafe(1));
            Assert.assertEquals(1001, rs1.GetInt64Unsafe(0));

            String select2 = "select col1 from tsql1010;";
            ResultSet rs2 = router.executeSQL(dbname, select2);
            Assert.assertEquals(4, rs2.Size());
            Assert.assertEquals(1, rs2.GetSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs2.GetSchema().GetColumnType(0).toString());
            Assert.assertTrue(rs2.Next());
            Assert.assertEquals(1000, rs2.GetInt64Unsafe(0));
            Assert.assertTrue(rs2.Next());
            Assert.assertEquals(1001, rs2.GetInt64Unsafe(0));

            String select3 = "select col2 from tsql1010;";
            ResultSet rs3 = router.executeSQL(dbname, select3);
            Assert.assertEquals(4, rs3.Size());
            Assert.assertEquals(1, rs3.GetSchema().GetColumnCnt());
            Assert.assertEquals("kTypeString", rs3.GetSchema().GetColumnType(0).toString());
            Assert.assertTrue(rs3.Next());
            Assert.assertEquals("hello", rs3.GetStringUnsafe(0));
            Assert.assertTrue(rs3.Next());
            Assert.assertEquals("world", rs3.GetStringUnsafe(0));
            // drop table
            String drop = "drop table tsql1010;";
            ok = router.executeDDL(dbname, drop);
            Assert.assertTrue(ok);
            // insert into deleted table
            ok = router.executeInsert(dbname, insertPlaceholder, insertRow);
            Assert.assertFalse(ok);
            // drop database
            ok = router.dropDB(dbname);
            Assert.assertTrue(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
