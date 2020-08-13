package com._4paradigm.sql.jdbc;

import com._4paradigm.sql.*;
import com._4paradigm.sql.sdk.InsertPreparedStatementImpl;
import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import com.sun.xml.internal.ws.policy.AssertionSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Objects;
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
            router.dropDB(dbname);
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
            InsertPreparedStatementImpl impl = router.getInsertPrepareStmt(dbname, insertPlaceholder);
            impl.setLong(1, 1001);
            impl.setString(2, "world");
            ok = impl.execute();
            // ok = router.executeInsert(dbname, insertPlaceholder, insertRow);
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

    @Test
    public void testInsertPreparedState() {
        SdkOption option = new SdkOption();
        option.setZkPath(TestConfig.ZK_PATH);
        option.setZkCluster(TestConfig.ZK_CLUSTER);
        option.setSessionTimeout(200000);
        try {
            SqlExecutor router = new SqlClusterExecutor(option);
            String dbname = "db" + random.nextInt(100000);
            // create db
            router.dropDB(dbname);
            boolean ok = router.createDB(dbname);
            Assert.assertTrue(ok);
            String ddl = "create table tsql1010 ( col1 bigint, col2 bigint, col3 string, col4 float, index(key=col2, ts=col1));";
            // create table
            ok = router.executeDDL(dbname, ddl);
            Assert.assertTrue(ok);
            // insert normal
            String insert = "insert into tsql1010 values(1000, 1, 'shijiazhuang', 1.0);";
            ok = router.executeInsert(dbname, insert);
            Assert.assertTrue(ok);
            // insert placeholder
            String insertPlaceholder = "insert into tsql1010 values(?, 2, 'taiyuan', 2.0);";
            InsertPreparedStatementImpl impl = router.getInsertPrepareStmt(dbname, insertPlaceholder);
            impl.setLong(1, 1001);
            try {
                impl.setInt(2, 1002);
            } catch (Exception e) {
                Assert.assertEquals("out of data range", e.getMessage());
            }
            ok = impl.execute();
            Assert.assertTrue(ok);
            String insertPlaceholder2 = "insert into tsql1010 values(1002, ?, 'shengyang', 3.0);";
            InsertPreparedStatementImpl impl2 = router.getInsertPrepareStmt(dbname, insertPlaceholder2);
            try {
                impl2.execute();
            } catch (Exception e) {
                Assert.assertEquals("data not enough", e.getMessage());
            }
            try {
                impl2.setString(1, "c");
            } catch (Exception e) {
                Assert.assertEquals("data type not match", e.getMessage());
            }
            impl2.setLong(1, 3);
            ok = impl2.execute();
            Assert.assertTrue(ok);

            String insertPlaceholder3 = "insert into tsql1010 values(?, ?, ?, ?);";
            SQLInsertRow insertRow = router.getInsertRow(dbname, insertPlaceholder);
            InsertPreparedStatementImpl impl3 = router.getInsertPrepareStmt(dbname, insertPlaceholder3);
            impl3.setLong(1, 1003);
            impl3.setString(3, "hangzhouxxxxxx");
            impl3.setString(3, "hangzhou");
            impl3.setLong(2, 4);
            impl3.setFloat(4, 4.0f);
            impl3.closeOnCompletion();
            Assert.assertTrue(impl3.isCloseOnCompletion());
            ok = impl3.execute();
            try {
                impl3.execute();
            } catch (Exception e) {
                Assert.assertEquals("preparedstatement closed", e.getMessage());
            }
            Assert.assertTrue(ok);
            InsertPreparedStatementImpl impl4 = router.getInsertPrepareStmt(dbname, insertPlaceholder3);
            impl4.close();
            Assert.assertTrue(impl4.isClosed());
            // select
            String select1 = "select * from tsql1010;";
            ResultSet rs1 = router.executeSQL(dbname, select1);
            Assert.assertEquals(4, rs1.Size());
            Assert.assertEquals(4, rs1.GetSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs1.GetSchema().GetColumnType(0).toString());
            Assert.assertEquals("kTypeInt64", rs1.GetSchema().GetColumnType(1).toString());
            Assert.assertEquals("kTypeString", rs1.GetSchema().GetColumnType(2).toString());
            Assert.assertEquals("kTypeFloat", rs1.GetSchema().GetColumnType(3).toString());
            Object[][]datas = new Object[][]{
                {4.0f, "hangzhou", 4l, 1003l},
                {3.0f, "shengyang", 3l, 1002l},
                {1.0f, "shijiazhuang", 1l, 1000l},
                {2.0f, "taiyuan", 2l, 1001l},
            };
            for (int i = 0; i < rs1.Size(); i++) {
                Assert.assertTrue(rs1.Next());
                Assert.assertEquals(datas[i][2], rs1.GetInt64Unsafe(1));
                Assert.assertEquals(datas[i][1], rs1.GetStringUnsafe(2));
                Assert.assertEquals(datas[i][0], rs1.GetFloatUnsafe(3));
                Assert.assertEquals(datas[i][3], rs1.GetInt64Unsafe(0));
            }

            String select2 = "select col1 from tsql1010;";
            ResultSet rs2 = router.executeSQL(dbname, select2);
            Assert.assertEquals(4, rs2.Size());
            Assert.assertEquals(1, rs2.GetSchema().GetColumnCnt());
            Assert.assertEquals("kTypeInt64", rs2.GetSchema().GetColumnType(0).toString());
            Assert.assertTrue(rs2.Next());
            Assert.assertEquals(1003, rs2.GetInt64Unsafe(0));
            Assert.assertTrue(rs2.Next());
            Assert.assertEquals(1002, rs2.GetInt64Unsafe(0));

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
