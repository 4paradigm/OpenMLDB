package com._4paradigm.fesql_auto_test.performance;

import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlException;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.BatchCallablePreparedStatementImpl;
import com._4paradigm.sql.sdk.impl.CallablePreparedStatementImpl;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Random;

public class ComplexSql3Example extends BaseExample {
    private static final Logger logger = LoggerFactory.getLogger(ComplexSql3Example.class);

    private String ddl1 = "create table `table_2`(\n" +
            "`s1` string,\n" +
            "`s2` string,\n" +
            "`t1` timestamp,\n" +
            "`t2` date,\n" +
            "`d1` float,\n" +
            "`d2` double,\n" +
            "`c1` int,\n" +
            "`c2` bigint,\n" +
            "`ai` string,\n" +
            "`kn` string,\n" +
            "`ks` string,\n" +
            "index(key=(`s1`), ts=`t1`, ttl=1440m, ttl_type=absolute)\n" +
            ");";
    private String ddl2 = "create table `main`(\n" +
            "`label` int,\n" +
            "`s1` string,\n" +
            "`s2` string,\n" +
            "`t1` timestamp,\n" +
            "`t2` date,\n" +
            "`d1` float,\n" +
            "`d2` double,\n" +
            "`c1` int,\n" +
            "`c2` bigint,\n" +
            "`ai` string,\n" +
            "`kn` string,\n" +
            "`ks` string,\n" +
            "index(key=(`s2`), ts=`t1`, ttl=1440m, ttl_type=absolute)\n" +
            ");";


    private SqlExecutor sqlExecutor = null;
    private String db = "fix";
    private String spSql;

    public void init() throws SqlException {
        try {
            String path = System.getProperty("user.dir");
            File file = new File(path + "/fesql-auto-test-java/src/test/resources/xjd_sp_ddl.txt");
            spSql = IOUtils.toString(new FileInputStream(file), "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setSessionTimeout(10000);
        sqlExecutor = new SqlClusterExecutor(option);
    }

    public void initDDL() throws Exception {
        sqlExecutor.dropDB(db);
        sqlExecutor.createDB(db);
        boolean ok = sqlExecutor.executeDDL(db, ddl1);
        Assert.assertTrue(ok);
        ok = sqlExecutor.executeDDL(db, ddl2);
        Assert.assertTrue(ok);
        System.out.println("init ddl finished");
    }

    public void createProcedure() throws Exception {
        boolean ok = sqlExecutor.executeDDL(db, spSql);
        Assert.assertTrue(ok);
    }

    public void callProcedureWithPstms() throws Exception {
//        Object[] requestRow = new Object[7];
//        requestRow[0] = "bb";
//        requestRow[1] = 24;
//        requestRow[2] = 100000000;
//        requestRow[3] = 1.5f;
//        requestRow[4] = 2.5;
//        requestRow[5] = new Timestamp(1590738994000l);
//        requestRow[6] = Date.valueOf("2020-05-05");
        CallablePreparedStatementImpl callablePreparedStmt = sqlExecutor.getCallablePreparedStmt(db, "xjd1");
        ResultSetMetaData metaData = callablePreparedStmt.getMetaData();
        Random random = new Random();
        Object num = random.nextInt();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
//            Object obj = requestRow[i];
//            if (obj == null) {
//                callablePreparedStmt.setNull(i + 1, 0);
//                continue;
//            }
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.BOOLEAN) {
                callablePreparedStmt.setBoolean(i + 1, true);
            } else if (columnType == Types.SMALLINT) {
                callablePreparedStmt.setShort(i + 1, (short) 1);
            } else if (columnType == Types.INTEGER) {
                callablePreparedStmt.setInt(i + 1, 11);
            } else if (columnType == Types.BIGINT) {
                callablePreparedStmt.setLong(i + 1, Long.valueOf(num.toString()));
            } else if (columnType == Types.FLOAT) {
                callablePreparedStmt.setFloat(i + 1, Long.valueOf(num.toString()));
            } else if (columnType == Types.DOUBLE) {
                callablePreparedStmt.setDouble(i + 1, 1.0);
            } else if (columnType == Types.TIMESTAMP) {
                callablePreparedStmt.setTimestamp(i + 1, new Timestamp(1590738994000l));
            } else if (columnType == Types.DATE) {
                callablePreparedStmt.setDate(i + 1, Date.valueOf("2020-05-05"));
            } else if (columnType == Types.VARCHAR) {
                callablePreparedStmt.setString(i + 1, "string");
            } else {
                logger.error("fail to build request row: invalid data type {]", columnType);
                return;
            }
        }
        ResultSet sqlResultSet = callablePreparedStmt.executeQuery();
        Assert.assertTrue(sqlResultSet.next());
        for (int i = 1; i < sqlResultSet.getMetaData().getColumnCount(); i++) {
            System.out.println("output column: " + i + ", " + sqlResultSet.getNString(i));
        }

//        Assert.assertEquals(sqlResultSet.getMetaData().getColumnCount(), 3);
//        Assert.assertEquals(sqlResultSet.getString(1), "bb");
//        Assert.assertEquals(sqlResultSet.getInt(2), 24);
//        Assert.assertEquals(sqlResultSet.getLong(3), 34);
//        Thread.sleep(5000);
        System.out.println("call ok");
        sqlResultSet.close();
        callablePreparedStmt.close();
    }

    public static void run() {
        final ComplexSql3Example example = new ComplexSql3Example();
        try {
            example.init();
            System.out.println("init success");
//            example.initDDL();
            example.createProcedure();
//            example.callProcedureWithPstms();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        run();
    }

}
