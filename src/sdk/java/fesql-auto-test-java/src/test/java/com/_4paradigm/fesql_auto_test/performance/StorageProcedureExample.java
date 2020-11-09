package com._4paradigm.fesql_auto_test.performance;

import com._4paradigm.sql.sdk.*;
import com._4paradigm.sql.sdk.impl.CallablePreparedStatementImpl;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import java.sql.*;
import java.sql.Date;

public class StorageProcedureExample extends BaseExample {

    private static final Logger logger = LoggerFactory.getLogger(StorageProcedureExample.class);

    private String ddl = "create table trans(c1 string,\n" +
            "                   c3 int,\n" +
            "                   c4 bigint,\n" +
            "                   c5 float,\n" +
            "                   c6 double,\n" +
            "                   c7 timestamp,\n" +
            "                   c8 date,\n" +
            "                   index(key=c1, ts=c7));";
    String sql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    private SqlExecutor sqlExecutor = null;
    private String db = "test_db2";
    private String spName = "sp";
    private String dropDdl = "drop table trans;";

    public void init() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setSessionTimeout(10000);
        sqlExecutor = new SqlClusterExecutor(option);
    }

    public void initDDL() throws Exception {
        sqlExecutor.dropDB(db);
        sqlExecutor.createDB(db);
        sqlExecutor.executeDDL(db, dropDdl);
        sqlExecutor.executeDDL(db, ddl);
        sqlExecutor.executeDDL(db, "drop procedure sp;");
//        Schema inputSchema = sqlExecutor.getInputSchema(db, sql);
    }

    public void initSample() {
        sqlExecutor.executeInsert(db, "insert into trans values(\"aa\",20,30,1.1,2.1,1590738990000,\"2020-05-01\");");
        sqlExecutor.executeInsert(db, "insert into trans values(\"aa\",21,31,1.2,2.2,1590738991000,\"2020-05-02\");");
        sqlExecutor.executeInsert(db, "insert into trans values(\"aa\",22,32,1.3,2.3,1590738992000,\"2020-05-03\");");
        sqlExecutor.executeInsert(db, "insert into trans values(\"aa\",23,33,1.4,2.4,1590738993000,\"2020-05-04\");");
    }

    public void clearDDL() throws Exception {
        sqlExecutor.executeDDL(db, dropDdl);
    }

    public void createProcedure() throws Exception {
        String spSql = "create procedure " + spName + "(" + "const c1 string, const c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date" + ")" +
                " begin " + sql + " end;";
        boolean ok = sqlExecutor.executeDDL(db, spSql);
        Assert.assertTrue(ok);
    }

    public void callProcedureWithPstms() throws Exception {
        Object[] requestRow = new Object[7];
        requestRow[0] = "bb";
        requestRow[1] = 24;
        requestRow[2] = 34l;
        requestRow[3] = 1.5f;
        requestRow[4] = 2.5;
        requestRow[5] = new Timestamp(1590738994000l);
        requestRow[6] = Date.valueOf("2020-05-05");

        CallablePreparedStatementImpl callablePreparedStmt = sqlExecutor.getCallablePreparedStmt(db, spName);
        ResultSetMetaData metaData = callablePreparedStmt.getMetaData();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            Object obj = requestRow[i];
            if (obj == null) {
                callablePreparedStmt.setNull(i + 1, 0);
                continue;
            }
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.BOOLEAN) {
                callablePreparedStmt.setBoolean(i + 1, Boolean.parseBoolean(obj.toString()));
            } else if (columnType == Types.SMALLINT) {
                callablePreparedStmt.setShort(i + 1, Short.parseShort(obj.toString()));
            } else if (columnType == Types.INTEGER) {
                callablePreparedStmt.setInt(i + 1, Integer.parseInt(obj.toString()));
            } else if (columnType == Types.BIGINT) {
                callablePreparedStmt.setLong(i + 1, Long.parseLong(obj.toString()));
            } else if (columnType == Types.FLOAT) {
                callablePreparedStmt.setFloat(i + 1, Float.parseFloat(obj.toString()));
            } else if (columnType == Types.DOUBLE) {
                callablePreparedStmt.setDouble(i + 1, Double.parseDouble(obj.toString()));
            } else if (columnType == Types.TIMESTAMP) {
                callablePreparedStmt.setTimestamp(i + 1, (Timestamp)obj);
            } else if (columnType == Types.DATE) {
                callablePreparedStmt.setDate(i + 1, (Date) obj);
            } else if (columnType == Types.VARCHAR) {
                callablePreparedStmt.setString(i + 1, (String)obj);
            } else {
                logger.error("fail to build request row: invalid data type {]", columnType);
                return;
            }
        }
        ResultSet sqlResultSet = callablePreparedStmt.executeQuery();
        Assert.assertTrue(sqlResultSet.next());

        Assert.assertEquals(sqlResultSet.getMetaData().getColumnCount(), 3);
        Assert.assertEquals(sqlResultSet.getString(1), "bb");
        Assert.assertEquals(sqlResultSet.getInt(2), 24);
        Assert.assertEquals(sqlResultSet.getLong(3), 34);
        System.out.println("ok");
        sqlResultSet.close();
        callablePreparedStmt.close();
    }

    public static void run() {
        final StorageProcedureExample example = new StorageProcedureExample();
        try {
            example.init();
            example.initDDL();
            example.initSample();
            example.createProcedure();
            System.out.println("create ok");
            example.callProcedureWithPstms();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        run();
    }
}
