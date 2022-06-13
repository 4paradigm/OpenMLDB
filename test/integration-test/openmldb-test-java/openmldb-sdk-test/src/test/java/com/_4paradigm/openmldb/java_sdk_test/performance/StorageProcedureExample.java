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

package com._4paradigm.openmldb.java_sdk_test.performance;

import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.sdk.QueryFuture;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.sql.*;
import java.util.concurrent.TimeUnit;

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
    private String db = "mydb01";
    private String spName = "sp";
    private String dropDdl = "drop table trans;";

    public void init() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setZkSessionTimeout(10000);
        option.setRequestTimeout(60000);
        sqlExecutor = new SqlClusterExecutor(option);
    }

    public void initDDL() {
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
        sqlExecutor.showProcedure(db, spName);
    }

    public void callProcedureWithPstms() throws Exception {
        CallablePreparedStatement callablePreparedStmt = sqlExecutor.getCallablePreparedStmt(db, spName);
        ResultSetMetaData metaData = callablePreparedStmt.getMetaData();
        if (setData(callablePreparedStmt, metaData, "bb")) return;
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

    public void callProcedureWithPstmsAsync() throws Exception {
        CallablePreparedStatement callablePreparedStmt = sqlExecutor.getCallablePreparedStmt(db, spName);
        ResultSetMetaData metaData = callablePreparedStmt.getMetaData();
        if (setData(callablePreparedStmt, metaData, "bb")) return;
        QueryFuture future = callablePreparedStmt.executeQueryAsync(100, TimeUnit.MILLISECONDS);
        System.out.println("done: " + future.isDone());
        ResultSet sqlResultSet = future.get();
        Assert.assertTrue(sqlResultSet.next());

        Assert.assertEquals(sqlResultSet.getMetaData().getColumnCount(), 3);
        Assert.assertEquals(sqlResultSet.getString(1), "bb");
        Assert.assertEquals(sqlResultSet.getInt(2), 24);
        Assert.assertEquals(sqlResultSet.getLong(3), 34);
        System.out.println("ok");
        sqlResultSet.close();
        callablePreparedStmt.close();
    }

    private boolean setData(CallablePreparedStatement callablePreparedStmt, ResultSetMetaData metaData, String strVal) throws SQLException {
        for (int i = 0; i < metaData.getColumnCount(); i++) {
//            if (obj == null) {
//                callablePreparedStmt.setNull(i + 1, 0);
//                continue;
//            }
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.BOOLEAN) {
                callablePreparedStmt.setBoolean(i + 1, true);
            } else if (columnType == Types.SMALLINT) {
                callablePreparedStmt.setShort(i + 1, (short) 22);
            } else if (columnType == Types.INTEGER) {
                callablePreparedStmt.setInt(i + 1, 24);
            } else if (columnType == Types.BIGINT) {
                callablePreparedStmt.setLong(i + 1, 34l);
            } else if (columnType == Types.FLOAT) {
                callablePreparedStmt.setFloat(i + 1, 1.5f);
            } else if (columnType == Types.DOUBLE) {
                callablePreparedStmt.setDouble(i + 1, 2.5);
            } else if (columnType == Types.TIMESTAMP) {
                callablePreparedStmt.setTimestamp(i + 1, new Timestamp(1590738994000l));
            } else if (columnType == Types.DATE) {
                callablePreparedStmt.setDate(i + 1, Date.valueOf("2020-05-05"));
            } else if (columnType == Types.VARCHAR) {
                callablePreparedStmt.setString(i + 1, strVal);
            } else {
                logger.error("fail to build request row: invalid data type {]", columnType);
                return false;
            }
        }
        return true;
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
            example.callProcedureWithPstmsAsync();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        run();
    }
}
