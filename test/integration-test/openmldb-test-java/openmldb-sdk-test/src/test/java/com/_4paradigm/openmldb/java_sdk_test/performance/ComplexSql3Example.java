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
import com._4paradigm.openmldb.sdk.ProcedureInfo;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ComplexSql3Example extends BaseExample {
    private static final Logger logger = LoggerFactory.getLogger(ComplexSql3Example.class);
    private SqlExecutor sqlExecutor = null;
    private String db = "fix_test32";
    private String spSql;
    private List<String> tableDDLList = new ArrayList<>();

    public void init() throws SqlException {
        try {
            String path = System.getProperty("user.dir");
            File file = new File(path + "/fesql-auto-test-java/src/test/resources/xjd_sp_ddl.txt");
            spSql = IOUtils.toString(new FileInputStream(file), "UTF-8");
            file = new File(path + "/fesql-auto-test-java/src/test/resources/xjd_table_ddl.txt");
            String tableDDL = IOUtils.toString(new FileInputStream(file), "UTF-8");
            String[] split = tableDDL.split(";");
            tableDDLList = Arrays.asList(split);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setZkSessionTimeout(10000);
        option.setRequestTimeout(60000);
        sqlExecutor = new SqlClusterExecutor(option);
    }

    public void initDDL() throws Exception {
        sqlExecutor.dropDB(db);
        sqlExecutor.createDB(db);
        for (String ddl : tableDDLList) {
            boolean ok = sqlExecutor.executeDDL(db, ddl + ";");
            Assert.assertTrue(ok);
        }
        System.out.println("init ddl finished");
    }

    public void createProcedure() throws Exception {
        boolean ok = sqlExecutor.executeDDL(db, spSql);
        Assert.assertTrue(ok);
    }

    public void callProcedureWithPstms() throws Exception {
        ProcedureInfo xjd10 = sqlExecutor.showProcedure(db, "xjd10");
        CallablePreparedStatement callablePreparedStmt = sqlExecutor.getCallablePreparedStmt(db, "xjd10");
        ResultSetMetaData metaData = callablePreparedStmt.getMetaData();
        if (setData(callablePreparedStmt, metaData, "bb")) return;
        ResultSet sqlResultSet = callablePreparedStmt.executeQuery();
        Assert.assertTrue(sqlResultSet.next());
        for (int i = 1; i < sqlResultSet.getMetaData().getColumnCount(); i++) {
            System.out.println("output column: " + i + ", " + sqlResultSet.getNString(i));
        }
        System.out.println("call ok");

        CallablePreparedStatement batchPsmt = sqlExecutor.getCallablePreparedStmtBatch(db, "xjd10");
        metaData = batchPsmt.getMetaData();
        metaData = batchPsmt.getMetaData();
        if (setData(batchPsmt, metaData, "bb")) return;
        batchPsmt.addBatch();
        if (setData(batchPsmt, metaData, "")) return;
        batchPsmt.addBatch();
        sqlResultSet = batchPsmt.executeQuery();
        Assert.assertTrue(sqlResultSet.next());

        System.out.println("call batch ok");
        sqlResultSet.close();
        callablePreparedStmt.close();
    }

    private boolean setData(CallablePreparedStatement callablePreparedStmt, ResultSetMetaData metaData, String strVal) throws SQLException {
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
                callablePreparedStmt.setLong(i + 1, Long.valueOf(300l));
            } else if (columnType == Types.FLOAT) {
                callablePreparedStmt.setFloat(i + 1, Float.valueOf(3.0f));
            } else if (columnType == Types.DOUBLE) {
                callablePreparedStmt.setDouble(i + 1, 1.0);
            } else if (columnType == Types.TIMESTAMP) {
                callablePreparedStmt.setTimestamp(i + 1, new Timestamp(1590738994000l));
            } else if (columnType == Types.DATE) {
                callablePreparedStmt.setDate(i + 1, Date.valueOf("2020-05-05"));
            } else if (columnType == Types.VARCHAR) {
                callablePreparedStmt.setString(i + 1, strVal);
            } else {
                logger.error("fail to build request row: invalid data type {]", columnType);
                return true;
            }
        }
        return false;
    }

    public static void run() {
        final ComplexSql3Example example = new ComplexSql3Example();
        try {
            example.init();
            System.out.println("init success");
            example.initDDL();
            example.createProcedure();
            example.callProcedureWithPstms();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        run();
    }

}
