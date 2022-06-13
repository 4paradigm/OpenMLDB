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
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.sql.*;
import java.util.Random;

public class ComplexSql2Example extends BaseExample {
    private static final Logger logger = LoggerFactory.getLogger(ComplexSql2Example.class);

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

    String spSql = "CREATE PROCEDURE sp(`label` int32,`s1` string,`s2` string,`t1` timestamp,`t2` date,`d1` float,`d2` double,`c1` int32,`c2` int64,`ai` string,`kn` string,`ks` string)\n" +
            "BEGIN\n" +
            "# start sql code\n" +
            "# output table name: sql_table\n" +
            "\n" +
            "select\n" +
            "    c1 as c1_1,\n" +
            "    distinct_count(d2) over table_2_s1_t1_0s_1d as table_2_d2_0,\n" +
            "    case when !isnull(at(s2, 1)) over table_2_s1_t1_0_10 then count(s2) over table_2_s1_t1_0_10 else null end as table_2_s2_0\n" +
            "from\n" +
            "    (select s2 as s1, '' as s2, t1 as t1, date('2019-07-18') as t2, float(0) as d1, double(0) as d2, int(0) as c1, bigint(0) as c2, '' as ai, '' as kn, '' as ks from main)\n" +
            "    window table_2_s1_t1_0s_1d as (\n" +
            "UNION table_2 partition by s1 order by t1 rows_range between 1d preceding and 0s preceding INSTANCE_NOT_IN_WINDOW),\n" +
            "    table_2_s1_t1_0_10 as (\n" +
            "UNION table_2 partition by s1 order by t1 rows_range between 10 preceding and 0 preceding INSTANCE_NOT_IN_WINDOW);\n" +
            "END;";

    private SqlExecutor sqlExecutor = null;
    private String db = "test";

    public void init() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setZkSessionTimeout(10000);
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
        CallablePreparedStatement callablePreparedStmt = sqlExecutor.getCallablePreparedStmt(db, "sp");
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

//        Assert.assertEquals(sqlResultSet.getMetaData().getColumnCount(), 3);
//        Assert.assertEquals(sqlResultSet.getString(1), "bb");
//        Assert.assertEquals(sqlResultSet.getInt(2), 24);
//        Assert.assertEquals(sqlResultSet.getLong(3), 34);
        CallablePreparedStatement batchCallablePreparedStatement = sqlExecutor.getCallablePreparedStmtBatch(db, "sp");
        System.out.println("call ok");
        sqlResultSet.close();
        callablePreparedStmt.close();
    }

    public static void run() {
        final ComplexSql2Example example = new ComplexSql2Example();
        try {
            example.init();
            System.out.println("init success");
//            example.initDDL();
//            example.createProcedure();
            example.callProcedureWithPstms();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        run();
    }

}
