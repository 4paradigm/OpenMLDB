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
import com._4paradigm.openmldb.sdk.*;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.testng.Assert;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Demo {

    private SqlExecutor sqlExecutor = null;
    private String db = "mydb16";
    private String table = "trans";
    private String sp = "sp";

    public static void main(String[] args) {
        Demo demo = new Demo();
        try {
            demo.init();
            demo.createDataBase();
            demo.createTable();

            demo.insertWithoutPlaceholder();
            demo.insertWithPlaceholder();
            demo.select();
            demo.requestSelect();
            demo.batchRequestSelect();
            demo.createProcedure();
            demo.callProcedureSync();
            demo.callProcedureAsync();
            demo.batchCallProcedureSync();
            demo.batchCallProcedureAsync();

            demo.dropProcedure();
            demo.dropTable();
            demo.dropDataBase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkCluster("172.27.128.37:7181");
        option.setZkPath("/rtidb_wb");
        option.setSessionTimeout(10000);
        option.setRequestTimeout(60000);
        sqlExecutor = new SqlClusterExecutor(option);
    }

    private void createDataBase() {
        Assert.assertTrue(sqlExecutor.createDB(db));
    }

    private void dropDataBase() {
        Assert.assertTrue(sqlExecutor.dropDB(db));
    }

    private void createTable() {
        String createTableSql = "create table trans(c1 string,\n" +
                "                   c3 int,\n" +
                "                   c4 bigint,\n" +
                "                   c5 float,\n" +
                "                   c6 double,\n" +
                "                   c7 timestamp,\n" +
                "                   c8 date,\n" +
                "                   index(key=c1, ts=c7));";
        Assert.assertTrue(sqlExecutor.executeDDL(db, createTableSql));
    }

    private void dropTable() {
        String dropTableSql = "drop table trans;";
        Assert.assertTrue(sqlExecutor.executeDDL(db, dropTableSql));
    }

    private void getInputSchema(String selectSql) {
        try {
            Schema inputSchema = sqlExecutor.getInputSchema(db, selectSql);
            Assert.assertEquals(inputSchema.getColumnList().size(), 7);
            Column column = inputSchema.getColumnList().get(0);
            Assert.assertEquals(column.getColumnName(), "c1");
            Assert.assertEquals(column.getSqlType(), Types.VARCHAR);
            Assert.assertEquals(column.isConstant(), false);
            Assert.assertEquals(column.isNotNull(), false);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private void showProcedure(String spSql) {
        try {
            ProcedureInfo procedureInfo = sqlExecutor.showProcedure(db, sp);
            Assert.assertEquals(procedureInfo.getDbName(), db);
            Assert.assertEquals(procedureInfo.getProName(), sp);
            Assert.assertEquals(procedureInfo.getSql(), spSql);
            Assert.assertEquals(procedureInfo.getMainTable(), table);
            Assert.assertEquals(procedureInfo.getInputTables().size(), 1);
            Assert.assertEquals(procedureInfo.getInputTables().get(0), table);
            Assert.assertEquals(procedureInfo.getInputSchema().getColumnList().size(), 7);
            Assert.assertEquals(procedureInfo.getOutputSchema().getColumnList().size(), 3);
            Column column = procedureInfo.getInputSchema().getColumnList().get(0);
            Assert.assertEquals(column.getColumnName(), "c1");
            Assert.assertEquals(column.getSqlType(), Types.VARCHAR);
            Assert.assertEquals(column.isConstant(), true);
            Assert.assertEquals(column.isNotNull(), false);
            Column column1 = procedureInfo.getOutputSchema().getColumnList().get(0);
            Assert.assertEquals(column1.getColumnName(), "c1");
            Assert.assertEquals(column1.getSqlType(), Types.VARCHAR);
            Assert.assertEquals(column1.isConstant(), false);
            Assert.assertEquals(column1.isNotNull(), false);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private void createProcedure() {
        String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        getInputSchema(selectSql);
        String createProcedureSql = "create procedure " + sp +
                "(const c1 string, const c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date)" +
                " begin " + selectSql + " end;";
        Assert.assertTrue(sqlExecutor.executeDDL(db, createProcedureSql));
        showProcedure(createProcedureSql);
    }

    private void dropProcedure() {
        String dropSpSql = "drop procedure sp;";
        Assert.assertTrue(sqlExecutor.executeDDL(db, dropSpSql));
    }

    private void insertWithoutPlaceholder() {
        String insertSql = "insert into trans values(\"aa\",23,33,1.4,2.4,1590738993000,\"2020-05-04\");";
        PreparedStatement pstmt = null;
        try {
            pstmt = sqlExecutor.getInsertPreparedStmt(db, insertSql);
            Assert.assertTrue(pstmt.execute());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    private void insertWithPlaceholder() {
        String insertSql = "insert into trans values(\"aa\", ?, 33, ?, 2.4, 1590738993000, \"2020-05-04\");";
        PreparedStatement pstmt = null;
        try {
            pstmt = sqlExecutor.getInsertPreparedStmt(db, insertSql);
            ResultSetMetaData metaData = pstmt.getMetaData();
            setData(pstmt, metaData);
            Assert.assertTrue(pstmt.execute());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    private void select() {
        String selectSql = "select * from trans;";
        com._4paradigm.openmldb.ResultSet result = (com._4paradigm.openmldb.ResultSet)sqlExecutor.executeSQL(db, selectSql);
        Assert.assertEquals(result.Size(), 2);
    }

    private void requestSelect() {
        String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            pstmt = sqlExecutor.getRequestPreparedStmt(db, selectSql);
            ResultSetMetaData metaData = pstmt.getMetaData();
            setData(pstmt, metaData);
            resultSet = pstmt.executeQuery();

            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertFalse(resultSet.next());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    private void batchRequestSelect() {
        String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            List<Integer> idx_list = new ArrayList<>();
            idx_list.add(0);
            idx_list.add(1);
            pstmt = sqlExecutor.getBatchRequestPreparedStmt(db, selectSql, idx_list);
            ResultSetMetaData metaData = pstmt.getMetaData();
            setData(pstmt, metaData);
            pstmt.addBatch();
            setData(pstmt, metaData);
            pstmt.addBatch();
            resultSet = pstmt.executeQuery();

            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertFalse(resultSet.next());
            Assert.assertFalse(resultSet.next());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    private void callProcedureSync() {
        CallablePreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            // pstmt can be reused
            // while:
            //    setData
            //    executeQuery
            pstmt = sqlExecutor.getCallablePreparedStmt(db, sp);
            ResultSetMetaData metaData = pstmt.getMetaData();
            setData(pstmt, metaData);
            resultSet = pstmt.executeQuery();

            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertFalse(resultSet.next());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    private void callProcedureAsync() {
        CallablePreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            pstmt = sqlExecutor.getCallablePreparedStmt(db, sp);
            ResultSetMetaData metaData = pstmt.getMetaData();
            setData(pstmt, metaData);
            QueryFuture future = pstmt.executeQueryAsync(1000, TimeUnit.MILLISECONDS);
            resultSet = future.get();
            Assert.assertTrue(future.isDone());

            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertFalse(resultSet.next());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    private void batchCallProcedureSync() {
        CallablePreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            pstmt = sqlExecutor.getCallablePreparedStmtBatch(db, sp);
            ResultSetMetaData metaData = pstmt.getMetaData();
            setData(pstmt, metaData);
            pstmt.addBatch();
            setData(pstmt, metaData);
            pstmt.addBatch();
            resultSet = pstmt.executeQuery();

            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertFalse(resultSet.next());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    private void batchCallProcedureAsync() {
        CallablePreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            pstmt = sqlExecutor.getCallablePreparedStmtBatch(db, sp);
            ResultSetMetaData metaData = pstmt.getMetaData();
            setData(pstmt, metaData);
            pstmt.addBatch();
            setData(pstmt, metaData);
            pstmt.addBatch();
            QueryFuture future = pstmt.executeQueryAsync(1000, TimeUnit.MILLISECONDS);
            resultSet = future.get();
            Assert.assertTrue(future.isDone());

            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertFalse(resultSet.next());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }


    private void setData(PreparedStatement pstmt, ResultSetMetaData metaData) throws SQLException {
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.BOOLEAN) {
                pstmt.setBoolean(i + 1, true);
            } else if (columnType == Types.SMALLINT) {
                pstmt.setShort(i + 1, (short) 22);
            } else if (columnType == Types.INTEGER) {
                pstmt.setInt(i + 1, 24);
            } else if (columnType == Types.BIGINT) {
                pstmt.setLong(i + 1, 34l);
            } else if (columnType == Types.FLOAT) {
                pstmt.setFloat(i + 1, 1.5f);
            } else if (columnType == Types.DOUBLE) {
                pstmt.setDouble(i + 1, 2.5);
            } else if (columnType == Types.TIMESTAMP) {
                pstmt.setTimestamp(i + 1, new Timestamp(1590738994000l));
            } else if (columnType == Types.DATE) {
                pstmt.setDate(i + 1, Date.valueOf("2020-05-05"));
            } else if (columnType == Types.VARCHAR) {
                pstmt.setString(i + 1, "bb");
            } else {
                throw new SQLException("set data failed");
            }
        }
    }
}
