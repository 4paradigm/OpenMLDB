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

package com.openmldb.demo;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

public class App {
    private SqlExecutor sqlExecutor = null;
    private String zkCluster = "127.0.0.1:2181";
    private String zkPath = "/openmldb";
    private String db = "mydb16";
    private String table = "trans";
    private String deploymentName = "d1";

    public static void main(String[] args) {
        // 如果中途出现异常，将无法清理掉创建的数据库和表，需要手动清理，否则重试将会与预期不符
        App demo = new App();
        try {
            // 初始化构造 SqlExecutor
            demo.init();
            demo.createDatabase();
            // set online and default db in SqlExecutor, no need to set in the below sql
            demo.setOnlineAndDB();

            demo.createTable();
            demo.createDeployment();
            // 通过insert语句插入
            demo.insertWithoutPlaceholder();
            // 通过placeholder的方式插入。placeholder方式不会重复编译sql, 在性能上会比直接insert好很多
            demo.insertWithPlaceholder();
            // 执行select语句
            demo.select();
            // 在request模式下执行sql
            demo.requestSelect();
            // 执行 deployment
            demo.executeDeployment();
            demo.dropDeployment();
            // 删除表
            demo.dropTable();
            // 删除数据库
            demo.dropDatabase();
            System.out.println("Succeed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setSessionTimeout(10000);
        option.setRequestTimeout(60000);
        // sqlExecutor执行sql操作是多线程安全的，在实际环境中只创建一个即可
        sqlExecutor = new SqlClusterExecutor(option);
    }

    private void createDatabase() {
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            state.execute("create database " + db + ";");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dropDatabase() {
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            state.execute("drop database " + db + ";");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setOnlineAndDB() {
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            state.execute("SET @@execute_mode='online';");
            state.execute("use " + db + ";");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createTable() {
        String createTableSql = "create table " + table + "(c1 string,\n" +
                "                   c3 int,\n" +
                "                   c4 bigint,\n" +
                "                   c5 float,\n" +
                "                   c6 double,\n" +
                "                   c7 timestamp,\n" +
                "                   c8 date,\n" +
                "                   index(key=c1, ts=c7));";
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            state.execute(createTableSql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dropTable() {
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            state.execute("drop table " + table);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createDeployment() {
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            String selectSql = String.format("SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM %s WINDOW w1 AS " +
                    "(PARTITION BY %s.c1 ORDER BY %s.c7 ROWS_RANGE BETWEEN 2d PRECEDING AND CURRENT ROW);", table,
                    table, table);
            // 上线一个Deployment
            String deploySql = String.format("DEPLOY %s OPTIONS(RANGE_BIAS='inf', ROWS_BIAS='inf') %s", deploymentName, selectSql);
            // set return null rs, don't check the returned value, it's false
            state.execute(deploySql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dropDeployment() {
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            state.execute("DROP DEPLOYMENT " + deploymentName + ";");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertWithoutPlaceholder() {
        String insertSql = String.format("insert into %s values(\"aa\",23,33,1.4,2.4,1590738993000,\"2020-05-04\");",
                table);
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
                    // PrepareStatement用完之后必须close
                    pstmt.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    private void insertWithPlaceholder() {
        String insertSql = String.format("insert into %s values(\"aa\", ?, 33, ?, 2.4, 1590738993000, \"2020-05-04\");",
                table);
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
        String selectSql = "select * from " + table;
        java.sql.ResultSet result = null;
        int num = 0;
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            boolean ret = state.execute(selectSql);
            Assert.assertTrue(ret);
            result = state.getResultSet();
            while (result.next()) {
                num++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (result != null) {
                    result.close();
                }
                state.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

        }
        // result数据解析参考下面requestSelect方法
        Assert.assertEquals(num, 2);
    }

    private void requestSelect() {
        String selectSql = String.format("SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM %s WINDOW w1 AS " +
                "(PARTITION BY %s.c1 ORDER BY %s.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);", table, table, table);
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            pstmt = sqlExecutor.getRequestPreparedStmt(db, selectSql);
            // 如果是执行deployment, 可以通过名字获取preparedstatement
            // pstmt = sqlExecutor.getCallablePreparedStmt(db, deploymentName);
            ResultSetMetaData metaData = pstmt.getMetaData();
            // 执行request模式需要在RequestPreparedStatement设置一行请求数据
            setData(pstmt, metaData);
            // 调用executeQuery会执行这个select sql, 然后将结果放在了resultSet中
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
                    // result用完之后需要close
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

    private void executeDeployment() {
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            pstmt = sqlExecutor.getCallablePreparedStmt(db, deploymentName);
            // 如果是执行deployment, 可以通过名字获取preparedstatement
            // pstmt = sqlExecutor.getCallablePreparedStmt(db, deploymentName);
            ResultSetMetaData metaData = pstmt.getMetaData();
            // 执行request模式需要在RequestPreparedStatement设置一行请求数据
            setData(pstmt, metaData);
            // 调用executeQuery会执行这个select sql, 然后将结果放在了resultSet中
            resultSet = pstmt.executeQuery();

            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
            Assert.assertEquals(resultSet.getString(1), "bb");
            Assert.assertEquals(resultSet.getInt(2), 24);
            Assert.assertEquals(resultSet.getLong(3), 34);
            Assert.assertFalse(resultSet.next());

            // reuse way
            for (int i = 0; i < 5; i++) {
                setData(pstmt, metaData);
                pstmt.executeQuery();
                // skip result check
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    // result用完之后需要close
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
        String selectSql = String.format("SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM %s WINDOW w1 AS " +
                "(PARTITION BY %s.c1 ORDER BY %s.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);", table, table, table);
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            List<Integer> list = new ArrayList<Integer>();
            pstmt = sqlExecutor.getBatchRequestPreparedStmt(db, selectSql, list);
            // 如果是执行deployment, 可以通过名字获取preparedstatement
            // pstmt = sqlExecutor.getCallablePreparedStmtBatch(db, deploymentName);
            ResultSetMetaData metaData = pstmt.getMetaData();
            // 执行request模式需要在设置PreparedStatement请求数据
            // 设置一个batch发送多少条数据
            int batchSize = 5;
            for (int idx = 0; idx < batchSize; idx++) {
                setData(pstmt, metaData);
                // 每次设置完一行数据后需要调用一次addBatch
                pstmt.addBatch();
            }
            // 调用executeQuery会执行这个select sql, 然后将结果放在了resultSet中
            resultSet = pstmt.executeQuery();
            // 依次取出每一条数据对应的特征结果
            while (resultSet.next()) {
                Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
                Assert.assertEquals(resultSet.getString(1), "bb");
                Assert.assertEquals(resultSet.getInt(2), 24);
                Assert.assertEquals(resultSet.getLong(3), 34);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            try {
                if (resultSet != null) {
                    // result用完之后需要close
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
