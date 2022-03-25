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

package com._4paradigm.openmldb.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@Slf4j
public class JDBCDriverTest {
    @Test
    public void testSmoke() {
        String zk = "localhost:6181";
        String zkPath = "/onebox";
        try {
            Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
            // No database in jdbcUrl
            Connection connection = DriverManager.getConnection(String.format("jdbc:openmldb:///?zk=%s&zkPath=%s", zk, zkPath));
            Statement stmt = connection.createStatement();
            String dbName = "driver_test";
            String tableName = "connector_test";
            try {
                stmt.execute("create database if not exists " + dbName);
            } catch (Exception ignored) {
                Assert.fail();
            }
            try {
                stmt.execute(String.format("use %s", dbName));
                stmt.execute(String.format("create table if not exists %s(c1 int, c2 string)", tableName));
            } catch (Exception e) {
                // TODO(hw): support create table if not exists
                e.printStackTrace();
            }
            connection.close();
            // Set database in jdbcUrl
            connection = DriverManager.getConnection(String.format("jdbc:openmldb:///%s?zk=%s&zkPath=%s", dbName, zk, zkPath));
            // all pulsar jdbc sink connector will do
            connection.setAutoCommit(false); // useless but shouldn't be failed
            connection.getAutoCommit();

            java.sql.DatabaseMetaData metadata = connection.getMetaData();

            String catalogName = null, schemaName = null;
            try (ResultSet rs = metadata.getTables(null, null, tableName, new String[]{"TABLE"})) {
                if (rs.next()) {
                    catalogName = rs.getString(1);
                    schemaName = rs.getString(2);
                    String gotTableName = rs.getString(3);
                    Assert.assertEquals(gotTableName, tableName, "TableName not match: " + tableName + " Got: " + gotTableName);
                } else {
                    Assert.fail("Not able to find table: " + tableName);
                }
            }
            List<String> keyList = new ArrayList<>(), nonKeyList = new ArrayList<>();
            String[][] expected = {{"c1", "INTEGER", "1"}, {"c2", "VARCHAR", "2"}};
            List<String> columns = new ArrayList<>();
            try (ResultSet rs = connection.getMetaData().getColumns(
                    catalogName,
                    schemaName,
                    tableName,
                    null
            )) {
                while (rs.next()) {
                    final String columnName = rs.getString(4);
//                    final int sqlDataType = rs.getInt(5);
                    final String typeName = rs.getString(6);
                    final int position = rs.getInt(17);

                    Assert.assertEquals(columnName, expected[position - 1][0]);
                    Assert.assertEquals(typeName, expected[position - 1][1]);
                    Assert.assertEquals(String.valueOf(position), expected[position - 1][2]);
                    columns.add(columnName);
                }
            }

            // build insert into sql
            StringBuilder insertSQLBuilder = new StringBuilder();
            insertSQLBuilder.append("INSERT INTO ");
            insertSQLBuilder.append(tableName);
            insertSQLBuilder.append("(");
            columns.forEach(colName -> insertSQLBuilder.append(colName).append(","));
            insertSQLBuilder.deleteCharAt(insertSQLBuilder.length() - 1);
            insertSQLBuilder.append(") VALUES(");
            IntStream.range(0, columns.size() - 1).forEach(i -> insertSQLBuilder.append("?,"));
            insertSQLBuilder.append("?)");
            PreparedStatement insertStatement = connection.prepareStatement(insertSQLBuilder.toString());
            log.info(insertSQLBuilder.toString());
            for (int i = 0; i < 10; i++) {
                insertStatement.setInt(1, 1);
                insertStatement.setString(2, "a");
                // execute immediately
                Assert.assertTrue(insertStatement.execute());
            }
            try {
                String updateSQL = "UPDATE table ...";
                connection.prepareStatement(updateSQL);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(e.getMessage(), "unsupported sql");
            }
            try {
                String deleteSQL = "DELETE FROM table ...";
                connection.prepareStatement(deleteSQL);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(e.getMessage(), "unsupported sql");
            }

            // useless but won't fail
            connection.commit();
            connection.close();
            Assert.assertTrue(connection.isClosed());
            // double-close is ok
            connection.close();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail("jdbc connection failed");
        }
    }
}
