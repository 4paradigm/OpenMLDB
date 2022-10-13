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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Slf4j
public class JDBCDriverTest {
    private Connection connection;
    private final String dbName = "driver_test";
    String zk = TestConfig.ZK_CLUSTER;
    String zkPath = TestConfig.ZK_PATH;

    @BeforeTest
    public void connection() {
        try {

            Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
            // No database in jdbcUrl, and print zk log
            connection = DriverManager.getConnection(String.format("jdbc:openmldb:///?zk=%s&zkPath=%s", zk, zkPath));
            Statement stmt = connection.createStatement();
            try {
                stmt.execute("create database if not exists " + dbName);
            } catch (Exception ignored) {
                Assert.fail();
            }

            connection.close();
            // Set database in jdbcUrl, so we don't need to execute 'use db', no zk log
            connection = DriverManager.getConnection(
                    String.format("jdbc:openmldb:///%s?zk=%s&zkPath=%s&zkLogLevel=0", dbName, zk, zkPath));
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
            Assert.fail("jdbc connection failed");
        }
    }

    @AfterTest
    public void close() throws SQLException {
        if (!connection.isClosed()) {
            connection.close();
        }
        Assert.assertTrue(connection.isClosed());
    }

    @Test
    public void testAllOptionsInUrl() throws Exception {
        connection = DriverManager.getConnection(String.format(
                "jdbc:openmldb:///%s?zk=%s&zkPath=%s&zkLogFile=&glogDir=&requestTimeout=100000&maxSqlCacheSize=100", dbName, zk, zkPath));

        log.info("can't see log below");
        connection = DriverManager.getConnection(String
                .format("jdbc:openmldb:///%s?zk=%s&zkPath=%s&zkLogLevel=0&glogLevel=1", dbName, zk, zkPath));
    }

    @Test
    public void testForPulsarConnector() throws SQLException {
        String tableName = "pulsar_test";

        Statement stmt = connection.createStatement();
        try {
            stmt.execute(String.format("create table if not exists %s(c1 int, c2 string)", tableName));
        } catch (Exception e) {
            Assert.fail();
        }
        // all pulsar jdbc sink connector will do
        connection.setAutoCommit(false); // useless but shouldn't be failed
        connection.getAutoCommit();

        java.sql.DatabaseMetaData metadata = connection.getMetaData();

        String catalogName = null, schemaName = null;
        try (ResultSet rs = metadata.getTables(null, null, tableName, new String[] { "TABLE" })) {
            if (rs.next()) {
                catalogName = rs.getString(1);
                schemaName = rs.getString(2);
                String gotTableName = rs.getString(3);
                Assert.assertEquals(gotTableName, tableName,
                        "TableName not match: " + tableName + " Got: " + gotTableName);
            } else {
                Assert.fail("Not able to find table: " + tableName);
            }
        }
        String[][] expected = { { "c1", "INTEGER", "1" }, { "c2", "VARCHAR", "2" } };
        List<String> columns = new ArrayList<>();
        try (ResultSet rs = connection.getMetaData().getColumns(
                catalogName,
                schemaName,
                tableName,
                null)) {
            while (rs.next()) {
                final String columnName = rs.getString(4);
                // final int sqlDataType = rs.getInt(5);
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
            String deleteSQL = "DELETE FROM " + tableName + " WHERE c1 = ?";
            PreparedStatement deleteStatement = connection.prepareStatement(deleteSQL);
            deleteStatement.setInt(1, 1);
            deleteStatement.execute();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // useless but won't fail
        connection.commit();
        connection.close();
        Assert.assertTrue(connection.isClosed());
        // double-close is ok
        connection.close();
    }

    @Test
    public void testForKafkaConnector() throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        log.info("{}-{}-{}-{}-{}",
                metadata.getJDBCMajorVersion(),
                metadata.getJDBCMinorVersion(),
                metadata.getDriverName(),
                metadata.getDatabaseProductName(),
                metadata.getDatabaseProductVersion());

        // if major version >= 4, must support isValid()
        Assert.assertFalse(metadata.getJDBCMajorVersion() >= 4);
        Assert.assertFalse(connection.isValid(0), "unsupported now in 1.4");
        Statement stmt = connection.createStatement();
        Assert.assertTrue(stmt.execute("SELECT 1"));
        {
            ResultSet rs = stmt.getResultSet();
            // do nothing with the result set
            rs.close();
        }

        String tableName = "kafka_test";
        stmt = connection.createStatement();
        try {
            stmt.execute(String.format("create table if not exists %s(c1 int, c2 string, c3 timestamp)", tableName));
        } catch (Exception e) {
            Assert.fail();
        }

        // only support insert sql
        // Quote `, ID_DELIM .
        String insertSql = "INSERT INTO " +
                tableName +
                "(`c1`,`c2`) VALUES(?,?)";
        PreparedStatement pstmt = connection.prepareStatement(insertSql);
        // don't work, but do not throw exception
        pstmt.setFetchSize(100);

        pstmt.addBatch();
        insertSql = "INSERT INTO " +
                tableName +
                "(`c3`,`c2`) VALUES(?,?)";
        pstmt = connection.prepareStatement(insertSql);
        Assert.assertEquals(pstmt.getMetaData().getColumnCount(), 2);
        // index starts from 1
        Assert.assertEquals(pstmt.getMetaData().getColumnType(2), Types.VARCHAR);
        Assert.assertEquals(pstmt.getMetaData().getColumnName(2), "c2");

        try {
            PreparedStatement preparedStatement = connection
                    .prepareStatement("DELETE FROM " + tableName + " WHERE c1=?");
            preparedStatement.setInt(1, 1);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // sink, catalog and schema patterns are always be null
        // OpenMLDB only has table, no view
        List<String> typeResults = new ArrayList<>();
        try (ResultSet rs = metadata.getTableTypes()) {
            while (rs.next()) {
                String tableType = rs.getString(1);
                typeResults.add(tableType);
            }
        }
        Assert.assertEquals(typeResults.size(), 1);
        Assert.assertEquals(typeResults.get(0), "TABLE");

        // if the arg tableTypes in `getTables` has 'VIEW', no effect
        List<String> tableResults = new ArrayList<>();
        try (ResultSet rs = metadata.getTables(null, null, "%", new String[] { "TABLE", "VIEW" })) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tName = rs.getString(3);
                // "null" string, not the null
                Assert.assertNull(catalogName);
                Assert.assertNull(schemaName);
                log.info("get table {}", tName);
                tableResults.add(tName);
            }
        }
        Assert.assertTrue(tableResults.contains(tableName));

        Assert.assertEquals(metadata.getIdentifierQuoteString(), "`");
        Assert.assertEquals(metadata.getCatalogSeparator(), ".");

        // kafka currentTimeOnDB, only source needs it?
        String timeSql = "SELECT CURRENT_TIMESTAMP";

        // no isolation
        Assert.assertFalse(metadata.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
        try {
            connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
            Assert.fail();
        } catch (Exception ignored) {

        }

        // won't work
        connection.setAutoCommit(true);

        // one table, not table pattern
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(
                null, null, tableName)) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tName = rs.getString(3);
                final String colName = rs.getString(4);
            }
        }

        try (ResultSet rs = connection.getMetaData().getColumns(
                null,
                null,
                tableName,
                null)) {
            final int rsColumnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String tName = rs.getString(3);
                final String columnName = rs.getString(4);
                final int jdbcType = rs.getInt(5);
                final String typeName = rs.getString(6);
                final int precision = rs.getInt(7);
                final int scale = rs.getInt(9);
                final String typeClassName = null;
                final int nullableValue = rs.getInt(11);
                Boolean autoIncremented = null;
                if (rsColumnCount >= 23) {
                    // Not all drivers include all columns ...
                    String isAutoIncremented = rs.getString(23);
                    Assert.assertTrue("no".equalsIgnoreCase(isAutoIncremented));
                }
            }
        }

    }
}
