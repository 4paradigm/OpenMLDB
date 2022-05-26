/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DropOptions;

public final class DatabaseHelper {

  public interface ResultSetReadCallback {
    void read(final ResultSet rs) throws SQLException;
  }

  public final DatabaseDialect dialect;
  private Connection connection;

  public DatabaseHelper(final DatabaseDialect dialect) {
    this.dialect = dialect;
  }

  public void setUp() throws SQLException, IOException {
    connection = dialect.getConnection();
  }

  public void tearDown() throws SQLException, IOException {
    try {
      if (connection != null) {
        connection.close();
      }
    } finally {
      if (dialect != null) {
        dialect.close();
      }
    }
  }

  public void createTable(final String createSql) throws SQLException {
    execute(createSql);
  }

  public void deleteTable(final TableId tableId) throws SQLException {
    deleteTable(tableId, new DropOptions().setIfExists(true));
  }

  public void deleteTable(final TableId tableId, DropOptions options) throws SQLException {
    String sql = dialect.buildDropTableStatement(tableId, options);
    execute(sql);

    //random errors of table not being available happens in the unit tests
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public int select(final String query, final DatabaseHelper.ResultSetReadCallback callback) throws SQLException {
    int count = 0;
    try (Statement stmt = connection.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          callback.read(rs);
          count++;
        }
      }
    }
    return count;
  }

  public void execute(String sql) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(sql);
      if (!connection.getAutoCommit()) connection.commit();
    }
  }

}
