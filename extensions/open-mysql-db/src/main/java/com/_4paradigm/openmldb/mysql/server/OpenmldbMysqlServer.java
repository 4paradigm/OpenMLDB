package com._4paradigm.openmldb.mysql.server;

import cn.paxos.mysql.MySqlListener;
import cn.paxos.mysql.ResultSetWriter;
import cn.paxos.mysql.engine.QueryResultColumn;
import cn.paxos.mysql.engine.SqlEngine;
import cn.paxos.mysql.util.SHAUtils;
import cn.paxos.mysql.util.Utils;
import com._4paradigm.openmldb.common.Pair;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.mysql.util.TypeUtil;
import com._4paradigm.openmldb.mysql.mock.MockResult;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com.google.common.base.Strings;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OpenmldbMysqlServer {
  private final Map<Integer, SqlClusterExecutor> sqlClusterExecutorMap = new ConcurrentHashMap<>();

  private final Pattern showFullColumnsPattern =
      Pattern.compile("(?i)SHOW FULL COLUMNS FROM `(.+)` FROM `(.+)`");

  //  private final Pattern crateTableResultPattern =
  //      Pattern.compile(
  //          "(?i)(?s)(?m)CREATE TABLE `(.+)` \\([\\s\\r\\n]+(?:`(.+)` (.+),[\\s\\r\\n]+)+.*\\)
  // OPTIONS .*");

  public OpenmldbMysqlServer(int port, String zkCluster, String zkRootPath) {
    new MySqlListener(
        port,
        100,
        new SqlEngine() {
          @Override
          public void authenticate(
              int connectionId,
              String database,
              String userName,
              byte[] scramble411,
              byte[] authSeed)
              throws IOException {

            // mocked username
            String validUser = ServerConfig.getOpenmldbUser();
            if (!userName.equals(validUser)) {
              throw new IOException(
                  new IllegalAccessException(
                      "Authentication failed: User " + userName + " is not allowed to connect"));
            }
            // mocked password
            String validPassword = ServerConfig.getOpenmldbPassword();

            String validPasswordSha1 = SHAUtils.SHA(validPassword, SHAUtils.SHA_1);
            String validScramble411WithSeed20 = Utils.scramble411(validPasswordSha1, authSeed);

            if (!Utils.compareDigest(
                validScramble411WithSeed20, Base64.getEncoder().encodeToString(scramble411))) {
              throw new IOException(
                  new IllegalAccessException("Authentication failed: Validation failed"));
            }

            try {
              if (!sqlClusterExecutorMap.containsKey(connectionId)) {
                synchronized (this) {
                  if (!sqlClusterExecutorMap.containsKey(connectionId)) {
                    SdkOption option = new SdkOption();
                    option.setZkCluster(zkCluster);
                    option.setZkPath(zkRootPath);
                    // TODO: Make these configurable
                    option.setSessionTimeout(10000);
                    option.setRequestTimeout(60000);
                    option.setUser(userName);
                    option.setPassword(validPassword);
                    SqlClusterExecutor sqlExecutor = new SqlClusterExecutor(option);
                    try {
                      System.out.println(
                          "Try to set default execute mode online, Database: "
                              + database
                              + ", User: "
                              + userName);
                      java.sql.Statement stmt = sqlExecutor.getStatement();
                      stmt.execute("SET @@execute_mode='online'");
                      stmt.close();
                    } catch (SQLException e) {
                      e.printStackTrace();
                    }
                    sqlClusterExecutorMap.put(connectionId, sqlExecutor);
                  }
                }
              }
            } catch (SqlException e) {
              throw new IOException(e);
            }
          }

          @Override
          public void query(
              int connectionId,
              ResultSetWriter resultSetWriter,
              String database,
              String userName,
              byte[] scramble411,
              byte[] authSeed,
              String sql)
              throws IOException {
            // Print useful information
            System.out.println("Try to execute query, Database: " + database + ", User: "
                    + userName + ", SQL: " + sql);

            this.authenticate(connectionId, database, userName, scramble411, authSeed);

            try {
              if (MockResult.mockResults.containsKey(sql.toLowerCase())) {
                Pair<List<QueryResultColumn>, List<List<String>>> pair =
                    MockResult.mockResults.get(sql.toLowerCase());
                resultSetWriter.writeColumns(pair.getKey());
                for (List<String> row : pair.getValue()) {
                  resultSetWriter.writeRow(row);
                }
                resultSetWriter.finish();
              } else {
                for (String patternStr : MockResult.mockPatternResults.keySet()) {
                  Pattern pattern = Pattern.compile(patternStr);
                  if (pattern.matcher(sql).matches()) {
                    Pair<List<QueryResultColumn>, List<List<String>>> pair =
                        MockResult.mockPatternResults.get(patternStr);
                    resultSetWriter.writeColumns(pair.getKey());
                    for (List<String> row : pair.getValue()) {
                      resultSetWriter.writeRow(row);
                    }
                    resultSetWriter.finish();
                    return;
                  }
                }

                java.sql.Statement stmt = sqlClusterExecutorMap.get(connectionId).getStatement();

                Matcher showFullColumnsMatcher = showFullColumnsPattern.matcher(sql);
                if (showFullColumnsMatcher.matches()) {
                  String tableName = showFullColumnsMatcher.group(1);
                  String databaseName = showFullColumnsMatcher.group(2);
                  String selectStarSql = "select * from `" + databaseName + "`.`" + tableName + "`";
                  stmt.execute(selectStarSql);

                  List<QueryResultColumn> columns = new ArrayList<>();
                  // # Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment
                  // id, int, , NO, PRI, , auto_increment, select,insert,update,references,
                  columns.add(new QueryResultColumn("Field", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("Type", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("Collation", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("Null", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("Key", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("Default", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("Extra", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("Privileges", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("Comment", "VARCHAR(255)"));
                  resultSetWriter.writeColumns(columns);

                  SQLResultSet resultSet = (SQLResultSet) stmt.getResultSet();
                  Schema schema = resultSet.GetInternalSchema();
                  // int columnCount = schema.GetColumnCnt();
                  int columnCount = schema.getColumnList().size();
                  // Add schema
                  for (int i = 0; i < columnCount; i++) {
                    List<String> row = new ArrayList<>();
                    row.add(schema.getColumnName(i));
                    row.add(TypeUtil.openmldbTypeToMysqlTypeString(schema.getColumnType(i)));
                    row.add("");
                    row.add("");
                    row.add("");
                    row.add("");
                    row.add("");
                    row.add("");
                    row.add("");
                    resultSetWriter.writeRow(row);
                  }
                  resultSetWriter.finish();
                  return;
                }

                if (!Strings.isNullOrEmpty(database)) {
                  stmt.execute("use " + database);
                }
                if (sql.equalsIgnoreCase("SHOW FULL TABLES")) {
                  sql = "SHOW TABLES";
                }
                stmt.execute(sql);

                if (sql.toLowerCase().startsWith("select")
                    || sql.toLowerCase().startsWith("show")) {
                  SQLResultSet resultSet = (SQLResultSet) stmt.getResultSet();
                  outputResultSet(resultSetWriter, resultSet, sql);
                }

                System.out.println("Success to execute OpenMLDB SQL: " + sql);

                // Close resources
                stmt.close();
              }
            } catch (Exception e) {
              e.printStackTrace();
              throw new IOException(e.getMessage());
            }
          }

          @Override
          public void close(int connectionId) {
            sqlClusterExecutorMap.remove(connectionId);
          }
        });
  }

  public static void main(String[] args) {
    int serverPort = ServerConfig.getPort();
    String zkCluster = ServerConfig.getZkCluster();
    String zkRootPath = ServerConfig.getZkRootPath();

    try {
      OpenmldbMysqlServer server = new OpenmldbMysqlServer(serverPort, zkCluster, zkRootPath);
      server.start();
      // Conenct with mysql client, mysql -h127.0.0.1 -P3307
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void outputResultSet(ResultSetWriter resultSetWriter, SQLResultSet resultSet, String sql)
      throws SQLException {
    // Build and respond columns
    List<QueryResultColumn> columns = new ArrayList<>();

    Schema schema = resultSet.GetInternalSchema();
    // int columnCount = schema.GetColumnCnt();
    int columnCount = schema.getColumnList().size();

    // Add schema
    for (int i = 0; i < columnCount; i++) {
      String columnName = schema.getColumnName(i);
      int columnType = schema.getColumnType(i);
      columns.add(
          new QueryResultColumn(columnName, TypeUtil.openmldbTypeToMysqlTypeString(columnType)));
    }

    resultSetWriter.writeColumns(columns);

    // Add rows
    while (resultSet.next()) {
      // Build and respond rows
      List<String> row = new ArrayList<>();

      for (int i = 0; i < columnCount; i++) {
        // DataType type = schema.GetColumnType(i);
        int type = schema.getColumnType(i);
        String columnValue = TypeUtil.getResultSetStringColumn(resultSet, i + 1, type);
        row.add(columnValue);
      }

      resultSetWriter.writeRow(row);
    }

    // mysql workbench will check some variables
    if (sql.equalsIgnoreCase("show variables")) {
      List<String> row;
      for (String variable : MockResult.mockVariables.keySet()) {
        row = new ArrayList<>();
        row.add(variable);
        row.add(MockResult.mockVariables.get(variable));
        resultSetWriter.writeRow(row);
      }
    }

    // Finish the response
    resultSetWriter.finish();
  }

  public void start() {
    Thread currentThread = Thread.currentThread();

    try {
      currentThread.join(); // This will cause the current thread to wait indefinitely
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
