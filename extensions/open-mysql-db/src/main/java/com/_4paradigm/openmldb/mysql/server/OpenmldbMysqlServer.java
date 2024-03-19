package com._4paradigm.openmldb.mysql.server;

import cn.paxos.mysql.MySqlListener;
import cn.paxos.mysql.ResultSetWriter;
import cn.paxos.mysql.engine.QueryResultColumn;
import cn.paxos.mysql.engine.SqlEngine;
import cn.paxos.mysql.util.SHAUtils;
import cn.paxos.mysql.util.Utils;
import com._4paradigm.openmldb.common.Pair;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.mysql.mock.MockResult;
import com._4paradigm.openmldb.mysql.util.TypeUtil;
import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com.google.common.base.Strings;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OpenmldbMysqlServer {
  private final Map<Integer, SqlClusterExecutor> sqlClusterExecutorMap = new ConcurrentHashMap<>();

  private final Pattern showFullColumnsPattern =
      Pattern.compile("(?i)SHOW FULL COLUMNS FROM `(.+)` FROM `(.+)`");

  private final Pattern selectTablesPattern =
      Pattern.compile(
          "(?i)SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = '(.+)' ORDER BY TABLE_SCHEMA, TABLE_TYPE");

  private final Pattern selectColumnsPattern =
      Pattern.compile(
          "(?i)SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '(.+)' ORDER BY TABLE_SCHEMA, TABLE_NAME");

  private final Pattern selectCountUnionPattern =
      Pattern.compile(
          "(?i)(SELECT COUNT\\(\\*\\) FROM .+)(?: UNION (SELECT COUNT\\(\\*\\) FROM .+))+");

  // SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'xzs'
  private final Pattern selectCountTablesPattern =
      Pattern.compile(
          "(?i)SELECT COUNT\\(\\*\\) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '(.+)'");

  // SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'xzs'
  private final Pattern selectCountColumnsPattern =
      Pattern.compile(
          "(?i)SELECT COUNT\\(\\*\\) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '(.+)'");

  // SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'xzs'
  private final Pattern selectCountRoutinesPattern =
      Pattern.compile(
          "(?i)SELECT COUNT\\(\\*\\) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = '(.+)'");

  private final Pattern createDatabasePattern = Pattern.compile("(?i)(CREATE DATABASE `.+`).*");

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
            System.out.println(
                "Try to execute query, Database: "
                    + database
                    + ", User: "
                    + userName
                    + ", SQL: "
                    + sql);

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
                // SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'demo_db'
                // UNION SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA =
                // 'demo_db' UNION SELECT COUNT(*) FROM information_schema.ROUTINES WHERE
                // ROUTINE_SCHEMA = 'demo_db'
                Matcher selectCountUnionMatcher = selectCountUnionPattern.matcher(sql);
                if (selectCountUnionMatcher.matches()) {
                  // COUNT(*)
                  List<QueryResultColumn> columns = new ArrayList<>();
                  columns.add(new QueryResultColumn("COUNT(*)", "VARCHAR(255)"));
                  resultSetWriter.writeColumns(columns);
                  List<String> row;
                  for (int i = 1; i <= selectCountUnionMatcher.groupCount(); ++i) {
                    String unionSql = selectCountUnionMatcher.group(i);

                    Matcher selectCountTablesMatcher = selectCountTablesPattern.matcher(unionSql);
                    Matcher selectCountColumnsMatcher = selectCountColumnsPattern.matcher(unionSql);
                    Matcher selectCountRoutinesMatcher =
                        selectCountRoutinesPattern.matcher(unionSql);
                    if (selectCountTablesMatcher.matches()) {
                      String dbName = selectCountTablesMatcher.group(1);
                      row = new ArrayList<>();
                      row.add(
                          Integer.toString(
                              sqlClusterExecutorMap
                                  .get(connectionId)
                                  .getTableNames(dbName)
                                  .size()));
                      resultSetWriter.writeRow(row);
                    } else if (selectCountColumnsMatcher.matches()) {
                      String dbName = selectCountTablesMatcher.group(1);
                      int columnCount = 0;
                      for (String tableName :
                          sqlClusterExecutorMap.get(connectionId).getTableNames(dbName)) {
                        columnCount +=
                            sqlClusterExecutorMap
                                .get(connectionId)
                                .getTableSchema(dbName, tableName)
                                .getColumnList()
                                .size();
                      }
                      row = new ArrayList<>();
                      row.add(Integer.toString(columnCount));
                      resultSetWriter.writeRow(row);
                    } else if (selectCountRoutinesMatcher.matches()) {
                      row = new ArrayList<>();
                      row.add(Integer.toString(0));
                      resultSetWriter.writeRow(row);
                    }
                  }
                  resultSetWriter.finish();
                  return;
                }

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

                // mock SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM
                // information_schema.SCHEMATA
                if (sql.equalsIgnoreCase(
                    "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA")) {
                  List<String> dbs = sqlClusterExecutorMap.get(connectionId).showDatabases();
                  // SCHEMA_NAME	DEFAULT_CHARACTER_SET_NAME	DEFAULT_COLLATION_NAME
                  // mysql	utf8mb4	utf8mb4_0900_ai_ci
                  List<QueryResultColumn> columns = new ArrayList<>();
                  columns.add(new QueryResultColumn("SCHEMA_NAME", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("DEFAULT_CHARACTER_SET_NAME", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("DEFAULT_COLLATION_NAME", "VARCHAR(255)"));
                  resultSetWriter.writeColumns(columns);
                  List<String> row;
                  for (String db : dbs) {
                    row = new ArrayList<>();
                    row.add(db);
                    row.add("utf8mb4");
                    row.add("utf8mb4_0900_ai_ci");
                    resultSetWriter.writeRow(row);
                  }
                  resultSetWriter.finish();
                  return;
                }

                // mock SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES
                // WHERE TABLE_SCHEMA = 'demo_db' ORDER BY TABLE_SCHEMA, TABLE_TYPE
                Matcher selectTablesMatcher = selectTablesPattern.matcher(sql);
                if (selectTablesMatcher.matches()) {
                  String dbName = selectTablesMatcher.group(1);
                  List<String> tableNames =
                      sqlClusterExecutorMap.get(connectionId).getTableNames(dbName);
                  // TABLE_SCHEMA	TABLE_NAME	TABLE_TYPE
                  List<QueryResultColumn> columns = new ArrayList<>();
                  columns.add(new QueryResultColumn("TABLE_SCHEMA", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("TABLE_NAME", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("TABLE_TYPE", "VARCHAR(255)"));
                  resultSetWriter.writeColumns(columns);
                  List<String> row;
                  for (String tableName : tableNames) {
                    row = new ArrayList<>();
                    row.add(dbName);
                    row.add(tableName);
                    row.add("BASE TABLE");
                    resultSetWriter.writeRow(row);
                  }
                  resultSetWriter.finish();
                  return;
                }

                // mock SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM
                // information_schema.COLUMNS WHERE TABLE_SCHEMA = 'demo_db' ORDER BY TABLE_SCHEMA,
                // TABLE_NAME
                Matcher selectColumnsMatcher = selectColumnsPattern.matcher(sql);
                if (selectColumnsMatcher.matches()) {
                  String dbName = selectColumnsMatcher.group(1);
                  List<String> tableNames =
                      sqlClusterExecutorMap.get(connectionId).getTableNames(dbName);
                  // TABLE_SCHEMA	TABLE_NAME	COLUMN_NAME	COLUMN_TYPE
                  List<QueryResultColumn> columns = new ArrayList<>();
                  columns.add(new QueryResultColumn("TABLE_SCHEMA", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("TABLE_NAME", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("COLUMN_NAME", "VARCHAR(255)"));
                  columns.add(new QueryResultColumn("COLUMN_TYPE", "VARCHAR(255)"));
                  resultSetWriter.writeColumns(columns);
                  List<String> row;
                  Collections.sort(tableNames);
                  for (String tableName : tableNames) {
                    Schema tableSchema =
                        sqlClusterExecutorMap.get(connectionId).getTableSchema(dbName, tableName);
                    for (Column column : tableSchema.getColumnList()) {
                      row = new ArrayList<>();
                      row.add(dbName);
                      row.add(tableName);
                      row.add(column.getColumnName());
                      row.add(TypeUtil.openmldbTypeToMysqlTypeString(column.getSqlType()));
                      resultSetWriter.writeRow(row);
                    }
                  }
                  resultSetWriter.finish();
                  return;
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
                if (sql.equalsIgnoreCase("SHOW FULL TABLES")
                    || sql.equalsIgnoreCase("SHOW FULL TABLES WHERE Table_type != 'VIEW'")) {
                  sql = "SHOW TABLES";
                } else {
                  Matcher matcher = createDatabasePattern.matcher(sql);
                  if (matcher.matches()) {
                    sql = matcher.group(1);
                  }
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
              throw new IOException(e);
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
