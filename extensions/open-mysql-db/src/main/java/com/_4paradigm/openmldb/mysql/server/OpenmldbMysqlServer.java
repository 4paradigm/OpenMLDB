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
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com.google.common.base.Strings;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OpenmldbMysqlServer {
  private final Map<Integer, SqlClusterExecutor> sqlClusterExecutorMap = new ConcurrentHashMap<>();

  private final Pattern showFullColumnsPattern =
      Pattern.compile("(?i)SHOW FULL COLUMNS FROM `(.+)` FROM `(.+)`");

  private final Pattern showColumnsPattern =
      Pattern.compile("(?i)SHOW COLUMNS FROM `(.+)` FROM `(.+)`");

  private final Pattern selectTablesPattern =
      Pattern.compile(
          "(?i)SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema\\.TABLES WHERE TABLE_SCHEMA = '(.+)' ORDER BY TABLE_SCHEMA, TABLE_TYPE");

  private final Pattern selectColumnsPattern =
      Pattern.compile(
          "(?i)SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM information_schema\\.COLUMNS WHERE TABLE_SCHEMA = '(.+)' ORDER BY TABLE_SCHEMA, TABLE_NAME");

  // SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='xzs' AND TABLE_NAME='t_exam_paper'
  // ORDER BY ORDINAL_POSITION
  private final Pattern selectColumns4DbeaverPattern =
      Pattern.compile(
          "(?i)SELECT \\* FROM information_schema\\.COLUMNS WHERE TABLE_SCHEMA='(.+)' AND TABLE_NAME='(.+)' ORDER BY ORDINAL_POSITION");

  private final Pattern selectCountUnionPattern =
      Pattern.compile(
          "(?i)(SELECT COUNT\\(\\*\\) FROM .+)(?: UNION (SELECT COUNT\\(\\*\\) FROM .+))+");

  // SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'xzs'
  private final Pattern selectCountTablesPattern =
      Pattern.compile(
          "(?i)SELECT COUNT\\(\\*\\) FROM information_schema\\.TABLES WHERE TABLE_SCHEMA = '(.+)'");

  // SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'xzs' AND table_name =
  // 't_exam_paper'
  private final Pattern selectCountSchemaTablesPattern =
      Pattern.compile(
          "(?i)SELECT COUNT\\(\\*\\) FROM information_schema\\.TABLES WHERE TABLE_SCHEMA = '(.+)' AND table_name = '(.+)'");

  // SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'xzs'
  private final Pattern selectCountColumnsPattern =
      Pattern.compile(
          "(?i)SELECT COUNT\\(\\*\\) FROM information_schema\\.COLUMNS WHERE TABLE_SCHEMA = '(.+)'");

  // SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'xzs'
  private final Pattern selectCountRoutinesPattern =
      Pattern.compile(
          "(?i)SELECT COUNT\\(\\*\\) FROM information_schema\\.ROUTINES WHERE ROUTINE_SCHEMA = '(.+)'");

  private final Pattern createDatabasePattern = Pattern.compile("(?i)(CREATE DATABASE `.+`).*");

  private final Pattern selectLimitPattern = Pattern.compile("(?i)(SELECT .+) limit \\d+,\\s*\\d+");

  // SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='demo_db'
  private final Pattern selectSchemaTaPattern =
      Pattern.compile("(?i)SELECT \\* FROM information_schema\\.SCHEMATA WHERE SCHEMA_NAME='(.+)'");

  // SHOW FULL TABLES FROM demo_db
  private final Pattern showTablesFromDbPattern = Pattern.compile("(?i)SHOW FULL TABLES FROM (.+)");

  public OpenmldbMysqlServer(
      int port, String zkCluster, String zkRootPath, long sessionTimeout, long requestTimeout) {
    new MySqlListener(
        port,
        100,
        new SqlEngine() {
          @Override
          public void useDatabase(int connectionId, String database) throws IOException {
            try {
              System.out.println("Use database: " + database);
              java.sql.Statement stmt = sqlClusterExecutorMap.get(connectionId).getStatement();
              stmt.execute("use " + database);
              stmt.close();
            } catch (Exception e) {
              e.printStackTrace();
              throw new IOException(e);
            }
          }

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
                    option.setSessionTimeout(sessionTimeout);
                    option.setRequestTimeout(requestTimeout);
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
              if (mockPlainQuery(resultSetWriter, sql)) {
                return;
              }

              if (mockSelectSchemaTableCount(connectionId, resultSetWriter, sql)) {
                return;
              }

              // This mock must execute before mockPatternQuery
              // SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'demo_db'
              // UNION SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA =
              // 'demo_db' UNION SELECT COUNT(*) FROM information_schema.ROUTINES WHERE
              // ROUTINE_SCHEMA = 'demo_db'
              if (mockSelectCountUnion(connectionId, resultSetWriter, sql)) {
                return;
              }

              if (mockPatternQuery(resultSetWriter, sql)) {
                return;
              }

              // mock SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM
              // information_schema.SCHEMATA
              if (mockInformationSchemaSchemaTa(connectionId, resultSetWriter, sql)) {
                return;
              }

              // mock SELECT DATABASE()
              if (mockSelectDatabase(connectionId, resultSetWriter, sql)) {
                return;
              }

              // mock SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES
              // WHERE TABLE_SCHEMA = 'demo_db' ORDER BY TABLE_SCHEMA, TABLE_TYPE
              if (mockSelectTables(connectionId, resultSetWriter, sql)) {
                return;
              }

              // mock SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM
              // information_schema.COLUMNS WHERE TABLE_SCHEMA = 'demo_db' ORDER BY TABLE_SCHEMA,
              // TABLE_NAME
              if (mockSelectColumns(connectionId, resultSetWriter, sql)) {
                return;
              }

              // mock SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='xzs' AND
              // TABLE_NAME='t_exam_paper' ORDER BY ORDINAL_POSITION
              if (mockSelectColumns4Dbeaver(connectionId, resultSetWriter, sql)) {
                return;
              }

              // mock SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='demo_db'
              if (mockSchemaTa(resultSetWriter, sql)) {
                return;
              }

              java.sql.Statement stmt = sqlClusterExecutorMap.get(connectionId).getStatement();

              if (!Strings.isNullOrEmpty(database)) {
                stmt.execute("use " + database);
              }

              if (mockShowColumns(resultSetWriter, sql, stmt)) {
                return;
              }

              String originalSql = sql;
              if (sql.startsWith("SHOW FULL TABLES")) {
                // SHOW FULL TABLES WHERE Table_type != 'VIEW'
                Matcher showTablesFromDbMatcher = showTablesFromDbPattern.matcher(sql);
                if (showTablesFromDbMatcher.matches()) {
                  String dbName = showTablesFromDbMatcher.group(1);
                  stmt.execute("use " + dbName);
                  sql = "SHOW TABLES FROM " + dbName;
                } else {
                  sql = "SHOW TABLES";
                }
              } else if (sql.matches("(?i)(?s)^\\s*CREATE TABLE.*$")) {
                // convert data type TEXT to STRING
                sql = sql.replaceAll("(?i) TEXT", " STRING");
                // sql = sql.replaceAll("(?i) DATETIME", " DATE");
                if (!sql.toLowerCase().contains(" not null")
                    && sql.toLowerCase().contains(" null")) {
                  sql = sql.replaceAll("(?i) null", "");
                }
              } else {
                Matcher crateDatabaseMatcher = createDatabasePattern.matcher(sql);
                Matcher selectLimitMatcher = selectLimitPattern.matcher(sql);
                if (crateDatabaseMatcher.matches()) {
                  sql = crateDatabaseMatcher.group(1);
                } else if (selectLimitMatcher.matches()) {
                  sql = selectLimitMatcher.group(1);
                }
              }
              sql = sql.replaceAll("(?i)\\s*limit\\s*\\d+\\s*,\\s*\\d+", "");
              stmt.execute(sql);

              if (sql.toLowerCase().startsWith("select") || sql.toLowerCase().startsWith("show")) {
                SQLResultSet resultSet = (SQLResultSet) stmt.getResultSet();
                outputResultSet(resultSetWriter, resultSet, originalSql);
              }

              System.out.println("Success to execute OpenMLDB SQL: " + sql);

              // Close resources
              stmt.close();
            } catch (Exception e) {
              e.printStackTrace();
              throw new IOException(e);
            }
          }

          private boolean mockPlainQuery(ResultSetWriter resultSetWriter, String sql) {
            if (MockResult.mockResults.containsKey(sql.toLowerCase())) {
              Pair<List<QueryResultColumn>, List<List<String>>> pair =
                  MockResult.mockResults.get(sql.toLowerCase());
              resultSetWriter.writeColumns(pair.getKey());
              for (List<String> row : pair.getValue()) {
                resultSetWriter.writeRow(row);
              }
              resultSetWriter.finish();
              return true;
            }
            return false;
          }

          private boolean mockShowColumns(
              ResultSetWriter resultSetWriter, String sql, Statement stmt) throws SQLException {
            Matcher showFullColumnsMatcher = showFullColumnsPattern.matcher(sql);
            Matcher showColumnsMatcher = showColumnsPattern.matcher(sql);
            boolean showFullColumnsMatch = showFullColumnsMatcher.matches();
            boolean showColumnsMatch = showColumnsMatcher.matches();
            if (showFullColumnsMatch || showColumnsMatch) {
              String tableName;
              String databaseName;
              if (showFullColumnsMatch) {
                tableName = showFullColumnsMatcher.group(1);
                databaseName = showFullColumnsMatcher.group(2);
              } else {
                tableName = showColumnsMatcher.group(1);
                databaseName = showColumnsMatcher.group(2);
              }
              String selectStarSql = "select * from `" + databaseName + "`.`" + tableName + "`";
              stmt.execute(selectStarSql);

              List<QueryResultColumn> columns = new ArrayList<>();
              // # Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment
              // id, int, , NO, PRI, , auto_increment, select,insert,update,references,
              columns.add(new QueryResultColumn("Field", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("Type", "VARCHAR(255)"));
              if (showFullColumnsMatch) {
                columns.add(new QueryResultColumn("Collation", "VARCHAR(255)"));
              }
              columns.add(new QueryResultColumn("Null", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("Key", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("Default", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("Extra", "VARCHAR(255)"));
              if (showFullColumnsMatch) {
                columns.add(new QueryResultColumn("Privileges", "VARCHAR(255)"));
                columns.add(new QueryResultColumn("Comment", "VARCHAR(255)"));
              }
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
                if (showFullColumnsMatch) {
                  row.add("");
                }
                row.add("");
                row.add("");
                row.add("");
                row.add("");
                if (showFullColumnsMatch) {
                  row.add("");
                  row.add("");
                }
                resultSetWriter.writeRow(row);
              }
              resultSetWriter.finish();
              stmt.close();
              return true;
            }
            return false;
          }

          private boolean mockSchemaTa(ResultSetWriter resultSetWriter, String sql) {
            // mock SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='demo_db'
            Matcher selectSchemaTaMatcher = selectSchemaTaPattern.matcher(sql);
            if (selectSchemaTaMatcher.matches()) {
              String dbName = selectSchemaTaMatcher.group(1);
              // CATALOG_NAME	SCHEMA_NAME	DEFAULT_CHARACTER_SET_NAME	DEFAULT_COLLATION_NAME
              //	SQL_PATH	DEFAULT_ENCRYPTION
              List<QueryResultColumn> columns = new ArrayList<>();
              columns.add(new QueryResultColumn("CATALOG_NAME", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("SCHEMA_NAME", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("DEFAULT_CHARACTER_SET_NAME", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("DEFAULT_COLLATION_NAME", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("SQL_PATH", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("DEFAULT_ENCRYPTION", "VARCHAR(255)"));
              resultSetWriter.writeColumns(columns);
              List<String> row;
              // def	xzs	utf8mb4	utf8mb4_0900_ai_ci		NO
              row = new ArrayList<>();
              row.add("def");
              row.add(dbName);
              row.add("utf8mb4");
              row.add("utf8mb4_0900_ai_ci");
              row.add(null);
              row.add("NO");
              resultSetWriter.writeRow(row);
              resultSetWriter.finish();
              return true;
            }
            return false;
          }

          private boolean mockSelectColumns4Dbeaver(
              int connectionId, ResultSetWriter resultSetWriter, String sql) throws SQLException {
            // mock SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='xzs' AND
            // TABLE_NAME='t_exam_paper' ORDER BY ORDINAL_POSITION
            Matcher selectColumns4DbeaverMatcher = selectColumns4DbeaverPattern.matcher(sql);
            if (selectColumns4DbeaverMatcher.matches()) {
              String dbName = selectColumns4DbeaverMatcher.group(1);
              String tableName = selectColumns4DbeaverMatcher.group(2);
              // TABLE_CATALOG	TABLE_SCHEMA	TABLE_NAME	COLUMN_NAME	ORDINAL_POSITION
              //	COLUMN_DEFAULT	IS_NULLABLE	DATA_TYPE	CHARACTER_MAXIMUM_LENGTH
              //	CHARACTER_OCTET_LENGTH	NUMERIC_PRECISION	NUMERIC_SCALE	DATETIME_PRECISION
              //	CHARACTER_SET_NAME	COLLATION_NAME	COLUMN_TYPE	COLUMN_KEY	EXTRA	PRIVILEGES
              //	COLUMN_COMMENT	GENERATION_EXPRESSION	SRS_ID
              // def	xzs	t_exam_paper	id	1		NO	int			10	0				int	PRI	auto_increment
              //	select,insert,update,references
              List<QueryResultColumn> columns = new ArrayList<>();
              columns.add(new QueryResultColumn("TABLE_CATALOG", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("TABLE_SCHEMA", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("TABLE_NAME", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("COLUMN_NAME", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("ORDINAL_POSITION", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("COLUMN_DEFAULT", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("IS_NULLABLE", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("DATA_TYPE", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("CHARACTER_MAXIMUM_LENGTH", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("CHARACTER_OCTET_LENGTH", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("NUMERIC_PRECISION", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("NUMERIC_SCALE", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("DATETIME_PRECISION", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("CHARACTER_SET_NAME", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("COLLATION_NAME", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("COLUMN_TYPE", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("COLUMN_KEY", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("EXTRA", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("PRIVILEGES", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("COLUMN_COMMENT", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("GENERATION_EXPRESSION", "VARCHAR(255)"));
              columns.add(new QueryResultColumn("SRS_ID", "VARCHAR(255)"));
              resultSetWriter.writeColumns(columns);
              List<String> row;
              Schema tableSchema =
                  sqlClusterExecutorMap.get(connectionId).getTableSchema(dbName, tableName);
              int ordinalPosition = 1;
              for (Column column : tableSchema.getColumnList()) {
                row = new ArrayList<>();
                row.add("def");
                row.add(dbName);
                row.add(tableName);
                row.add(column.getColumnName());
                row.add(Integer.toString(ordinalPosition++));
                row.add(null);
                row.add("NO");
                row.add(TypeUtil.openmldbTypeToMysqlTypeString(column.getSqlType()));
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(null);
                row.add(TypeUtil.openmldbTypeToMysqlTypeString(column.getSqlType()));
                row.add(null);
                row.add(null);
                row.add("select,insert,update,references");
                row.add(null);
                row.add(null);
                row.add(null);
                resultSetWriter.writeRow(row);
              }

              resultSetWriter.finish();
              return true;
            }
            return false;
          }

          private boolean mockSelectColumns(
              int connectionId, ResultSetWriter resultSetWriter, String sql) throws SQLException {
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
              return true;
            }
            return false;
          }

          private boolean mockSelectTables(
              int connectionId, ResultSetWriter resultSetWriter, String sql) {
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
              return true;
            }
            return false;
          }

          private boolean mockSelectDatabase(
              int connectionId, ResultSetWriter resultSetWriter, String sql) throws SQLException {
            // mock SELECT DATABASE()
            if (sql.equalsIgnoreCase("SELECT DATABASE()")) {
              java.sql.Statement stmt = sqlClusterExecutorMap.get(connectionId).getStatement();
              stmt.execute("show table status");

              // DATABASE()
              // demo_db
              List<QueryResultColumn> columns = new ArrayList<>();
              columns.add(new QueryResultColumn("DATABASE()", "VARCHAR(255)"));
              resultSetWriter.writeColumns(columns);
              SQLResultSet resultSet = (SQLResultSet) stmt.getResultSet();
              Schema schema = resultSet.GetInternalSchema();
              int columnCount = schema.getColumnList().size();
              int dbNameIndex = -1;
              for (int i = 0; i < columnCount; i++) {
                if (schema.getColumnName(i).equalsIgnoreCase("Database_name")) {
                  dbNameIndex = i;
                  break;
                }
              }

              if (dbNameIndex != -1) {
                while (resultSet.next()) {
                  // Build and respond rows
                  List<String> row = new ArrayList<>();
                  for (int i = 0; i < columnCount; i++) {
                    if (i == dbNameIndex) {
                      int type = schema.getColumnType(i);
                      String columnValue =
                          TypeUtil.getResultSetStringColumn(resultSet, i + 1, type);
                      row.add(columnValue);
                      break;
                    }
                  }
                  resultSetWriter.writeRow(row);
                }
              }

              resultSetWriter.finish();
              stmt.close();
              return true;
            }
            return false;
          }

          private boolean mockInformationSchemaSchemaTa(
              int connectionId, ResultSetWriter resultSetWriter, String sql) {
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
              return true;
            }
            return false;
          }

          private boolean mockPatternQuery(ResultSetWriter resultSetWriter, String sql) {
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
                return true;
              }
            }
            return false;
          }

          private boolean mockSelectSchemaTableCount(
              int connectionId, ResultSetWriter resultSetWriter, String sql) throws SQLException {
            // SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'xzs' AND
            // table_name = 't_exam_paper'
            Matcher selectCountSchemaTablesMatcher = selectCountSchemaTablesPattern.matcher(sql);
            if (selectCountSchemaTablesMatcher.matches()) {
              // COUNT(*)
              List<QueryResultColumn> columns = new ArrayList<>();
              columns.add(new QueryResultColumn("COUNT(*)", "VARCHAR(255)"));
              resultSetWriter.writeColumns(columns);

              List<String> row;
              String dbName = selectCountSchemaTablesMatcher.group(1);
              String tableName = selectCountSchemaTablesMatcher.group(2);
              row = new ArrayList<>();
              NS.TableInfo tableInfo =
                  sqlClusterExecutorMap.get(connectionId).getTableInfo(dbName, tableName);
              if (tableInfo == null || tableInfo.getName().equals("")) {
                row.add("0");
              } else {
                row.add("1");
              }
              resultSetWriter.writeRow(row);

              resultSetWriter.finish();
              return true;
            }
            return false;
          }

          private boolean mockSelectCountUnion(
              int connectionId, ResultSetWriter resultSetWriter, String sql) throws SQLException {
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
                Matcher selectCountRoutinesMatcher = selectCountRoutinesPattern.matcher(unionSql);
                if (selectCountTablesMatcher.matches()) {
                  String dbName = selectCountTablesMatcher.group(1);
                  row = new ArrayList<>();
                  row.add(
                      Integer.toString(
                          sqlClusterExecutorMap.get(connectionId).getTableNames(dbName).size()));
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
              return true;
            }
            return false;
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
    long sessionTimeout = ServerConfig.getSessionTimeout();
    long requestTimeout = ServerConfig.getRequestTimeout();

    try {
      OpenmldbMysqlServer server =
          new OpenmldbMysqlServer(
              serverPort, zkCluster, zkRootPath, sessionTimeout, requestTimeout);
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

    int tableIdColumnIndex = 0;
    // Add schema
    for (int i = 0; i < columnCount; i++) {
      String columnName = schema.getColumnName(i);
      if ((sql.startsWith("SHOW FULL TABLES") || sql.equalsIgnoreCase("show table status"))
          && columnName.equalsIgnoreCase("table_id")) {
        tableIdColumnIndex = i;
        continue;
      }
      int columnType = schema.getColumnType(i);
      columns.add(
          new QueryResultColumn(columnName, TypeUtil.openmldbTypeToMysqlTypeString(columnType)));
    }
    if (sql.startsWith("SHOW FULL TABLES")) {
      columns.add(new QueryResultColumn("Table_type", "VARCHAR(255)"));
    }

    resultSetWriter.writeColumns(columns);

    // Add rows
    while (resultSet.next()) {
      // Build and respond rows
      List<String> row = new ArrayList<>();

      for (int i = 0; i < columnCount; i++) {
        // DataType type = schema.GetColumnType(i);
        // hack: skip table id, table_id will be parsed as table name by navicat
        if (sql.equalsIgnoreCase("show table status") && i == tableIdColumnIndex) {
          continue;
        }
        int type = schema.getColumnType(i);
        String columnValue = TypeUtil.getResultSetStringColumn(resultSet, i + 1, type);
        row.add(columnValue);
      }
      if (sql.startsWith("SHOW FULL TABLES")) {
        row.add("BASE TABLE");
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
