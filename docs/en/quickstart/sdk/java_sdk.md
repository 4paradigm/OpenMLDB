# Java SDK

## Java SDK package installation

- Installing Java SDK package on Linux

  Configure the maven pom:

```XML
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.8.3</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.8.3</version>
</dependency>
```

- Installing Java SDK package on Mac

  Configure the maven pom

```XML
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.8.3</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.8.3-macos</version>
</dependency>
```

Note: Since the openmldb-native package contains the C++ static library compiled for OpenMLDB, it is defaults to the Linux static library. For macOS, the version of openmldb-native should be changed to `0.8.3-macos`, while the version of openmldb-jdbc should remain unchanged.

The macOS version of openmldb-native only supports macOS 12. To run it on macOS 11 or macOS 10.15, the openmldb-native package needs to be compiled from source code on the corresponding OS. For detailed compilation methods, please refer to [Concurrent Compilation of Java SDK](https://openmldb.ai/docs/zh/main/deploy/compile.html#java-sdk).

To connect to the OpenMLDB service using the Java SDK, you can use JDBC (recommended) or connect directly through SqlClusterExecutor. The following will demonstrate both connection methods in order.

## JDBC method 

The connection method using JDBC is as follows:

```java
Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
// No database in jdbcUrl
Connection connection = DriverManager.getConnection("jdbc:openmldb:///?zk=localhost:6181&zkPath=/openmldb");

// Set database in jdbcUrl
Connection connection1 = DriverManager.getConnection("jdbc:openmldb:///test_db?zk=localhost:6181&zkPath=/openmldb");
```

The database specified in the Connection address must exist when creating the connection.

```{caution}
he default execution mode for JDBC Connection is `online`.
```

### Usage overview

All SQL commands can be executed using `Statement`, both in online and offline modes. To switch between offline and online modes, use command `SET @@execute_mode='...';``. For example:

```java
Statement stmt = connection.createStatement();
stmt.execute("SET @@execute_mode='offline"); // Switch to offline mode
stmt.execute("SELECT * from t1"); // Offline select
ResultSet res = stmt.getResultSet(); // The ResultSet of the previous execute

stmt.execute("SET @@execute_mode='online"); // Switch to online mode
res = stmt.executeQuery("SELECT * from t1"); // For online mode, select or executeQuery can directly obtain the ResultSet result.
```

The `LOAD DATA` command is an asynchronous command, and the returned ResultSet contains information such as the job ID and state. You can execute `show job <id>` to check if the job has been completed. Note that the ResultSet needs to execute `next()` method to move the cursor to the first row of data.

It is also possible to change it to a synchronous command:

```SQL
SET @@sync_job=true;
```

If the actual execution time of the synchronous command exceeds the default maximum idle wait time of 0.5 hours, please [adjust the configuration](https://openmldb.ai/docs/zh/main/openmldb_sql/ddl/SET_STATEMENT.html#id4).

### PreparedStatement 

`PreparedStatement` supports `SELECT`, `INSERT`, and `DELETE` operations. Note that `INSERT` only supports online insertion.

```java
PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM t1 WHERE id=?");
PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO t1 VALUES (?,?)");
PreparedStatement insertStatement = connection.prepareStatement("DELETE FROM t1 WHERE id=?");
```

## SqlClusterExecutor method 

### Creating a SqlClusterExecutor 

First, configure the OpenMLDB connection parameters.

```java
SdkOption option = new SdkOption();
option.setZkCluster("127.0.0.1:2181");
option.setZkPath("/openmldb");
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);
```

Then, use SdkOption to create the Executor.

```java
sqlExecutor = new SqlClusterExecutor(option);
```

`SqlClusterExecutor` execution of SQL operations is thread-safe, and in actual environments, a single `SqlClusterExecutor` can be created. However, since the execution mode (execute_mode) is an internal variable of `SqlClusterExecutor`, if you want to execute an offline command and an online command at the same time, unexpected results may occur. In this case, please use multiple `SqlClusterExecutors`.

```{caution}
The default execution mode for SqlClusterExecutor is offline, which is different from the default mode for JDBC.
```

### Statement

`SqlClusterExecutor` can obtain a `Statement` similar to the JDBC approach and can use `Statement::execute`.

```java
java.sql.Statement state = sqlExecutor.getStatement();
try {
    state.execute("create database db_test");
} catch (Exception e) {
    e.printStackTrace();
} finally {
    state.close();
}
```

Note that `SqlClusterExecutor` does not have the concept of a default database, so you need to execute a `USE <db>` command before you can continue to create tables.

```java
java.sql.Statement state = sqlExecutor.getStatement();
try {
    state.execute("use db_test");
    String createTableSql = "create table trans(c1 string,\n" +
                    "                   c3 int,\n" +
                    "                   c4 bigint,\n" +
                    "                   c5 float,\n" +
                    "                   c6 double,\n" +
                    "                   c7 timestamp,\n" +
                    "                   c8 date,\n" +
                    "                   index(key=c1, ts=c7));";
    state.execute(createTableSql);
} catch (Exception e) {
    e.printStackTrace();
} finally {
    state.close();
}
```

#### Executing batch SQL queries with Statement 

Use the `Statement::execute` interface to execute batch SQL queries:

```java
java.sql.Statement state = sqlExecutor.getStatement();
try {
    state.execute("use db_test");
    // The default execution mode for sqlExecutor is offline. If the mode has not been changed to online before, the execution mode needs to be set to online here.
    state.execute("SET @@execute_mode='online;");
    // If the return value of execute is true, it means that the operation is successful, and the result can be obtained through getResultSet.
    boolean ret = state.execute("select * from trans;");
    Assert.assertTrue(ret);
    java.sql.ResultSet rs = state.getResultSet();
} catch (Exception e) {
    e.printStackTrace();
}
```

Accessing query results:

```java
// Accessing the ResultSet and printing the first three columns of data.
try {
    while (result.next()) {
        System.out.println(resultSet.getString(1) + "," + resultSet.getInt(2) "," + resultSet.getLong(3));
    }
} catch (SQLException e) {
    e.printStackTrace();
} finally {
    try {
        if (result != null) {
            result.close();
        }
    } catch (SQLException throwables) {
        throwables.printStackTrace();
    }
}
```

### PreparedStatement

`SqlClusterExecutor` can also obtain `PreparedStatement`, but you need to specify which type of `PreparedStatement` to obtain. For example, when using InsertPreparedStmt for insertion operations, there are three ways to do it.

```{note}
Insert operation only supports online mode and is not affected by execution mode. The data will always be inserted into the online database.
```

#### Common Insert

1. Use the `SqlClusterExecutor::getInsertPreparedStmt(db, insertSql)` method to get the InsertPrepareStatement.
2. Use the `PreparedStatement::execute()` method to execute the insert statement.

```java
String insertSql = "insert into trans values(\"aa\",23,33,1.4,2.4,1590738993000,\"2020-05-04\");";
java.sql.PreparedStatement pstmt = null;
try {
    pstmt = sqlExecutor.getInsertPreparedStmt(db, insertSql);
    Assert.assertTrue(pstmt.execute());
} catch (SQLException e) {
    e.printStackTrace();
    Assert.fail();
} finally {
    if (pstmt != null) {
        try {
            // After using the PrepareStatement, it must be closed.
            pstmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
```

#### Insert With Placeholder

1. Get InsertPrepareStatement by calling `SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)` interface.
2. Use `PreparedStatement::setType(index, value)` interface to fill in data to the InsertPrepareStatement. Note that the index starts from 1.
3. Use `PreparedStatement::execute()` interface to execute the insert statement.

```{note}
When the conditions of the PreparedStatement are the same, you can repeatedly call the set method of the same object to fill in data before executing execute(). There is no need to create a new PreparedStatement object.
```

```java
String insertSqlWithPlaceHolder = "insert into trans values(\"aa\", ?, 33, ?, 2.4, 1590738993000, \"2020-05-04\");";
java.sql.PreparedStatement pstmt = null;
try {
  pstmt = sqlExecutor.getInsertPreparedStmt(db, insertSqlWithPlaceHolder);
  pstmt.setInt(1, 24);
  pstmt.setInt(2, 1.5f);
  pstmt.execute();
} catch (SQLException e) {
  e.printStackTrace();
  Assert.fail();
} finally {
  if (pstmt != null) {
    try {
      // After using the PrepareStatement, it must be closed.
      pstmt.close();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
```

```{note}
After execute, the cached data will be cleared and it is not possible to retry execute.
```

#### Batch Insert With Placeholder

1. To use batch insert, first obtain the InsertPrepareStatement using the `SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)` interface.
2. Then use the `PreparedStatement::setType(index, value)` interface to fill data into the InsertPrepareStatement.
3. Use the `PreparedStatement::addBatch()` interface to complete filling for one row.
4. Continue to use `setType(index, value)` and `addBatch()` to fill multiple rows.
5. Use the `PreparedStatement::executeBatch()` interface to complete the batch insertion.

```java
String insertSqlWithPlaceHolder = "insert into trans values(\"aa\", ?, 33, ?, 2.4, 1590738993000, \"2020-05-04\");";
java.sql.PreparedStatement pstmt = null;
try {
  pstmt = sqlExecutor.getInsertPreparedStmt(db, insertSqlWithPlaceHolder);
  pstmt.setInt(1, 24);
  pstmt.setInt(2, 1.5f);
  pstmt.addBatch();
  pstmt.setInt(1, 25);
  pstmt.setInt(2, 1.7f);
  pstmt.addBatch();
  pstmt.executeBatch();
} catch (SQLException e) {
  e.printStackTrace();
  Assert.fail();
} finally {
  if (pstmt != null) {
    try {
      // After using the PrepareStatement, it must be closed.
      pstmt.close();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
```

```{note}
After executeBatch(), all cached data will be cleared and it's not possible to retry executeBatch().
```

### Execute SQL request query 

`RequestPreparedStmt` is a unique query mode (not supported by JDBC). This mode requires both the selectSql and a request data, so you need to provide the SQL and set the request data using setType when calling `getRequestPreparedStmt`.

There are three steps to execute a SQL request query:

```{note}
request queries only support online mode and are not affected by the execution mode. They must be performed as online request queries.
```

1. Use the `SqlClusterExecutor::getRequestPreparedStmt(db, selectSql)` interface to obtain a RequestPrepareStatement.
2. Call the `PreparedStatement::setType(index, value)` interface to set the request data. Please call the `setType` interface and configure valid values based on the data type corresponding to each column in the data table.
3. Call the `Statement::executeQuery()` interface to execute the request-style query statement.

```java
String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
PreparedStatement pstmt = null;
ResultSet resultSet = null;
/*
c1 string,\n" +
                "                   c3 int,\n" +
                "                   c4 bigint,\n" +
                "                   c5 float,\n" +
                "                   c6 double,\n" +
                "                   c7 timestamp,\n" +
                "                   c8 date,\n" +
*/
try {
    // Step 1，get RequestPrepareStatement
    pstmt = sqlExecutor.getRequestPreparedStmt(db, selectSql);
    
    // Step 2，To execute the request mode, you need to set a row of request data in the RequestPreparedStatement.
    pstmt.setString(1, "bb");
    pstmt.setInt(2, 24);
    pstmt.setLong(3, 34l);
    pstmt.setFloat(4, 1.5f);
    pstmt.setDouble(5, 2.5);
    pstmt.setTimestamp(6, new Timestamp(1590738994000l));
    pstmt.setDate(7, Date.valueOf("2020-05-05"));
    
    // Calling executeQuery will execute the select sql, and then put the result in the resultSet
    resultSet = pstmt.executeQuery();
    
    // Access resultSet
    Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(resultSet.getString(1), "bb");
    Assert.assertEquals(resultSet.getInt(2), 24);
    Assert.assertEquals(resultSet.getLong(3), 34);
    
    // The return result set of the ordinary request query contains only one row of results. Therefore, the result of the second call to resultSet. next() is false
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
```

### Delete all data of a key under the specified index

There are two ways to delete data through the Java SDK:

- Execute delete SQL directly

- Use delete PreparedStatement

Note that this can only delete data under one index, not all indexes. Refer to [DELETE function boundary](https://openmldb.ai/docs/zh/main/quickstart/function_boundary.html#delete) for details.

```java
java.sql.Statement state = router.getStatement();
try {
    String sql = "DELETE FROM t1 WHERE col2 = 'key1';";
    state.execute(sql);
    sql = "DELETE FROM t1 WHERE col2 = ?;";
    java.sql.PreparedStatement p1 = router.getDeletePreparedStmt("test", sql);
    p1.setString(1, "key2");
    p1.executeUpdate();
    p1.close();
} catch (Exception e) {
    e.printStackTrace();
    Assert.fail();
} finally {
    try {
        state.close();
    } catch (Exception e) {
        e.printStackTrace();
    }
}
```

### A complete example of using SqlClusterExecutor

Refer to the [Java quickstart demo](https://github.com/4paradigm/OpenMLDB/tree/main/demo/java_quickstart/demo). If it is used on macOS, please use openmldb-native of macOS version and increase the dependency of openmldb-native.

Compile and run:

```
mvn package
java -cp target/demo-1.0-SNAPSHOT.jar com.openmldb.demo.App
```

## SDK Configuration Details

You must fill in `zkCluster` and `zkPath` (set method or the configuration `foo=bar` after `?` in JDBC).

### Optional configuration

| Optional configuration | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| enableDebug            | The default is false. Enable the debug log of hybridse (note that it is not the global debug log). You can view more logs of sql compilation and operation. However, not all of these logs are collected by the client. You need to view the tablet server logs. |
| requestTimeout         | The default is 60000 ms. This timeout is the rpc timeout sent by the client, except for those sent to the taskmanager (the rpc timeout of the job is controlled by the variable `job_timeout`). |
| glogLevel              | The default is 0, which is similar to the minloglevel of the glog. The `INFO/WARNING/ERROR/FATAL` log corresponds to `0/1/2/3` respectively. 0 means to print INFO and the level on. |
| glogDir                | The default is empty. When the log directory is empty, it is printed to stderr. This is referring to the console. |
| maxSqlCacheSize        | The default is 50, the maximum number of sql caches for a single execution mode of a single database on the client. If there is an error caused by cache obsolescence, you can increase this size to avoid the problem. |
| sessionTimeout         | Default 10000 ms, session timeout of zk                      |
| zkLogLevel             | By default, 3, `0/1/2/3/4` respectively means that `all zk logs/error/warn/info/debug are prohibited` |
| zkLogFile              | The default is empty, which is printed to stdout.            |
| sparkConfPath          | The default is empty. You can change the spark conf used by the job through this configuration without configuring the taskmanager to restart. |

## SQL verification

The Java client supports the correct verification of SQL to verify whether it is executable. It is divided into batch and request modes.

- `ValidateSQLInBatch` can verify whether SQL can be executed at the offline end.
- `ValidateSQLInRequest` can verify whether SQL can be deployed online.

Both interfaces need to go through all table schemas required by SQL. Currently, only single db is supported. Please do not use `db.table` format in SQL statements.

For example, verify SQL `select count (c1) over w1 from t3 window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row);`, In addition to this statement, you need to go through in the schema of table `t3` as the second parameter schemaMaps. The format is Map, key is the name of the db, and value is all the table schemas (maps) of each db. In fact, only a single db is supported, so there is usually only one db here, as shown in db3 below. The table schema map key under db is table name, and the value is com._ 4paradigm.openmldb.sdk.Schema, consisting of the name and type of each column.

```java
Map<String, Map<String, Schema>> schemaMaps = new HashMap<>();
Map<String, Schema> dbSchema = new HashMap<>();
dbSchema = new HashMap<>();
dbSchema.put("t3", new Schema(Arrays.asList(new Column("c1", Types.VARCHAR), new Column("c2", Types.BIGINT))));
schemaMaps.put("db3", dbSchema);
List<String> ret = SqlClusterExecutor.validateSQLInRequest("select count(c1) over w1 from t3 window "+
        "w1 as(partition by c1 order by c2 rows between unbounded preceding and current row);", schemaMaps);
Assert.assertEquals(ret.size(), 0);
```

