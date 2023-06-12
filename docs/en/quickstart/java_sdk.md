# Java SDK Quickstart

## 1. Package Installation

### Package Installation on Linux
Configure maven pom

```xml
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.8.0</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.8.0</version>
</dependency>
```
### Package Installation on Mac
Configure maven pom

```xml
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.8.0</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.8.0-macos</version>
</dependency>
```
Note that since `openmldb-native` contains the C++ static library compiled by OpenMLDB, by default it is a Linux's static library. On macOS, the version of the above openmldb-native needs to be changed to `0.8.0-macos`, and the version of openmldb-jdbc remains unchanged.

The macOS native relase only supports macos-12. If you want use in macos-11 or macos 10.15, you should build openmldb-native from source in macos-11/macos-10.15, see [Build Java SDK](../deploy/compile.md#build-java-sdk-with-multi-processes) for details.

## 2. Quickstart

We can connect the OpenMLDB by JDBC Connection or SqlClusterExecutor.

### JDBC Connection

JDBC Connecton only supports OpenMLDB cluster, no standalone.

```
Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
// No database in jdbcUrl
Connection connection = DriverManager.getConnection("jdbc:openmldb:///?zk=localhost:6181&zkPath=/openmldb");

// Set database in jdbcUrl
Connection connection1 = DriverManager.getConnection("jdbc:openmldb:///test_db?zk=localhost:6181&zkPath=/openmldb");
```

The database in connection url must exist.

```{caution}
JDBC Connection default execute mode is`online`.
```

#### 使用概览

You can use `Statement` to execute all sql in online or offline mode. To switch the execute mode, you should `SET @@execute_mode='...';`. For example:
```java
Statement stmt = connection.createStatement();
stmt.execute("SET @@execute_mode='offline"); // set offline mode
stmt.execute("SELECT * from t1"); // offline select
ResultSet res = stmt.getResultSet(); // get the job info of the offline select
stmt.execute("SET @@execute_mode='online"); // set online mode
res = stmt.executeQuery("SELECT * from t1"); // online select, and executeQuery will return the result
```

The offline sql and online `LOAD DATA` are async in default, so the result is the job info(id, state, etc.), not the data. You can execute `show job <id>` to check if the job is finished. **You should run `ResultSet.next()` to get the first row in result, do not run `ResultSet.getXXX` without `next()`**.

The job can be set to sync：
```
SET @@sync_job=true;
```
```{tip}
If the sync job takes more than 0.5h, you should [change the config](../reference/sql/ddl/SET_STATEMENT.md#offline-commands-configuration-details).
```

#### PreparedStatement

`PreparedStatement` supports `SELECT`, `INSERT` and `DELETE`，`INSERT` only inserts into online.
```java
PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM t1 WHERE id=?");
PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO t1 VALUES (?,?)");
PreparedStatement insertStatement = connection.prepareStatement("DELETE FROM t1 WHERE id=?");
```

### SqlClusterExecutor
#### Create SqlClusterExecutor

First, the OpenMLDB connection parameters should be configured. SdkOption is cluster mode in default.

```java
// cluster：
SdkOption option = new SdkOption();
option.setZkCluster("127.0.0.1:2181");
option.setZkPath("/openmldb");
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);

// standalone:
SdkOption option = new SdkOption();
option.setHost("127.0.0.1");
option.setPort(6527);
option.setClusterMode(false); // required
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);
```

Then，create the executor.

```java
sqlExecutor = new SqlClusterExecutor(option);
```

`SqlClusterExecutor` is thread-safe, but the execute mode is cached in `SqlClusterExecutor`. If one thread set online and execute an online job, and another thread set offline and execute an offline job, the result is unpredictable. If you want multi-threading and execute in multi modes, you should create multi `SqlClusterExecutor`.

```{caution}
SqlClusterExecutor execute mode is `offline` in default, it's different with JDBC Connection.
```
#### Statement

Create a database:

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

Create a table in database 'db_test':

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

##### Use Statement to Query

```java
java.sql.Statement state = sqlExecutor.getStatement();
try {
    state.execute("use db_test");
    // sqlExecutor execute mode is offline in default. Set online here
    state.execute("SET @@execute_mode='online;");
    // we can `getResultSet` only if returns true
    boolean ret = state.execute("select * from trans;");
    Assert.assertTrue(ret);
    java.sql.ResultSet rs = state.getResultSet();
} catch (Exception e) {
    e.printStackTrace();
}
```

Read result:

```java
// print the first three columns for demo
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

#### PreparedStatement

We can get `PreparedStatement` from `SqlClusterExecutor`, e.g. get `InsertPreparedStmt` by `getInsertPreparedStmt`. There're three ways to use `InsertPreparedStmt`.
```{note}
Insertion only supports online, the execute mode won't affect it.
```

##### Normal Insert

1.  Using the `SqlClusterExecutor::getInsertPreparedStmt(db, insertSql)` interface to get the `InsertPrepareStatement`.
2. Using the `Statement::execute()` interface to execute the insert statement.

```java
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
      // PrepareStatement must be closed after it is used up
      pstmt.close();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
```

##### Use Placeholder to Execute Insert Statement

1. Using the `SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)` interface to` get the InsertPrepareStatement`.
2. Calling the `PreparedStatement::setType(index, value)` interface to fill data into `InsertPrepareStatement`.
3. Using the `Statement::execute()` interface to execute the insert statement.

```java
String insertSqlWithPlaceHolder = "insert into trans values(\"aa\", ?, 33, ?, 2.4, 1590738993000, \"2020-05-04\");";
PreparedStatement pstmt = null;
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
      // PrepareStatement must be closed after it is used up
      pstmt.close();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
```

##### Use Placeholder to Execute Batch Insert

1. Using the `SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)` interface to` get the InsertPrepareStatement`.
2. Calling the `PreparedStatement::setType(index, value)` interface to fill data into `InsertPrepareStatement`.
3. Using the `PreparedStatement::addBatch()` interface to build current row.
4. Using the `PreparedStatement::setType(index, value)` and `PreparedStatement::addBatch()` to add new rows.
5. Using the `PreparedStatement::executeBatch()` to execute batch insert.

```java
String insertSqlWithPlaceHolder = "insert into trans values(\"aa\", ?, 33, ?, 2.4, 1590738993000, \"2020-05-04\");";
PreparedStatement pstmt = null;
try {
  pstmt = sqlExecutor.getInsertPreparedStmt(db, insertSqlWithPlaceHolder);
  pstmt.setInt(1, 24);
  pstmt.setInt(2, 1.5f);
  pstmt.addBatch();
  pstmt.setInt(1, 25);
  pstmt.setInt(2, 1.6f);
  pstmt.addBatch();
  pstmt.executeBatch();
} catch (SQLException e) {
  e.printStackTrace();
  Assert.fail();
} finally {
  if (pstmt != null) {
    try {
      // PrepareStatement must be closed after it is used up
      pstmt.close();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
```

#### SQL Queries in the Request Mode

1. Using the `SqlClusterExecutor::getRequestPreparedStmt(db, selectSql)` interface to get the `RequestPrepareStatement`.
2. Calling the `PreparedStatement::setType(index, value)` interface to set the request data. Please call the `setType` interface and configure a valid value according to the data type corresponding to each column in the data table.
3. Calling the `Statement::executeQuery()` interface to execute the request query statement.

```java
String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
PreparedStatement pstmt = null;
ResultSet resultSet = null;
/*
c1 string,\n" +
                " c3 int,\n" +
                " c4 bigint,\n" +
                " c5 float,\n" +
                " c6 double,\n" +
                "c7 timestamp,\n" +
                " c8 date,\n" +
*/
try {
  // The first step, get RequestPrepareStatement
  pstmt= sqlExecutor.getRequestPreparedStmt(db, selectSql);
  
  // The second step, execute the request mode, you need to set a line of request data in RequestPreparedStatement
  pstmt.setString(1, "bb");
  pstmt.setInt(2, 24);
  pstmt.setLong(3, 34l);
  pstmt.setFloat(4, 1.5f);
  pstmt.setDouble(5, 2.5);
  pstmt.setTimestamp(6, new Timestamp(1590738994000l));
  pstmt.setDate(7, Date.valueOf("2020-05-05"));
  
  // Calling executeQuery will execute the select sql, the result in resultSet
  resultSet = pstmt.executeQuery();
  
  // access resultSet
  Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
  Assert.assertTrue(resultSet.next());
  Assert.assertEquals(resultSet.getString(1), "bb");
  Assert.assertEquals(resultSet.getInt(2), 24);
  Assert.assertEquals(resultSet.getLong(3), 34);
  
  // The returned result set of a normal request query contains only one row of results, so the result of the second call to resultSet.next() is false
  Assert.assertFalse(resultSet.next());
  
} catch (SQLException e) {
  e.printStackTrace();
  Assert.fail();
} finally {
  try {
    if (resultSet != null) {
      // need to close after result is used up
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

#### Delete all data under one key in specific index

There two methods to delete as below:

- use delete sql
- use delete preparestatement

```
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

### A Complete Example

See [Java quickstart demo](https://github.com/4paradigm/OpenMLDB/tree/main/demo/java_quickstart/demo). If macOS, add openmldb-native dependency and use the macos version.

You can run:
```
mvn package
java -cp target/demo-1.0-SNAPSHOT.jar com.openmldb.demo.App
```

## SDK Option

Connect to cluster must set `zkCluster` and `zkPath`(set methods or add `foo=bar` after `?` in jdbc url). Other options are optional.

Connect to standalone must set `host`, `port` and `isClusterMode`(`SDKOption.setClusterMode`). No jdbc supports. Notice that, `isClusterMode` is the required option, we can't detect it automatically now. Other options are optional.

### General Optional Options

We can set the options in cluster and standalone:
- enableDebug: default false. To enable the hybridse debug log(not the all log), you can see more log about sql compile and running. But the hybridse debug log may in tablet server log, the client won't collect all.
- requestTimeout: default 60000ms. To set the rpc timeout sent by client, exclude the rpc sent to taskmanager(job rpc timeout option is the variable `job_timeout`).
- glogLevel: default 0, the same to glog minloglevel. INFO, WARNING, ERROR, and FATAL are 0, 1, 2, and 3, respectively. so 0 will print INFO and higher levels。
- glogDir: default empty. When it's empty, it'll print to stderr.
- maxSqlCacheSize: default 50. The max cache num of one db in one sql mode(client side). If client met no cache error(e.g. get error `please use getInsertRow with ... first` but we did `getInsertRow` before), you can set it bigger.

### Optional Options for cluster

The OpenMLDB cluster has zk and taskmanager, so there're options about them：
- sessionTimeout: default 10000ms. the session timeout connect to zookeeper.
- zkLogLevel: default 3. 0-disable all zk log, 1-error, 2-warn, 3-info, 4-debug.
- zkLogFile: default empty. If empty, print log to stdout.
- sparkConfPath: default empty. set the spark conf file used by job in the client side, no need to set conf in taskmanager and restart it.

## SQL Validation

JAVA client supports validate if the sql can be executed or deployed, there're two modes: batch and request.

- `validateSQLInBatch` can validate if the sql can be executed on offline.

- `validateSQLInRequest` can validate if the sql can be deployed.

The two methods need all tables schema which need by sql, only support all tables in a single db, please **DO NOT** use `db.table` style in sql.