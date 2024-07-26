# Java SDK

In Java SDK, the default execution mode for JDBC Statements is online, while the default execution mode for SqlClusterExecutor is offline. Please keep this in mind.

## Java SDK Installation

- Install Java SDK on Linux

  Configure the maven pom:

```XML
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.9.1</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.9.1</version>
</dependency>
```

- Install Java SDK on Mac

  Configure the maven pom

```XML
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.9.1</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.9.1-macos</version>
</dependency>
```

Note: Since the openmldb-native package contains the C++ static library compiled for OpenMLDB, it defaults to the Linux static library. For macOS, the version of openmldb-native should be changed to `0.9.1-macos`, while the version of openmldb-jdbc remains unchanged.

The macOS version of openmldb-native only supports macOS 12. To run it on macOS 11 or macOS 10.15, the openmldb-native package needs to be compiled from the source code on the corresponding OS. For detailed compilation methods, please refer to [Java SDK](../../deploy/compile.md#Build-java-sdk-with-multi-processes). 
When using a self-compiled openmldb-native package, it is recommended to install it into your local Maven repository using `mvn install`. After that, you can reference it in your project's pom.xml file. It's not advisable to reference it using `scope=system`.

To connect to the OpenMLDB service using the Java SDK, you can use JDBC (recommended) or connect directly through SqlClusterExecutor. The following will demonstrate both connection methods.

## Connection with JDBC 
```java
Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
// No database in jdbcUrl
Connection connection = DriverManager.getConnection("jdbc:openmldb:///?zk=localhost:6181&zkPath=/openmldb");

// Set database in jdbcUrl
Connection connection1 = DriverManager.getConnection("jdbc:openmldb:///test_db?zk=localhost:6181&zkPath=/openmldb");

// Set user and password in jdbcUrl
Connection connection = DriverManager.getConnection("jdbc:openmldb:///?zk=localhost:6181&zkPath=/openmldb&user=root&password=123456");
```

The database specified in the Connection address must exist when creating the connection.

```{caution}
The default execution mode for JDBC Connection is `online`.
```

### Statement

All SQL commands can be executed using `Statement`, both in online and offline modes. To switch between offline and online modes, use command `SET @@execute_mode='...';`. For example:

```java
Statement stmt = connection.createStatement();
stmt.execute("SET @@execute_mode='offline"); // Switch to offline mode
stmt.execute("SELECT * from t1"); // Offline select
ResultSet res = stmt.getResultSet(); // The ResultSet of the previous execute

stmt.execute("SET @@execute_mode='online"); // Switch to online mode
res = stmt.executeQuery("SELECT * from t1"); // For online mode, select or executeQuery can directly obtain the ResultSet result.
```

The `LOAD DATA` command is an asynchronous command, and the returned ResultSet contains information such as the job ID and state. You can execute `show job <id>` to check if the job has been completed. Note that the ResultSet needs to execute `next()` method to move the cursor to the first row of data.

In offline mode, the default behavior is asynchronous execution, and the ResultSet returned is a Job Info. You can change this behavior to synchronous execution using `SET @@sync_job=true;`. However, please note that the ResultSet returned can vary depending on the specific SQL command. For more details, please refer to the [Function Boundary](../function_boundary.md). Synchronous execution is recommended when using `LOAD DATA` or `SELECT INTO` commands.

If synchronous commands are timing out, you can adjust the configuration as described in the [Offline Command Configuration](../../openmldb_sql/ddl/SET_STATEMENT.md).

```{caution}
When you execute `SET @@execute_mode='offline'` on a `Statement`, it not only affects the current `Statement` but also impacts all `Statement` objects created, both existing and yet to be created, within the same `Connection`. Therefore, it is not advisable to create multiple `Statement` objects and expect them to execute in different modes. If you need to execute SQL in different modes, it's recommended to create multiple `Connection`.
```

### PreparedStatement 

`PreparedStatement` supports `SELECT`, `INSERT`, and `DELETE`.
```{warning}
Any `PreparedStatement` executes only in the **online mode** and is not affected by the state before the `PreparedStatement` is created. `PreparedStatement` does not support switching to the offline mode. If you need to execute SQL in the offline mode, you can use a `Statement`.

There are three types of `PreparedStatement` created by a `Connection`, which correspond to `getPreparedStatement`, `getInsertPreparedStmt`, and `getDeletePreparedStm`t in SqlClusterExecutor.
```

```java
PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM t1 WHERE id=?");
PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO t1 VALUES (?,?)");
PreparedStatement insertStatement = connection.prepareStatement("DELETE FROM t1 WHERE id=?");
```

## SqlClusterExecutor 
`SqlClusterExecutor` is the most comprehensive Java SDK connection method. It not only provides the basic CRUD operations that you can use with JDBC but also offers additional features like request modes and more.

### Create a SqlClusterExecutor 

First, configure the OpenMLDB connection parameters.

```java
SdkOption option = new SdkOption();
option.setZkCluster("127.0.0.1:2181");
option.setZkPath("/openmldb");
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);
// If not specified, it defaults to 'root'
option.setUser("root");
// If not specified, it defaults to being empty
option.setPassword("123456");
```
Then, use SdkOption to create the Executor.

```java
sqlExecutor = new SqlClusterExecutor(option);
```

`SqlClusterExecutor` execution of SQL operations is thread-safe, and in actual environments, a single `SqlClusterExecutor` can be created. However, since the execution mode (`execute_mode`) is an internal variable of `SqlClusterExecutor`, if you want to execute an offline command and an online command at the same time, unexpected results may occur. In this case, please use multiple `SqlClusterExecutors`.

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

#### Execute Batch SQL Queries with Statement 

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

`SqlClusterExecutor` can also obtain `PreparedStatement`, but you need to specify which type of `PreparedStatement` to obtain. For example, when using `InsertPreparedStmt` for insertion operations, there are three ways to do it.

```{note}
Any `PreparedStatement` executes exclusively in the **online mode** and is not influenced by the state of the `SqlClusterExecutor` at the time of its creation. `PreparedStatement` does not support switching to the offline mode. If you need to execute SQL in the offline mode, you can use a `Statement`.
```

#### Common Insert

1. Use the `SqlClusterExecutor::getInsertPreparedStmt(db, insertSql)` method to get the `InsertPrepareStatement`.
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

#### Insert with Placeholder

1. Get `InsertPrepareStatement` by calling `SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)` interface.
2. Use `PreparedStatement::setType(index, value)` interface to fill in data to the `InsertPrepareStatement`. Note that the index starts from 1.
3. For String, Date and Timestamp types, null objects can be set using either `setType(index, null)` or `setNull(index)`.
4. Use `PreparedStatement::execute()` interface to execute the insert statement.

```{note}
When the conditions of the `PreparedStatement` are the same, you can repeatedly call the set method of the same object to fill in data before executing `execute`. There is no need to create a new `PreparedStatement` object.
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
After `execute`, the cached data will be cleared and it is not possible to rerun `execute`.
```

#### Batch Insert with Placeholder

1. To use batch insert, first obtain the `InsertPrepareStatement` using the `SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)` interface.
2. Then use the `PreparedStatement::setType(index, value)` interface to fill data into the `InsertPrepareStatement`.
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
After `executeBatch`, all cached data will be cleared and it's not possible to rerun `executeBatch`.
```

### Execute SQL Query 

`RequestPreparedStmt` is a unique query mode (not supported by JDBC). This mode requires both the selectSql and a request data, so you need to provide the SQL and set the request data using `setType` when calling `getRequestPreparedStmt`.

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
    
    // The return result set of the ordinary request query contains only one row of results. Therefore, the result of the second call to resultSet.next() is false
    Assert.assertFalse(resultSet.next());
  
} catch (SQLException e) {
    e.printStackTrace();
    Assert.fail();
} finally {
    try {
        if (resultSet != null) {
        // close result
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
### Execute Deployment
To execute a deployment, you can use the `SqlClusterExecutor::getCallablePreparedStmt(db, deploymentName)` interface to obtain a `CallablePreparedStatement`. In contrast to the SQL request-based queries mentioned earlier, deployments are already online on the server, which makes them faster compared to SQL request-based queries.

The process of using a deployment consists of two steps:
- Online Deployment
```java
// Deploy online (use selectSql). In a real production environment, deployments are typically already online and operational.
java.sql.Statement state = sqlExecutor.getStatement();
try {
    String selectSql = String.format("SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM %s WINDOW w1 AS " +
            "(PARTITION BY %s.c1 ORDER BY %s.c7 ROWS_RANGE BETWEEN 2d PRECEDING AND CURRENT ROW);", table,
            table, table);
    // Deploy
    String deploySql = String.format("DEPLOY %s OPTIONS(RANGE_BIAS='inf', ROWS_BIAS='inf') %s", deploymentName, selectSql);
    // set return null rs, don't check the returned value, it's false
    state.execute(deploySql);
} catch (Exception e) {
    e.printStackTrace();
}
```
- Execute Deployment
When executing a deployment, recreating a `CallablePreparedStmt` can be time-consuming. It is recommended to reuse the `CallablePreparedStmt` whenever possible. The `executeQuery()` method will automatically clear the request row cache for `setXX` requests.

```java
// Execute Deployment
PreparedStatement pstmt = null;
ResultSet resultSet = null;
try {
    pstmt = sqlExecutor.getCallablePreparedStmt(db, deploymentName);
    // Obtain preparedstatement with name
    // pstmt = sqlExecutor.getCallablePreparedStmt(db, deploymentName);
    ResultSetMetaData metaData = pstmt.getMetaData();
    // Execute request mode requires setting query data in RequestPreparedStatement
    setData(pstmt, metaData);
    // executeQuery will execute select sql, and put result in resultSet
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
            // close result
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

### Delete All Data of a Key under the Specified Index

There are two ways to delete data through the Java SDK:

- Execute delete SQL directly
- Use delete PreparedStatement

Note that this can only delete data under one index, not all indexes. Refer to [DELETE function boundary](../function_boundary.md#delete) for details.

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

### A Complete Example of SqlClusterExecutor

Refer to the [Java quickstart demo](https://github.com/4paradigm/OpenMLDB/tree/main/demo/java_quickstart/demo). If it is used on macOS, please use openmldb-native of macOS version and increase the dependency of openmldb-native.

Compile and run:

```
mvn package
java -cp target/demo-1.0-SNAPSHOT.jar com.openmldb.demo.App
```

## SDK Configuration Details

You must fill in `zkCluster` and `zkPath` (set method or the configuration `foo=bar` after `?` in JDBC).

### Optional Configuration

| Optional Configuration | Description                                                  |
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

## SQL Validation

The Java client supports the verification of SQL to verify whether it is executable. It is divided into batch and request modes.

- `ValidateSQLInBatch` can verify whether SQL can be executed offline.
- `ValidateSQLInRequest` can verify whether SQL can be deployed online.

Both interfaces require providing all the table schemas required by the SQL and support multiple databases. For backward compatibility, it's allowed not to specify the db (current database in use) in the parameters. In such cases, it's equivalent to using the first db listed in use schema. It's important to ensure that the `<table>` format tables are from the first db, which doesn't affect SQL statements in the `<db>.<table>` format.

For example, verify SQL `select count (c1) over w1 from t3 window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row);`, In addition to this statement, you need to go through in the schema of table `t3` as the second parameter schemaMaps. The format is Map, key is the name of the db, and value is all the table schemas (maps) of each db. In fact, only a single db is supported, so there is usually only one db here, as shown in db3 below. The table schema map key under db is table name, and the value is `com._ 4paradigm.openmldb.sdk.Schema`, consisting of the name and type of each column.

The return result is a `List<String>`. If the validation is successful, it returns an empty list. If the validation fails, it returns a list of error messages, such as `[error_msg, error_trace]`.

```java
Map<String, Map<String, Schema>> schemaMaps = new HashMap<>();
Map<String, Schema> dbSchema = new HashMap<>();
dbSchema = new HashMap<>();
dbSchema.put("t3", new Schema(Arrays.asList(new Column("c1", Types.VARCHAR), new Column("c2", Types.BIGINT))));
schemaMaps.put("db3", dbSchema);
List<String> ret = SqlClusterExecutor.validateSQLInRequest("select count(c1) over w1 from t3 window "+
        "w1 as(partition by c1 order by c2 rows between unbounded preceding and current row);", schemaMaps);
Assert.assertEquals(ret.size(), 0);

Map<String, Map<String, Schema>> schemaMaps = new HashMap<>();
Map<String, Schema> dbSchema = new HashMap<>();
dbSchema = new HashMap<>();
dbSchema.put("t3", new Schema(Arrays.asList(new Column("c1", Types.VARCHAR), new Column("c2", Types.BIGINT))));
schemaMaps.put("db3", dbSchema);
// Can use parameter format of no db. Make sure that there's only one db in schemaMaps，and only <table> format is used in sql.
// List<String> ret = SqlClusterExecutor.validateSQLInRequest("select count(c1) over w1 from t3 window "+
//        "w1 as(partition by c1 order by c2 rows between unbounded preceding and current row);", schemaMaps);
List<String> ret = SqlClusterExecutor.validateSQLInRequest("select count(c1) over w1 from t3 window "+
        "w1 as(partition by c1 order by c2 rows between unbounded preceding and current row);", "db3", schemaMaps);
Assert.assertEquals(ret.size(), 0);
```

## DDL Generation

The `public static List<String> genDDL(String sql, Map<String, Map<String, Schema>> tableSchema)` method can help users generate table creation statements based on the SQL they want to deploy. It currently supports only a **single** database. The `sql` parameter should not be in the `<db>.<table>` format. The `tableSchema` parameter should include the schemas of all tables that the SQL depends on. The format of `tableSchema` should be consistent with what was discussed earlier. Even if `tableSchema` contains multiple databases, the database information will be discarded, and all tables will be treated as if they belong to an unknown database.

## SQL Output Schema

The `public static Schema genOutputSchema(String sql, String usedDB, Map<String, Map<String, Schema>> tableSchema)` method allows you to obtain the Output Schema for SQL queries and supports multiple databases. If you specify the `usedDB`, you can use tables from that database within the SQL using the `<table>` format. For backward compatibility, there is also support for the` public static Schema genOutputSchema(String sql, Map<String, Map<String, Schema>> tableSchema)` method without specifying a database (usedDB). In this case, it is equivalent to using the first database listed as the used db. Therefore, you should ensure that tables in `<table>` format within the `SQ`L query are associated with this first database.


## SQL Table Lineage
The `public static List<Pair<String, String>> getDependentTables(String sql, String usedDB, Map<String, Map<String, Schema>> tableSchema)` method allows you to retrieve all tables that the sql query depends on. Each `Pair<String, String>` in the list corresponds to the database name and table name, with the first element being the primary table, and the rest `[1, end)` representing other dependent tables (excluding the primary table). If the input parameter `usedDB` is an empty string, it means the query is performed without specifying a database (use db) context, which is different from the compatibility rules mentioned earlier for methods like `genDDL`.

## SQL Merge
The Java client supports merging multiple SQL statements and performs correctness validation in request mode using the `mergeSQL` interface. However, it's important to note that merging is only possible when all the input SQL statements have the same primary table.

Input parameters: SQL group to be merged; the name of the current database being used; the join key(s) for the primary table (which can be multiple); the schema for all tables involved.

For example, let's consider four SQL feature views:
```
// Single-table direct feature
select c1 from main;
// Single-table aggregation feature
select sum(c1) over w1 of2 from main window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row);
// Multi-table feature
select t1.c2 of4 from main last join t1 order by t1.c2 on main.c1==t1.c1;
// Multi-table aggregation feature
select sum(c2) over w1 from main window w1 as (union (select \"\" as id, * from t1) partition by c1 order by c2 rows between unbounded preceding and current row);
```

Since all of them have the same primary table, "main," they can be merged. The merging process is essentially a join operation. To perform this operation, you also need to specify a unique column in the "main" table that can be used to identify a unique row of data. For example, if the "id" column in the "main" table is not unique and there may be multiple rows with the same "id" values, you can use a combination of "id" and "c1" columns for the join. Similar to SQL validation, you would also provide a schema map for the tables involved in the merge.


```java
//To simplify the demonstration, we are using tables from a single database, so you only need to specify used db and table names if your SQL statements all use the <table> format. you can leave the used database parameter as an empty string. If your SQL statements use the <db>.<table> format, you can leave the used db parameter as an empty string
String merged = SqlClusterExecutor.mergeSQL(sqls, "db", Arrays.asList("id", "c1"), schemaMaps);
```

The output is a single merged SQL statement, as shown below. The input SQL includes a total of four features, so the merged SQL will only output these four feature columns. (The join keys are automatically filtered.)

```
select `c1`, `of2`, `of4`, `sum(c2)over w1` from (select main.id as merge_id_0, c1 from main) as out0 last join (select main.id as merge_id_1, sum(c1) over w1 of2 from main window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row)) as out1 on out0.merge_id_0 = out1.merge_id_1 last join (select main.id as merge_id_2, t1.c2 of4 from main last join t1 order by t1.c2 on main.c1==t1.c1) as out2 on out0.merge_id_0 = out2.merge_id_2 last join (select main.id as merge_id_3, sum(c2) over w1 from main window w1 as (union (select "" as id, * from t1) partition by c1 order by c2 rows between unbounded preceding and current row)) as out3 on out0.merge_id_0 = out3.merge_id_3;
```

```{note}
If you encounter an "Ambiguous column name" error during the merging process, it may be due to having the same column names in different feature groups. To resolve this, you should use aliases in your input SQL to distinguish between them.
```



