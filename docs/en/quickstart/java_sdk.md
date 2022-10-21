# Java SDK Quickstart

## 1. Package Installation

### Package Installation on Linux
Configure maven pom

```xml
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.6.3</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.6.3</version>
</dependency>
```
### Package Installation on Mac
Configure maven pom

```xml
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.6.3</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.6.3-macos</version>
</dependency>
```
Note that since `openmldb-native` contains the C++ static library compiled by OpenMLDB, by default it is a Linux's static library. On macOS, the version of the above openmldb-native needs to be changed to `0.6.3-macos`, and the version of openmldb-jdbc remains unchanged .

## 2. Quickstart

### 2.1 Create SqlClusterExecutor

First, the OpenMLDB connection parameters should be configured.

```java
SdkOption option = new SdkOption();
option.setZkCluster("127.0.0.1:2181");
option.setZkPath("/openmldb");
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);
```

Next, you should create an `SqlExecutor` using `SdkOption`. `SqlClusterExecutor` is thread-safe to execute SQL operations, thus you only need to create one `SqlClusterExecutor`:

```java
sqlExecutor = new SqlClusterExecutor(option);
```

### 2.2 Create Database

A database is created by using the `SqlClusterExecutor::createDB()` interface:

```java
sqlExecutor.createDB("db_test");
```

### 2.3 Create Table

A table is created by using the `SqlClusterExecutor::executeDDL(db, createTableSql)` interface:

```java
String createTableSql = "create table trans(c1 string,\n" +
                " c3 int,\n" +
                " c4 bigint,\n" +
                " c5 float,\n" +
                " c6 double,\n" +
                "c7 timestamp,\n" +
                " c8 date,\n" +
                "index(key=c1, ts=c7));";
sqlExecutor.executeDDL("", createTableSql);
```

### 2.4 Insert Data into a Table

#### 2.4.1 Insert Data Directly

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

#### 2.4.2 Use Placeholder to Execute Insert Statement

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

#### 2.4.2 Use Placeholder to Execute Batch Insert

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

### 2.5 Execute SQL Batch Query

1. Using the `SqlClusterExecutor::executeSQL(selectSql)` interface to execute SQL batch query statements:

```java
String selectSql = "select * from trans;";
java.sql.ResultSet result = sqlExecutor.executeSQL(db, selectSql);
```

2. Accessing query results:

```java
// Access the result set ResultSet, and output the first three columns of data
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

### 2.6 SQL Queries in the Request Mode

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

### 2.7 Delete a Table

You should use the `SqlClusterExecutor::executeDDL(db, dropTableSql)` interface to delete a table:

```java
String dropTableSql = "drop table trans;";
sqlExecutor.executeDDL(db, dropTableSql);
```

### 2.8 Delete a Database

You should use the `SqlClusterExecutor::dropDB(db)` interface to drop a specified database:

```java
sqlExecutor.dropDB(db);
```

### 2.9 Delete all data under one key in specific index

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

## 3. A Complete Example

```java
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.sdk.*;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.testng.Assert;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Demo {

    private SqlExecutor sqlExecutor = null;
    private String db = "mydb16";
    private String table = "trans";
    private String sp = "sp";

    public static void main(String[] args) {
        Demo demo = new Demo();
        try {
            // Initialize the construction of SqlExecutor
            demo.init();
            demo.createDataBase();
            demo.createTable();
            // Insert by insert statement
            demo.insertWithoutPlaceholder();
            // Insert by way of placeholder. The placeholder method will not compile sql repeatedly, and its performance will be much better than direct insert
            demo.insertWithPlaceholder();
            // Execute the select statement
            demo.select();
            // Execute sql in request mode
            demo.requestSelect();
            // Delete table
            demo.dropTable();
            // Delete the database
            demo.dropDataBase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init() throws SqlException {
        SdkOption option = new SdkOption();
        option.setZkCluster("172.27.128.37:7181");
        option.setZkPath("/rtidb_wb");
        option.setSessionTimeout(10000);
        option.setRequestTimeout(60000);
        // sqlExecutor is multi-threaded safe to execute sql operations, and only one can be created in the actual environment
        sqlExecutor = new SqlClusterExecutor(option);
    }

    private void createDataBase() {
        Assert.assertTrue(sqlExecutor.createDB(db));
    }

    private void dropDataBase() {
        Assert.assertTrue(sqlExecutor.dropDB(db));
    }

    private void createTable() {
        String createTableSql = "create table trans(c1 string,\n" +
                " c3 int,\n" +
                " c4 bigint,\n" +
                " c5 float,\n" +
                " c6 double,\n" +
                "c7 timestamp,\n" +
                " c8 date,\n" +
                "index(key=c1, ts=c7));";
        Assert.assertTrue(sqlExecutor.executeDDL(db, createTableSql));
    }

    private void dropTable() {
        String dropTableSql = "drop table trans;";
        Assert.assertTrue(sqlExecutor.executeDDL(db, dropTableSql));
    }

    private void getInputSchema(String selectSql) {
        try {
            Schema inputSchema = sqlExecutor.getInputSchema(db, selectSql);
            Assert.assertEquals(inputSchema.getColumnList().size(), 7);
            Column column = inputSchema.getColumnList().get(0);
            Assert.assertEquals(column.getColumnName(), "c1");
            Assert.assertEquals(column.getSqlType(), Types.VARCHAR);
            Assert.assertEquals(column.isConstant(), false);
            Assert.assertEquals(column.isNotNull(), false);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private void insertWithoutPlaceholder() {
        String insertSql = "insert into trans values(\"aa\",23,33,1.4,2.4,1590738993000,\"2020-05-04\");";
        PreparedStatement pstmt = null;
        try {
            pstmt = sqlExecutor.getInsertPreparedStmt(db, insertSql);
            Assert.assertTrue(pstmt.execute());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            if (pstmt != null) {try {
                    // PrepareStatement must be closed after it is used up
                    pstmt.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    private void insertWithPlaceholder() {
        String insertSql = "insert into trans values(\"aa\", ?, 33, ?, 2.4, 1590738993000, \"2020-05-04\");";
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
        String selectSql = "select * from trans;";
        java.sql.ResultSet result = sqlExecutor.executeSQL(db,selectSql);
        int num = 0;
        try {
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
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

        }
        // result data analysis refer to the requestSelect method below
        Assert.assertEquals(num, 2);
    }

    private void requestSelect() {
        String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            pstmt = sqlExecutor.getRequestPreparedStmt(db, selectSql);
            // If you are executing deployment, you can get preparedstatement by name
            //pstmt = sqlExecutor.getCallablePreparedStmt(db, deploymentName);
            ResultSetMetaData metaData = pstmt.getMetaData();
            // To execute the request mode, you need to set a line of request data in RequestPreparedStatement
            setData(pstmt, metaData);
            // Calling executeQuery will execute the select sql, and then put the result in resultSet
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
    }

    private void batchRequestSelect() {
         String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            List<Integer> list = new ArrayList<Integer>();
            pstmt = sqlExecutor.getBatchRequestPreparedStmt(db, selectSql, list);
            // If you are executing deployment, you can get preparedstatement by name
            // pstmt = sqlExecutor.getCallablePreparedStmtBatch(db, deploymentName);
            ResultSetMetaData metaData = pstmt.getMetaData();
            // To execute request mode, you need to set PreparedStatement to request data
            // Set how many pieces of data to send in a batch
            int batchSize = 5;
            for (int idx = 0; idx < batchSize; idx++) {
                setData(pstmt, metaData);
                // After each row of data is set, addBatch needs to be called once
                pstmt.addBatch();
            }
            // Calling executeQuery will execute the select sql, and then put the result in resultSet
            resultSet = pstmt.executeQuery();
            // Take out the feature results corresponding to each data in turn
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
                    //result need to close after use
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
            } else if (columnType == Types. BIGINT) {
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
```

## 4. JDBC Connect
We can use the JDBC way to connect OpenMLDB(only support OpenMLDB cluster now):
```
Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
// No database in jdbcUrl
Connection connection = DriverManager.getConnection("jdbc:openmldb:///?zk=localhost:6181&zkPath=/openmldb");

// Set database in jdbcUrl
Connection connection1 = DriverManager.getConnection("jdbc:openmldb:///test_db?zk=localhost:6181&zkPath=/openmldb");
```

No database in jdbc url is fine, but set the database in jdbc url is recommended. After connected, the default execute mode is online.

You can create `Statement` by connection, to execute all sql, and set the execute mode. For example:
```
Statement stmt = connection.createStatement();
stmt.execute("SELECT * from t1");
```

`PreparedStatement` supports `SELECT`, `INSERT` and `DELETE`. And only supports insert into online.
```
PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM t1 WHERE id=?");
PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO t1 VALUES (?,?)");
PreparedStatement insertStatement = connection.prepareStatement("DELETE FROM t1 WHERE id=?");
```

## 5. SDK Option

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

##  6. SQL Validation

JAVA client supports validate if the sql can be executed or deployed, there're two modes: batch and request.

- `validateSQLInBatch` can validate if the sql can be executed on offline.

- `validateSQLInRequest` can validate if the sql can be deployed.

The two methods need all tables schema which need by sql, only support all tables in a single db, please **DO NOT** use `db.table` style in sql.