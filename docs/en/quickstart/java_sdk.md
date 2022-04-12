# Java SDK Quickstart

Notice: The Java SDK currently only supports the cluster version, and the standalone version is planned to be supported in the next version v0.5.0.

## 1. Java SDK package installation

### Java SDK package installation on Linux
configure maven pom

```xml
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.4.3</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.4.3</version>
</dependency>
```
### Java SDK package installation on Mac
configure maven pom

```xml
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.4.3</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.4.3-macos</version>
</dependency>
```
Notice: Since openmldb-native contains the C++ static library compiled by OpenMLDB, the default is the linux static library. On macOS, the version of the above openmldb-native needs to be changed to `0.4.3-macos`, and the version of openmldb-jdbc remains unchanged .

## 2. Java SDK Quick Start

### 2.1 Create SqlClusterExecutor

First, configure the OpenMLDB connection parameters

```java
SdkOption option = new SdkOption();
option.setZkCluster("127.0.0.1:2181");
option.setZkPath("/openmldb");
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);

```

Next, create an Executor using SdkOption. SqlClusterExecutor is thread-safe to execute SQL operations. In the actual environment, just create one `SqlClusterExecutor`:

```java
sqlExecutor = new SqlClusterExecutor(option);
```

### 2.2 Create database

Create a database using the `SqlClusterExecutor::createDB()` interface:

```java
sqlExecutor.createDB("db_test");
```

### 2.3 Create table

Create a table using the `SqlClusterExecutor::executeDDL(db, createTableSql)` interface:

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

### 2.4 Insert data into the table

#### 2.4.1 Insert data directly

The first step, use the `SqlClusterExecutor::getInsertPreparedStmt(db, insertSql)` interface to get the InsertPrepareStatement.

The second step, use the `Statement::execute()` interface to execute the insert statement.

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

#### 2.4.2 Use placeholder to execute insert statement

The first step, use the `SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)` interface to get the InsertPrepareStatement.

The second step, call the `PreparedStatement::setType(index, value)` interface to fill data into InsertPrepareStatement.

The third step, use the `Statement::execute()` interface to execute the insert statement.

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

### 2.5 Execute SQL batch query

Use the `SqlClusterExecutor::executeSQL(selectSql)` interface to execute SQL batch query statements:

```java
String selectSql = "select * from trans;";
java.sql.ResultSet result = sqlExecutor.executeSQL(db, selectSql);
```

Access query results:

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

### 2.6 SQL Queries in the request mode

The first step, use the `SqlClusterExecutor::getRequestPreparedStmt(db, selectSql)` interface to get the RequestPrepareStatement.

The second step, call the `PreparedStatement::setType(index, value)` interface to set the request data. Please call the setType interface and configure a legal value according to the data type corresponding to each column in the data table.

The third step, call the `Statement::executeQuery()` interface to execute the request query statement.

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

### 2.7 Delete table

Use the `SqlClusterExecutor::executeDDL(db, dropTableSql)` interface to delete a table:

```java
String dropTableSql = "drop table trans;";
sqlExecutor.executeDDL(db, dropTableSql);
```

### 2.8 Delete database

Use the `SqlClusterExecutor::dropDB(db)` interface to drop the specified database:

```java
sqlExecutor.dropDB(db);
```

## 3. A Complete Java SDK usage example

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
