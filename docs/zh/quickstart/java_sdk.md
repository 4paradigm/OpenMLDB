# Java SDK 快速上手

## 1. Java SDK包安装

### Linux下 Java SDK包安装
配置maven pom

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
### Mac下Java SDK包安装
配置maven pom

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
注意: 由于 openmldb-native 中包含了 OpenMLDB 编译的 C++ 静态库, 默认是 linux 静态库, macOS 上需将上述 openmldb-native 的 version 改成 `0.4.3-macos`, openmldb-jdbc 的版本保持不变。

## 2. Java SDK快速上手

### 2.1 创建SqlClusterExecutor

首先，进行OpenMLDB连接参数配置，java sdk集群版和单机版的区别在于连接参数配置不同，默认是集群版

```java
// 集群版配置方式如下：
SdkOption option = new SdkOption();
option.setZkCluster("127.0.0.1:2181");
option.setZkPath("/openmldb");
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);

// 单机版配置方式如下：
SdkOption option = new SdkOption();
option.setHost("127.0.0.1");
option.setPort(6527);
option.setClusterMode(false);
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);
```

接着，使用SdkOption创建Executor。SqlClusterExecutor执行sql操作是多线程安全的，在实际环境中只创建一个`SqlClusterExecutor`即可:

```java
sqlExecutor = new SqlClusterExecutor(option);
```

### 2.2 创建数据库

使用`Statement::execute`接口创建数据库：

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

### 2.3 创建表

使用`Statement::execute`接口创建一张表：

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

### 2.4 插入数据到表中

#### 2.4.1 直接执行插入数据

第一步，使用`SqlClusterExecutor::getInsertPreparedStmt(db, insertSql)`接口获取InsertPrepareStatement。

第二步，使用`PreparedStatement::execute()`接口执行insert语句。

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
            // PrepareStatement用完之后必须close
            pstmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
```

#### 2.4.2 使用placeholder的方式执行插入语句

第一步，使用`SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)`接口获取InsertPrepareStatement。

第二步，调用`PreparedStatement::setType(index, value)`接口，填充数据到InsertPrepareStatement中。

第三步，使用`PreparedStatement::execute()`接口执行insert语句。

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
      // PrepareStatement用完之后必须close
      pstmt.close();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
```
```{note}
execute后，缓存的数据将被清除，无法重试execute。
```

#### 2.4.3 使用placeholder的方式执行批量插入语句

第一步，使用`SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)`接口获取InsertPrepareStatement。

第二步，调用`PreparedStatement::setType(index, value)`接口，填充数据到InsertPrepareStatement中。

第三步，使用`PreparedStatement::addBatch()`接口完成一行的填充。

第四步，继续使用`setType`和`addBatch`，填充多行。

第五步，使用`PreparedStatement::addBatch()`接口完成批量插入。

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
      // PrepareStatement用完之后必须close
      pstmt.close();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
```
```{note}
executeBatch后，缓存的所有数据将被清除，无法重试executeBatch。
```

### 2.5 执行SQL批式查询

使用`Statement::execute`接口执行SQL批式查询语句:

```java
java.sql.Statement state = sqlExecutor.getStatement();
try {
    state.execute("use db_test");
    // execute返回值是true的话说明有数据返回，可以通过getResultSet获取
    boolean ret = state.execute("select * from trans;");
    Assert.assertTrue(ret);
    java.sql.ResultSet rs = state.getResultSet();
} catch (Exception e) {
    e.printStackTrace();
}
```

访问查询结果:

```java
// 访问结果集ResultSet，并输出前三列数据
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

### 2.6 执行SQL请求式查询

第一步，使用`SqlClusterExecutor::getRequestPreparedStmt(db, selectSql)`接口获取RequestPrepareStatement。

第二步，调用`PreparedStatement::setType(index, value)`接口设置请求数据。请根据数据表中每一列对应的数据类型调用setType接口以及配置合法的值。

第三步，调用`Statement::executeQuery()`接口执行请求式查询语句。

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
    // 第一步，获取RequestPrepareStatement
    pstmt = sqlExecutor.getRequestPreparedStmt(db, selectSql);
    
    // 第二步，执行request模式需要在RequestPreparedStatement设置一行请求数据
    pstmt.setString(1, "bb");
    pstmt.setInt(2, 24);
    pstmt.setLong(3, 34l);
    pstmt.setFloat(4, 1.5f);
    pstmt.setDouble(5, 2.5);
    pstmt.setTimestamp(6, new Timestamp(1590738994000l));
    pstmt.setDate(7, Date.valueOf("2020-05-05"));
    
    // 调用executeQuery会执行这个select sql, 然后将结果放在了resultSet中
    resultSet = pstmt.executeQuery();
    
    // 访问resultSet
    Assert.assertEquals(resultSet.getMetaData().getColumnCount(), 3);
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(resultSet.getString(1), "bb");
    Assert.assertEquals(resultSet.getInt(2), 24);
    Assert.assertEquals(resultSet.getLong(3), 34);
    
    // 普通请求式查询的返回结果集只包含一行结果，因此，第二次调用resultSet.next()结果为false
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

### 2.7 删除表

使用`Statement::execute`接口删除一张表：

```java
java.sql.Statement state = sqlExecutor.getStatement();
try {
    state.execute("use db_test");
    state.execute("drop table trans;");
} catch (Exception e) {
    e.printStackTrace();
}
```

### 2.8 删除数据库

使用`Statement::execute`接口删除指定数据库：

```java
java.sql.Statement state = sqlExecutor.getStatement();
try {
    state.execute("drop database db_test;");
} catch (Exception e) {
    e.printStackTrace();
} finaly {
    state.close();
}
```



## 3. 完整的Java SDK使用范例


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
            // 初始化构造SqlExecutor
            demo.init();
            demo.createDataBase();
            demo.createTable();
            // 通过insert语句插入
            demo.insertWithoutPlaceholder();
            // 通过placeholder的方式插入。placeholder方式不会重复编译sql, 在性能上会比直接insert好很多
            demo.insertWithPlaceholder();
            // 执行select语句
            demo.select();
            // 在request模式下执行sql
            demo.requestSelect();
            // 删除表
            demo.dropTable();
          	// 删除数据库
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
        // sqlExecutor执行sql操作是多线程安全的，在实际环境中只创建一个即可
        sqlExecutor = new SqlClusterExecutor(option);
    }

    private void createDataBase() {
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            state.execute("create database " + db + ";");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dropDataBase() {
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            state.execute("drop database " + db + ";");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createTable() {
        String createTableSql = "create table trans(c1 string,\n" +
                "                   c3 int,\n" +
                "                   c4 bigint,\n" +
                "                   c5 float,\n" +
                "                   c6 double,\n" +
                "                   c7 timestamp,\n" +
                "                   c8 date,\n" +
                "                   index(key=c1, ts=c7));";
        try {
            state.execute("use " + db + ";");
            state.execute(createTableSql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dropTable() {
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            state.execute("drop table trans;");
        } catch (Exception e) {
            e.printStackTrace();
        }
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
            if (pstmt != null) {
                try {
                    // PrepareStatement用完之后必须close
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
        java.sql.ResultSet result = null;
        int num = 0;
        java.sql.Statement state = sqlExecutor.getStatement();
        try {
            boolean ret = state.execute(selectSql);
            Assert.assertTrue(ret);
            result = state.getResultSet();
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
                state.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

        }
        // result数据解析参考下面requestSelect方法
        Assert.assertEquals(num, 2);
    }

    private void requestSelect() {
        String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            pstmt = sqlExecutor.getRequestPreparedStmt(db, selectSql);
            // 如果是执行deployment, 可以通过名字获取preparedstatement
            //pstmt = sqlExecutor.getCallablePreparedStmt(db, deploymentName);
            ResultSetMetaData metaData = pstmt.getMetaData();
            // 执行request模式需要在RequestPreparedStatement设置一行请求数据
            setData(pstmt, metaData);
            // 调用executeQuery会执行这个select sql, 然后将结果放在了resultSet中
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
    }

    private void batchRequestSelect() {
         String selectSql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS " +
                "(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            List<Integer> list = new ArrayList<Integer>();
            pstmt = sqlExecutor.getBatchRequestPreparedStmt(db, selectSql, list);
            // 如果是执行deployment, 可以通过名字获取preparedstatement 
            // pstmt = sqlExecutor.getCallablePreparedStmtBatch(db, deploymentName);
            ResultSetMetaData metaData = pstmt.getMetaData();
            // 执行request模式需要在设置PreparedStatement请求数据
            // 设置一个batch发送多少条数据
            int batchSize = 5;
            for (int idx = 0; idx < batchSize; idx++) {
                setData(pstmt, metaData);
                // 每次设置完一行数据后需要调用一次addBatch
                pstmt.addBatch();
            }
            // 调用executeQuery会执行这个select sql, 然后将结果放在了resultSet中
            resultSet = pstmt.executeQuery();
            // 依次取出每一条数据对应的特征结果
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
            } else if (columnType == Types.BIGINT) {
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

## 4. JDBC连接方式
除了直接使用SDK外，我们还提供了JDBC的方式，目前只能连接集群版OpenMLDB。连接方式如下：

```
Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
// No database in jdbcUrl
Connection connection = DriverManager.getConnection("jdbc:openmldb:///?zk=localhost:6181&zkPath=/openmldb");

// Set database in jdbcUrl
Connection connection1 = DriverManager.getConnection("jdbc:openmldb:///test_db?zk=localhost:6181&zkPath=/openmldb");
```

未设置db的Connection功能有限，更推荐创建Connection时就指定db。

默认为在线模式（之后会调整为“默认离线”）。

通过`Statement`的方式可以执行所有的sql命令，离线在线均可。切换为离线模式，仍使用`SET @@execute_mode='offline';`的方式。例如：
```
Statement stmt = connection.createStatement();
stmt.execute("SELECT * from t1");
```

`PreparedStatement`可支持`SELECT`,`INSERT`两种sql，`INSERT`仅支持插入到在线。
```
PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM t1 WHERE id=?");
PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO t1 VALUES (?,?)");
```
