# Java SDK

## Java SDK包安装

### Linux下 Java SDK包安装
配置maven pom

```xml
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.6.9</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.6.9</version>
</dependency>
```
### Mac下 Java SDK包安装
配置maven pom

```xml
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-jdbc</artifactId>
    <version>0.6.9</version>
</dependency>
<dependency>
    <groupId>com.4paradigm.openmldb</groupId>
    <artifactId>openmldb-native</artifactId>
    <version>0.6.9-macos</version>
</dependency>
```
注意: 由于 openmldb-native 中包含了 OpenMLDB 编译的 C++ native库, 默认是 linux 库, macOS 上需将上述 openmldb-native 的 version 改成 `0.6.9-macos`, openmldb-jdbc 的版本保持不变。

当前macOS native发行版只支持macos-12，如需在macos-11或macos-10.15上运行，需在相应OS上源码编译openmldb-native包，详细编译方法见[并发编译Java SDK](../deploy/compile.md#并发编译java-sdk)。

## Java SDK快速上手

Java SDK连接OpenMLDB服务，可以通过JDBC的方式（仅连接集群版），也可以通过SqlClusterExecutor的方式直连。如果连接集群版OpenMLDB，推荐使用JDBC的方式。

### JDBC 方式

JDBC的方式目前只能连接集群版OpenMLDB。连接方式如下：

```
Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
// No database in jdbcUrl
Connection connection = DriverManager.getConnection("jdbc:openmldb:///?zk=localhost:6181&zkPath=/openmldb");

// Set database in jdbcUrl
Connection connection1 = DriverManager.getConnection("jdbc:openmldb:///test_db?zk=localhost:6181&zkPath=/openmldb");
```

Connection地址指定的db在创建连接时必须存在。

```{caution}
JDBC Connection的默认执行模式为`online`。
```

#### 使用概览

通过`Statement`的方式可以执行所有的sql命令，离线在线模式下都可以。切换离线/在线模式，需执行`SET @@execute_mode='...';`。例如：
```java
Statement stmt = connection.createStatement();
stmt.execute("SET @@execute_mode='offline"); // 切换为离线模式
stmt.execute("SELECT * from t1"); // 离线select
ResultSet res = stmt.getResultSet(); // 上一次execute的ResultSet结果
stmt.execute("SET @@execute_mode='online"); // 切换为在线模式
res = stmt.executeQuery("SELECT * from t1"); // 在线select, executeQuery可直接获取ResultSet结果
```

其中，离线命令与"在线LOAD DATA(cluster)"命令是异步命令，返回的ResultSet包含该job的id、state等信息。可通过执行`show job <id>`来查询job是否执行完成。**注意ResultSet需要先执行`next()`游标才会指向第一行数据**。

也可以改为同步命令：
```
SET @@sync_job=true;
SET @@job_timeout=60000; // ms, 默认timeout 1min（考虑到异步时不需要太久的等待），同步情况下应调整大一点
```
```{tip}
如果同步命令实际耗时超过连接空闲默认的最大等待时间0.5h，请[调整Taskmanager的keepAliveTime](../maintain/faq.md#2-为什么收到-got-eof-of-socket-的警告日志)。
```

#### PreparedStatement

`PreparedStatement`可支持`SELECT`,`INSERT`和`DELETE`，`INSERT`仅支持插入到在线。
```java
PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM t1 WHERE id=?");
PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO t1 VALUES (?,?)");
PreparedStatement insertStatement = connection.prepareStatement("DELETE FROM t1 WHERE id=?");
```

### SqlClusterExecutor 方式

#### 创建SqlClusterExecutor

首先，进行OpenMLDB连接参数配置，java sdk集群版和单机版的区别在于连接参数配置不同，默认是集群版。

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
option.setClusterMode(false); // 必须
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);
```

接着，使用SdkOption创建Executor。

```java
sqlExecutor = new SqlClusterExecutor(option);
```

`SqlClusterExecutor`执行sql操作是多线程安全的，在实际环境中可以创建一个`SqlClusterExecutor`。但由于执行模式(execute_mode)是`SqlClusterExecutor`内部变量，如果同时想执行一个离线命令和一个在线命令，容易出现不可预期的结果。如果一定要多执行模式并发，请使用多个`SqlClusterExecutor`。

```{caution}
SqlClusterExecutor的默认执行模式为`offline`，与JDBC默认模式不同。
```

#### Statement

`SqlClusterExecutor`可以获得`Statement`，类似JDBC方式，可以使用`Statement::execute`。

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

注意`SqlClusterExecutor`没有默认db的概念，所以需要进行一次`USE <db>`才可以继续建表。

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

##### Statement执行SQL批式查询

使用`Statement::execute`接口执行SQL批式查询语句:

```java
java.sql.Statement state = sqlExecutor.getStatement();
try {
    state.execute("use db_test");
    // sqlExecutor默认执行模式为离线，如果此前没有更改模式为在线，此处需要设置执行模式为在线
    state.execute("SET @@execute_mode='online;");
    // execute返回值是true的话说明操作成功，结果可以通过getResultSet获取
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

#### PreparedStatement

`SqlClusterExecutor`也可以获得`PreparedStatement`，但需要指定获得哪种`PreparedStatement`。例如，我们使用InsertPreparedStmt进行插入操作，可以有三种方式。
```{note}
插入操作仅支持在线，不受执行模式影响，一定是插入数据到在线。
```

##### 普通Insert

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

##### Insert With Placeholder

第一步，使用`SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)`接口获取InsertPrepareStatement。

第二步，调用`PreparedStatement::setType(index, value)`接口，填充数据到InsertPrepareStatement中。注意index从1开始。

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

##### Batch Insert With Placeholder

第一步，使用`SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)`接口获取InsertPrepareStatement。

第二步，调用`PreparedStatement::setType(index, value)`接口，填充数据到InsertPrepareStatement中。

第三步，使用`PreparedStatement::addBatch()`接口完成一行的填充。

第四步，继续使用`setType(index, value)`和`addBatch()`，填充多行。

第五步，使用`PreparedStatement::executeBatch()`接口完成批量插入。

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

#### 执行SQL请求式查询

`RequestPreparedStmt`是一个独特的查询模式（JDBC不支持此模式）。此模式需要selectSql与一条请求数据，所以需要在`getRequestPreparedStmt`时填入sql，也需要`setType`设置请求数据。

```{note}
请求式查询仅支持在线，不受执行模式影响，一定是进行在线的请求式查询。
```

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

#### 删除指定索引下某个pk的所有数据

通过JAVA SDK可以有以下两种方式删除数据:
- 直接执行delete SQL
- 使用 delete preparestatement

注意，这样的删除仅能删除一个索引下的数据，不是对所有索引都生效。详情参考[DELETE功能边界](./function_boundary.md#delete)。

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

### 完整的 SqlClusterExecutor 使用范例

见[Java quickstart demo](https://github.com/4paradigm/OpenMLDB/tree/main/demo/java_quickstart/demo)。编译并运行：
```
mvn package
java -cp target/demo-1.0-SNAPSHOT.jar com.openmldb.demo.App
```

## SDK Option详解

连接集群版必须填写`zkCluster`和`zkPath`（set方法或JDBC中`?`后的配置项`foo=bar`）。其他选项可选。

连接单机版必须填写`host`和`port`以及`isClusterMode`(即`SDKOption.setClusterMode`)，注意必须设置clusterMode，目前不支持自动配置这一选项。其他选项可选。

### 通用可选项

连接单机或集群版都可以配置的选项有：
- enableDebug: 默认false，开启hybridse的debug日志（注意不是全局的debug日志），可以查看到更多sql编译和运行的日志。但这些日志不是全部被客户端收集，需要查看 tablet server 日志。
- requestTimeout: 默认60000ms，这个timeout是客户端发送的rpc超时时间，发送到taskmanager的除外（job的rpc timeout由variable `job_timeout`控制）。
- glogLevel: 默认0，和glog的minloglevel类似，INFO, WARNING, ERROR, and FATAL日志分别对应 0, 1, 2, and 3。0表示打印INFO以及上的等级。
- glogDir: 默认为empty，日志目录为空时，打印到stderr，即控制台。
- maxSqlCacheSize: 默认50，客户端单个db单种执行模式的最大sql cache数量，如果出现cache淘汰引发的错误，可以增大这一size避开问题。

### 集群版专有可选项

由于集群版有zk和taskmanager组件，所以有以下配置可选项：
- sessionTimeout: 默认10000ms，zk的session timeout。
- zkLogLevel: 默认3，0-禁止所有zk log, 1-error, 2-warn, 3-info, 4-debug。
- zkLogFile: 默认empty，打印到stdout。
- sparkConfPath: 默认empty，可以通过此配置更改job使用的spark conf，而不需要配置taskmanager重启。

## SQL 校验

JAVA客户端支持对sql进行正确性校验，验证sql是否可执行。分为batch和request两个模式。

- `validateSQLInBatch`可以验证sql是否能在离线端执行。

- `validateSQLInRequest`可以验证sql是否能被deploy。

两个接口都需要传入sql所需要的所有表schema。目前只支持单db，请**不要**在sql语句中使用`db.table`格式。
