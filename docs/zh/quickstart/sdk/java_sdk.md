# Java SDK

## Java SDK 包安装

- Linux 下 Java SDK 包安装

    配置 maven pom：

    ```XML
    <dependency>
        <groupId>com.4paradigm.openmldb</groupId>
        <artifactId>openmldb-jdbc</artifactId>
        <version>0.7.2</version>
    </dependency>
    <dependency>
        <groupId>com.4paradigm.openmldb</groupId>
        <artifactId>openmldb-native</artifactId>
        <version>0.7.2</version>
    </dependency>
    ```

- Mac 下 Java SDK 包安装

    配置 maven pom：

    ```XML
    <dependency>
        <groupId>com.4paradigm.openmldb</groupId>
        <artifactId>openmldb-jdbc</artifactId>
        <version>0.7.2</version>
    </dependency>
    <dependency>
        <groupId>com.4paradigm.openmldb</groupId>
        <artifactId>openmldb-native</artifactId>
        <version>0.7.2-macos</version>
    </dependency>
    ```

注意：由于 openmldb-native 中包含了 OpenMLDB 编译的 C++ 静态库，默认是 Linux 静态库，macOS 上需将上述 openmldb-native 的 version 改成 `0.7.2-macos`，openmldb-jdbc 的版本保持不变。

openmldb-native 的 macOS 版本只支持 macOS 12，如需在 macOS 11 或 macOS 10.15上运行，需在相应 OS 上源码编译 openmldb-native 包，详细编译方法见[并发编译 Java SDK](https://openmldb.ai/docs/zh/main/deploy/compile.html#java-sdk)。

Java SDK 连接 OpenMLDB 服务，可以使用 JDBC 的方式（推荐），也可以通过 SqlClusterExecutor 的方式直连。如果需要使用在线请求模式，只能使用 SqlClusterExecutor 。下面将依次演示两种连接方式。

## JDBC 方式

JDBC 的连接方式如下：

```java
Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
// No database in jdbcUrl
Connection connection = DriverManager.getConnection("jdbc:openmldb:///?zk=localhost:6181&zkPath=/openmldb");

// Set database in jdbcUrl
Connection connection1 = DriverManager.getConnection("jdbc:openmldb:///test_db?zk=localhost:6181&zkPath=/openmldb");
```

Connection 地址指定的 db 在创建连接时必须存在。

```{caution}
JDBC Connection 的默认执行模式为`online`。
```

### 使用概览

通过 `Statement` 的方式可以执行所有的 SQL 命令，离线在线模式下都可以。切换离线/在线模式，需执行 `SET @@execute_mode='...';`。例如：

```java
Statement stmt = connection.createStatement();
stmt.execute("SET @@execute_mode='offline"); // 切换为离线模式
stmt.execute("SELECT * from t1"); // 离线 select
ResultSet res = stmt.getResultSet(); // 上一次 execute 的 ResultSet 结果

stmt.execute("SET @@execute_mode='online"); // 切换为在线模式
res = stmt.executeQuery("SELECT * from t1"); // 在线 select, executeQuery 可直接获取 ResultSet 结果
```

其中，`LOAD DATA` 命令是异步命令，返回的 ResultSet 包含该 job 的 id、state 等信息。可通过执行 `show job <id>` 来查询 job 是否执行完成。注意 ResultSet 需要先执行 `next()` 游标才会指向第一行数据。

也可以改为同步命令：

```SQL
SET @@sync_job=true;
```

如果同步命令实际耗时超过连接空闲默认的最大等待时间 0.5 小时，请[调整配置](../../openmldb_sql/ddl/SET_STATEMENT.md#离线命令配置详情)。
### PreparedStatement

`PreparedStatement` 可支持 `SELECT`、`INSERT` 和 `DELETE`，`INSERT` 仅支持插入到在线。

```java
PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM t1 WHERE id=?");
PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO t1 VALUES (?,?)");
PreparedStatement insertStatement = connection.prepareStatement("DELETE FROM t1 WHERE id=?");
```

## SqlClusterExecutor 方式

### 创建 SqlClusterExecutor

首先，进行 OpenMLDB 连接参数配置。

```Java
SdkOption option = new SdkOption();
option.setZkCluster("127.0.0.1:2181");
option.setZkPath("/openmldb");
option.setSessionTimeout(10000);
option.setRequestTimeout(60000);
```

然后使用 SdkOption 创建 Executor。

```java
sqlExecutor = new SqlClusterExecutor(option);
```

`SqlClusterExecutor` 执行 SQL 操作是多线程安全的，在实际环境中可以创建一个 `SqlClusterExecutor`。但由于执行模式 (execute_mode) 是 `SqlClusterExecutor` 内部变量，如果想同时执行一个离线命令和一个在线命令，容易出现不可预期的结果。这时候请使用多个 `SqlClusterExecutor`。

```{caution}
SqlClusterExecutor 的默认执行模式为 `offline`，与 JDBC 默认模式不同。
```

### Statement

`SqlClusterExecutor` 可以获得 `Statement`，类似 JDBC 方式，可以使用 `Statement::execute`。

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

注意 `SqlClusterExecutor` 没有默认数据库的概念，所以需要进行一次 `USE <db>` 才可以继续建表。

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

#### Statement 执行 SQL 批式查询

使用 `Statement::execute` 接口执行 SQL 批式查询语句：

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

### PreparedStatement

`SqlClusterExecutor` 也可以获得 `PreparedStatement`，但需要指定获得哪种 `PreparedStatement`。例如，使用 InsertPreparedStmt 进行插入操作，可以有三种方式。

```{note}
插入操作仅支持在线，不受执行模式影响，一定是插入数据到在线。
```

#### 普通 Insert

1. 使用 `SqlClusterExecutor::getInsertPreparedStmt(db, insertSql)` 接口获取InsertPrepareStatement。
2. 使用 `PreparedStatement::execute()` 接口执行 insert 语句。

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

#### Insert With Placeholder

1. 使用 `SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)` 接口获取 InsertPrepareStatement。
2. 调用 `PreparedStatement::setType(index, value)` 接口，填充数据到 InsertPrepareStatement中。注意 index 从 1 开始。
3. 使用 `PreparedStatement::execute()` 接口执行 insert 语句。
```{note}
PreparedStatment条件相同时，可以对同一个对象反复set填充数据后，再执行execute，不需要重新创建PreparedStatement。
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
      // PrepareStatement用完之后必须close
      pstmt.close();
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }
}
```

```{note}
execute 后，缓存的数据将被清除，无法重试 execute。
```

#### Batch Insert With Placeholder

1. 使用 `SqlClusterExecutor::getInsertPreparedStmt(db, insertSqlWithPlaceHolder)` 接口获取 InsertPrepareStatement。
2. 调用 `PreparedStatement::setType(index, value)` 接口，填充数据到 InsertPrepareStatement 中。
3. 使用 `PreparedStatement::addBatch()` 接口完成一行的填充。
4. 继续使用 `setType(index, value)` 和 `addBatch()`，填充多行。
5. 使用 `PreparedStatement::executeBatch()` 接口完成批量插入。

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
executeBatch 后，缓存的所有数据将被清除，无法重试 executeBatch。
```

### 执行 SQL 请求式查询

`RequestPreparedStmt` 是一个独特的查询模式（JDBC Connection不支持创建这种查询）。此模式需要 selectSql 与一条请求数据，所以需要在 `getRequestPreparedStmt` 时填入 SQL，也需要 `setType` 设置请求数据。

执行 SQL 请求式查询有以下三步：

```{note}
请求式查询仅支持在线，不受执行模式影响，一定是进行在线的请求式查询。
```

1. 使用 `SqlClusterExecutor::getRequestPreparedStmt(db, selectSql)` 接口获取RequestPrepareStatement。
2. 调用 `PreparedStatement::setType(index, value)` 接口设置请求数据。请根据数据表中每一列对应的数据类型调用 setType 接口以及配置合法的值。
3. 调用 `Statement::executeQuery()` 接口执行请求式查询语句。

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

###  删除指定索引下某个 key 的所有数据

通过 Java SDK 可以有以下两种方式删除数据:

- 直接执行 delete SQL
- 使用 delete PreparedStatement

注意，这样仅能删除一个索引下的数据，不是对所有索引都生效。详情参考 [DELETE 功能边界](../function_boundary.md#delete)。

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

###  完整的 SqlClusterExecutor 使用范例

参考 [Java quickstart demo](https://github.com/4paradigm/OpenMLDB/tree/main/demo/java_quickstart/demo)。如果在 macOS 上使用，请使用 macOS 版本的 openmldb-native，并增加 openmldb-native 的依赖。

编译并运行：

```
mvn package
java -cp target/demo-1.0-SNAPSHOT.jar com.openmldb.demo.App
```

## SDK 配置项详解

必须填写 `zkCluster` 和 `zkPath`（set 方法或 JDBC 中 `?` 后的配置项 `foo=bar`）。

### 可选配置项

| **可选配置项** | **说明**                                                     |
| -------------- | ------------------------------------------------------------ |
| enableDebug     | 默认 false，开启 hybridse 的 debug 日志（注意不是全局的 debug 日志），可以查看到更多 sql 编译和运行的日志。但这些日志不是全部被客户端收集，需要查看 tablet server 日志。 |
| requestTimeout  | 默认 60000 ms，这个 timeout 是客户端发送的 rpc 超时时间，发送到 taskmanager 的除外（job 的 rpc timeout 由 variable `job_timeout` 控制）。 |
| glogLevel       | 默认 0，和 glog 的 minloglevel 类似，`INFO/WARNING/ERROR/FATAL` 日志分别对应 `0/1/2/3`。0 表示打印 INFO 以及上的等级。 |
| glogDir         | 默认为 empty，日志目录为空时，打印到 stderr，即控制台。      |
| maxSqlCacheSize | 默认 50，客户端单个 db 单种执行模式的最大 sql cache 数量，如果出现 cache淘汰引发的错误，可以增大这一 size 避开问题。 |
| sessionTimeout | 默认 10000 ms，zk 的 session timeout。                       |
| zkLogLevel     | 默认 3，`0/1/2/3/4` 分别代表 `禁止所有 zk log/error/warn/info/debug` |
| zkLogFile      | 默认 empty，打印到 stdout。                                  |
| sparkConfPath  | 默认 empty，可以通过此配置更改 job 使用的 spark conf，而不需要配置 taskmanager 重启。 |

## SQL 校验

Java 客户端支持对 SQL 进行正确性校验，验证是否可执行。分为 batch 和 request 两个模式。

- `validateSQLInBatch` 可以验证 SQL 是否能在离线端执行。
- `validateSQLInRequest` 可以验证 SQL 是否能被部署上线。

两个接口都需要传入 SQL 所需要的所有表 schema。目前只支持单 db，请不要在 SQL 语句中使用 `db.table` 格式。

例如：验证 SQL `select count(c1) over w1 from t3 window w1 as(partition by c1 order by c2 rows between unbounded preceding and current row);`，那么除了这个语句，还需要将表 `t3` 的 schema 作为第二参数 schemaMaps 传入。格式为 Map，key 为 db 名，value 为每个 db 的所有 table schema(Map)。实际只支持单 db，所以这里通常只有 1 个 db，如下所示的 db3。db 下的 table schema map key 为 table name，value 为 com.\_4paradigm.openmldb.sdk.Schema，由每列的 name 和 type 构成。

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
