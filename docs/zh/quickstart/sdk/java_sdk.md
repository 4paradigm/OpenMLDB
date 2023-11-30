# Java SDK

Java SDK中，JDBC Statement的默认执行模式为在线，SqlClusterExecutor的默认执行模式则是离线，请注意。

## Java SDK 包安装

- Linux 下 Java SDK 包安装

    配置 maven pom：

    ```XML
    <dependency>
        <groupId>com.4paradigm.openmldb</groupId>
        <artifactId>openmldb-jdbc</artifactId>
        <version>0.8.4</version>
    </dependency>
    <dependency>
        <groupId>com.4paradigm.openmldb</groupId>
        <artifactId>openmldb-native</artifactId>
        <version>0.8.4</version>
    </dependency>
    ```

- Mac 下 Java SDK 包安装

    配置 maven pom：

    ```XML
    <dependency>
        <groupId>com.4paradigm.openmldb</groupId>
        <artifactId>openmldb-jdbc</artifactId>
        <version>0.8.4</version>
    </dependency>
    <dependency>
        <groupId>com.4paradigm.openmldb</groupId>
        <artifactId>openmldb-native</artifactId>
        <version>0.8.4-macos</version>
    </dependency>
    ```

注意：由于 openmldb-native 中包含了 OpenMLDB 编译的 C++ 静态库，默认是 Linux 静态库，macOS 上需将上述 openmldb-native 的 version 改成 `0.8.4-macos`，openmldb-jdbc 的版本保持不变。

openmldb-native 的 macOS 版本只支持 macOS 12，如需在 macOS 11 或 macOS 10.15上运行，需在相应 OS 上源码编译 openmldb-native 包，详细编译方法见[并发编译 Java SDK](https://openmldb.ai/docs/zh/main/deploy/compile.html#java-sdk)。使用自编译的 openmldb-native 包，推荐使用`mvn install`安装到本地仓库，然后在 pom 中引用本地仓库的 openmldb-native 包，不建议用`scope=system`的方式引用。

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

### Statement

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

离线模式默认为异步执行，返回的ResultSet是Job Info，可以通过`SET @@sync_job=true;`改为同步执行，但返回的ResultSet根据SQL不同，详情见[功能边界-离线命令同步模式](../function_boundary.md#离线命令同步模式)。只推荐在`LOAD DATA`/`SELECT INTO`时选择同步执行。

如果同步命令超时，请参考[离线命令配置详情](../../openmldb_sql/ddl/SET_STATEMENT.md#离线命令配置详情)调整配置。

```{caution}
`Statement`执行`SET @@execute_mode='offline'`不仅会影响当前`Statement`，还会影响该`Connection`已创建和未创建的所有`Statement`。所以，不建议创建多个`Statement`，并期望它们在不同的模式下执行。如果需要在不同模式下执行SQL，建议创建多个Connection。
```
### PreparedStatement

`PreparedStatement` 可支持 `SELECT`、`INSERT` 和 `DELETE`。

```{warning}
任何`PreparedStatement`都只在**在线模式**下执行，不受创建`PreparedStatement`前的任何状态影响。`PreparedStatement`不支持切换到离线模式，如果需要在离线模式下执行SQL，可以使用`Statement`。

Connection创建的三种`PreparedStatement`，分别对应SqlClusterExecutor中的`getPreparedStatement`，`getInsertPreparedStmt`，`getDeletePreparedStmt`。
```

```java
PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM t1 WHERE id=?");
PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO t1 VALUES (?,?)");
PreparedStatement insertStatement = connection.prepareStatement("DELETE FROM t1 WHERE id=?");
```

## SqlClusterExecutor 方式

SqlClusterExecutor 是最全面的Java SDK连接方式，不仅有JDBC可以使用的增删查功能，还可以使用请求模式等额外功能。

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

```{warning}
任何`PreparedStatement`都只在**在线模式**下执行，不受创建`PreparedStatement`时的`SqlClusterExecutor`状态影响。`PreparedStatement`不支持切换到离线模式，如果需要在离线模式下执行SQL，可以使用`Statement`。
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
3. 对于String, Date和Timestamp类型, 可以通过`setType(index, null)`和`setNull(index)`两种方式来设置null对象 
4. 使用 `PreparedStatement::execute()` 接口执行 insert 语句。
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
请求式查询仅支持在线，不受`SqlClusterExecutor`的当前执行模式影响，一定是进行在线的请求式查询。
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

### 执行 Deployment

执行 Deployment ，是通过 `SqlClusterExecutor::getCallablePreparedStmt(db, deploymentName)` 接口获取 CallablePreparedStatement 。区别于上文的 SQL 请求式查询，Deployment 在服务端已上线，速度会快于 SQL 请求式查询。

Deployment 使用过程分为两步：

- 上线Deployment
```java
// 上线一个Deployment（此处使用上文的selectSql），实际生产环境通常已经上线成功
java.sql.Statement state = sqlExecutor.getStatement();
try {
    String selectSql = String.format("SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM %s WINDOW w1 AS " +
            "(PARTITION BY %s.c1 ORDER BY %s.c7 ROWS_RANGE BETWEEN 2d PRECEDING AND CURRENT ROW);", table,
            table, table);
    // 上线一个Deployment
    String deploySql = String.format("DEPLOY %s OPTIONS(RANGE_BIAS='inf', ROWS_BIAS='inf') %s", deploymentName, selectSql);
    // set return null rs, don't check the returned value, it's false
    state.execute(deploySql);
} catch (Exception e) {
    e.printStackTrace();
}
```
- 执行Deployment。重新创建 CallablePreparedStmt 有一定耗时，建议尽可以复用 CallablePreparedStmt，`executeQuery()`将会自动清除`setXX`的请求行缓存。
```java
// 执行Deployment
PreparedStatement pstmt = null;
ResultSet resultSet = null;
try {
    pstmt = sqlExecutor.getCallablePreparedStmt(db, deploymentName);
    // 如果是执行deployment, 可以通过名字获取preparedstatement
    // pstmt = sqlExecutor.getCallablePreparedStmt(db, deploymentName);
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

| **可选配置项**  | **说明**                                                                                                                                                                 |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| enableDebug     | 默认 false，开启 hybridse 的 debug 日志（注意不是全局的 debug 日志），可以查看到更多 sql 编译和运行的日志。但这些日志不是全部被客户端收集，需要查看 tablet server 日志。 |
| requestTimeout  | 默认 60000 ms，这个 timeout 是客户端发送的 rpc 超时时间，发送到 taskmanager 的除外（job 的 rpc timeout 由 variable `job_timeout` 控制）。                                |
| glogLevel       | 默认 0，和 glog 的 minloglevel 类似，`INFO/WARNING/ERROR/FATAL` 日志分别对应 `0/1/2/3`。0 表示打印 INFO 以及上的等级。                                                   |
| glogDir         | 默认为 empty，日志目录为空时，打印到 stderr，即控制台。                                                                                                                  |
| maxSqlCacheSize | 默认 50，客户端单个 db 单种执行模式的最大 sql cache 数量，如果出现 cache淘汰引发的错误，可以增大这一 size 避开问题。                                                     |
| sessionTimeout  | 默认 10000 ms，zk 的 session timeout。                                                                                                                                   |
| zkLogLevel      | 默认 3，`0/1/2/3/4` 分别代表 `禁止所有 zk log/error/warn/info/debug`                                                                                                     |
| zkLogFile       | 默认 empty，打印到 stdout。                                                                                                                                              |
| sparkConfPath   | 默认 empty，可以通过此配置更改 job 使用的 spark conf，而不需要配置 taskmanager 重启。                                                                                    |

## SQL 校验

Java 客户端支持对 SQL 进行正确性校验，验证是否可执行。分为 batch 和 request 两个模式。

- `validateSQLInBatch` 可以验证 SQL 是否能在离线端执行。
- `validateSQLInRequest` 可以验证 SQL 是否能被部署上线。

两种接口都需要传入 SQL 所需要的所有表 schema，支持多 db。为了向后兼容，允许参数中不填写`db`（当前use的db），等价于use schema表中的第一个db。这种情况下，输入 SQL 语句需要保证`<table>`格式的表来自第一个db，不影响`<db>.<table>`格式的 SQL。

例如：验证 SQL `select count(c1) over w1 from t3 window w1 as(partition by c1 order by c2 rows between unbounded preceding and current row);`，那么除了这个语句，还需要将表 `t3` 的 schema 作为第二参数 schemaMaps 传入。格式为 Map，key 为 db 名，value 为每个 db 的所有 table schema(Map)。这里为了演示简单，只有 1 个 db，如下所示的 db3。db 下的 table schema map key 为 table name，value 为 `com.\_4paradigm.openmldb.sdk.Schema`，由每列的 name 和 type 构成。

返回结果`List<String>`，如果校验正确，返回空列表；如果校验失败，返回错误信息列表`[error_msg, error_trace]`。

```java
Map<String, Map<String, Schema>> schemaMaps = new HashMap<>();
Map<String, Schema> dbSchema = new HashMap<>();
dbSchema = new HashMap<>();
dbSchema.put("t3", new Schema(Arrays.asList(new Column("c1", Types.VARCHAR), new Column("c2", Types.BIGINT))));
schemaMaps.put("db3", dbSchema);
// 可以使用no db参数的格式，需保证schemaMaps中只有一个db，且sql中只是用<table>格式
// List<String> ret = SqlClusterExecutor.validateSQLInRequest("select count(c1) over w1 from t3 window "+
//        "w1 as(partition by c1 order by c2 rows between unbounded preceding and current row);", schemaMaps);
List<String> ret = SqlClusterExecutor.validateSQLInRequest("select count(c1) over w1 from t3 window "+
        "w1 as(partition by c1 order by c2 rows between unbounded preceding and current row);", "db3", schemaMaps);
Assert.assertEquals(ret.size(), 0);
```

## 生成建表DDL

`public static List<String> genDDL(String sql, Map<String, Map<String, Schema>> tableSchema)`方法可以帮助用户，根据想要deploy的 SQL，自动生成建表语句，**目前只支持单db**。参数`sql`不可以是使用`<db>.<table>`格式，`tableSchema`输入sql依赖的所有table的schema，格式和前文一致，即使此处`tableSchema`存在多db，db信息也会被丢弃，所有表都等价于在同一个不知名的db中。

## SQL Output Schema

`public static Schema genOutputSchema(String sql, String usedDB, Map<String, Map<String, Schema>> tableSchema)`方法可以得到 SQL 的 Output Schema，支持多db。如果使用`usedDB`，`sql`中使用该db的表，可以使用`<table>`格式。为了向后兼容，还支持了`public static Schema genOutputSchema(String sql, Map<String, Map<String, Schema>> tableSchema)`无db的接口，等价于使用第一个db作为used db，因此，也需要保证`sql`中`<table>`格式的表来自此db。

## SQL 表血缘

`public static List<Pair<String, String>> getDependentTables(String sql, String usedDB, Map<String, Map<String, Schema>> tableSchema)`可以获得`sql`依赖的所有表，`Pair<String, String>`分别对应库名和表名，列表的第一个元素为主表，`[1,end)`为其他依赖表（不包括主表）。输入参数`usedDB`若为空串，即无use db下进行查询。（区别于前面的`genDDL`等兼容规则）

## SQL 合并

Java 客户端支持对多个 SQL 进行合并，并进行 request 模式的正确性校验，接口为`mergeSQL`，只能在所有输入SQL的主表一致的情况下合并。

输入参数：想要合并的 SQL 组，当前使用的库名，主表的join key（可多个），以及所有表的schema。

例如，我们有这样四个特征组SQL：
```
// 单表直出特征
select c1 from main;
// 单表聚合特征
select sum(c1) over w1 of2 from main window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row);
// 多表特征
select t1.c2 of4 from main last join t1 order by t1.c2 on main.c1==t1.c1;
// 多表聚合特征
select sum(c2) over w1 from main window w1 as (union (select \"\" as id, * from t1) partition by c1 order by c2 rows between unbounded preceding and current row);
```

它们的主表均为main表，所以它们可以进行 SQL 合并。合并本质是进行join，所以我们还需要知道main表的unique列，它们可以定位到唯一一行数据。例如，main表id并不唯一，可能存在多行的id值相同，但不会出现id与c1两列值都相同，那么我们可以用id与c1两列来进行join。类似 SQL 校验，我们也传入表的schema map。

```java
// 为了展示简单，我们仅使用单个db的表，所以只需要填写used db，sql中均使用<table>格式的表名。如果sql均使用<db>.<table>格式，used db可以填空串。
String merged = SqlClusterExecutor.mergeSQL(sqls, "db", Arrays.asList("id", "c1"), schemaMaps);
```

输出结果为单个合并后的 SQL，见下。输入的SQL一共选择四个特征，所以合并 SQL 只会输出这四个特征列。（我们会自动过滤join keys）

```
select `c1`, `of2`, `of4`, `sum(c2)over w1` from (select main.id as merge_id_0, c1 from main) as out0 last join (select main.id as merge_id_1, sum(c1) over w1 of2 from main window w1 as (partition by c1 order by c2 rows between unbounded preceding and current row)) as out1 on out0.merge_id_0 = out1.merge_id_1 last join (select main.id as merge_id_2, t1.c2 of4 from main last join t1 order by t1.c2 on main.c1==t1.c1) as out2 on out0.merge_id_0 = out2.merge_id_2 last join (select main.id as merge_id_3, sum(c2) over w1 from main window w1 as (union (select "" as id, * from t1) partition by c1 order by c2 rows between unbounded preceding and current row)) as out3 on out0.merge_id_0 = out3.merge_id_3;
```

```{note}
如果合并出现`Ambiguous column name`错误，可能是不同特征组里有相同的特征名，请在输入SQL中使用别名区分它们。
```
