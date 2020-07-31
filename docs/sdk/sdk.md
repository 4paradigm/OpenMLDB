# fedb sdk使用说明

## java sdk使用例子

### maven依赖配置
```
<dependency>
	<groupId>com._4paradigm.sql</groupId>
	<artifactId>sql-jdbc</artifactId>
	<version>2.0.0.0-beta2</version>
</dependency>
```

### 初始化sdk

```
String zkPath="xxx";
String zkCluster="xxx";
SdkOption option = new SdkOption();
option.setZkPath(zkPath);
option.setZkCluster(zkCluster);
option.setSessionTimeout(200000);
SqlExecutor router = new SqlClusterExecutor(option);
```

### 执行一条插入sql

```
String insert = "insert into tsql1010 values(1000, 'hello');";
boolean ok = router.executeInsert(dbname, insert);
```
### 执行一条查询语句

```
String select = "select col1 from tsql1010 limit 1;";
ResultSet rs = router.executeSQL(dbname, select);
```

## python sdk使用例子

### python wheel包下载地址

* linux wheel包地址http://pkg.4paradigm.com/fedb/linux/rtidb_sql-2.0.0.0-cp38-cp38-linux_x86_64.whl
* mac wheel包地址http://pkg.4paradigm.com/fedb/mac/rtidb_sql-2.0.0.0-cp37-cp37m-macosx_10_9_x86_64.whl

### 使用例子

```
from rtidb_sql import sql_router_sdk
options = sql_router_sdk.SQLRouterOptions()
options.zk_cluster = "172.27.128.37:4181"
options.zk_path = "/onebox"
sdk = sql_router_sdk.NewClusterSQLRouter(options)
dbname = "py_dx"
status = sql_router_sdk.Status()
ok = sdk.CreateDB(dbname, status)
ddl = "create table tsql1010 ( col1 bigint, col2 string, index(key=col2, ts=col1));"
ok = sdk.ExecuteDDL(dbname, ddl, status)
insert = "insert into tsql1010 values(1000, 'hello');"
ok = sdk.ExecuteInsert(dbname, insert, status);
select = "select col1 from tsql1010 limit 1;"
rs = sdk.ExecuteSQL(dbname, select, status)
```


