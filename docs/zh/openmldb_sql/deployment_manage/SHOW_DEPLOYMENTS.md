# 查看 DEPLOYMENTS 列表

`SHOW DEPLOYMENTS`语句用于显示处于在线请求模式的当前数据库下，已经部署的任务列表。


```SQL
SHOW DEPLOYMENTS;
```


## Example

创建一个数据库，并设置为当前数据库：

```sql
CREATE DATABASE db1;
-- SUCCEED

USE db1;
-- SUCCEED: Database changed
```

创建一张表`t1`：

```sql
CREATE TABLE t1(col0 STRING);
-- SUCCEED
```

部署表t1的查询语句：

```sql
DEPLOY demo_deploy select col0 from t1;
-- SUCCEED
```

查看当前数据库下已部署的所有任务：

```sql
SHOW DEPLOYMENTS;
 ----- ------------- 
  DB    Deployment   
 ----- ------------- 
  db1   demo_deploy  
 ----- ------------- 
1 row in set

```

## 相关语句

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[DEPLOY ](../deployment_manage/DEPLOY_STATEMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

