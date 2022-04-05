# 查看 DEPLOYMENTS 列表

```SQL
SHOW DEPLOYMENTS;
```

`SHOW DEPLOYMENTS`语句用户显示当前数据库下已经部署的Online serving列表。

## Example

创建一个数据库，并设置为当前数据库:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

USE db1;
-- SUCCEED: Database changed
```

创建一张表`t1`:

```sql
CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully
```

部署表t1的查询语句到OnlineServing:

```sql
DEPLOY demo_deploy select col0 from t1;
-- SUCCEED: deploy successfully
```

查看当前数据库下所有的deployments:

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

