# 查看 DEPLOYMENT 详情

```SQL
SHOW DEPLOYMENT deployment_name;
```

`SHOW DEPLOYMENT`语句用于显示某一个OnlineServing的详情。

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

查看新部署的deployment:

```sql
SHOW DEPLOYMENT demo_deploy;
```

```
 ----- ------------- 
  DB    Deployment   
 ----- ------------- 
  db1   demo_deploy  
 ----- ------------- 
 1 row in set
 
 ---------------------------------------------------------------------------------- 
  SQL                                                                               
 ---------------------------------------------------------------------------------- 
  CREATE PROCEDURE deme_deploy (col0 varchar) BEGIN SELECT
  col0
FROM
  t1
; END;  
 ---------------------------------------------------------------------------------- 
1 row in set

# Input Schema
 --- ------- ---------- ------------ 
  #   Field   Type       IsConstant  
 --- ------- ---------- ------------ 
  1   col0    kVarchar   NO          
 --- ------- ---------- ------------ 

# Output Schema
 --- ------- ---------- ------------ 
  #   Field   Type       IsConstant  
 --- ------- ---------- ------------ 
  1   col0    kVarchar   NO          
 --- ------- ---------- ------------ 

```

## 相关语句

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[DEPLOY ](../deployment_manage/DEPLOY_STATEMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

