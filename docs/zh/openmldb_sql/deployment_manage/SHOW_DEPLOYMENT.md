# 查看 DEPLOYMENT 详情

`SHOW DEPLOYMENT`语句用于显示在线请求模式下某个已部署的任务的详情。


```SQL
SHOW DEPLOYMENT deployment_name;
```


## Example

创建一个数据库，并设置为当前数据库:

```sql
CREATE DATABASE db1;
-- SUCCEED

USE db1;
-- SUCCEED: Database changed


```

创建一张表`t1`:

```sql
CREATE TABLE t1(col0 STRING);
-- SUCCEED

```

将一条关于表t1的查询语句部署上线：

```sql
DEPLOY demo_deploy select col0 from t1;
-- SUCCEED
```

查看新部署的deployment:

```sql
SHOW DEPLOYMENT demo_deploy;

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
  1   col0    Varchar   NO          
 --- ------- ---------- ------------ 

# Output Schema
 --- ------- ---------- ------------ 
  #   Field   Type       IsConstant  
 --- ------- ---------- ------------ 
  1   col0    Varchar   NO          
 --- ------- ---------- ------------
```

## 相关语句

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[DEPLOY ](../deployment_manage/DEPLOY_STATEMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

