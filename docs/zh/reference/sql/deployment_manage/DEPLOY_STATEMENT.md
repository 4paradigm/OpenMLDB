# 创建 DEPLOYMENT

## Syntax

```sql
CreateDeploymentStmt
									::= 'DEPLOY' deploymentName SelectStmt
deploymentName
							::= identifier
```

`DEPLOY`语句可以将SQL部署到线上。OpenMLDB仅支持部署[Select查询语句](../dql/SELECT_STATEMENT.md)，并且需要满足[OpenMLDB SQL上线规范和要求](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md)

```SQL
DEPLOY deployment_name SELECT clause
```

## Example: 部署一个SQL到online serving

```sqlite
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

USE db1;
-- SUCCEED: Database changed

CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully

DEPLOY demo_deploy select col0 from t1;
-- SUCCEED: deploy successfully
```

查看部署详情：

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
  1   col0    kVarchar   NO          
 --- ------- ---------- ------------ 

# Output Schema
 --- ------- ---------- ------------ 
  #   Field   Type       IsConstant  
 --- ------- ---------- ------------ 
  1   col0    kVarchar   NO          
 --- ------- ---------- ------------ 
```



## 相关SQL

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[SHOW DEPLOYMENT](../deployment_manage/SHOW_DEPLOYMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

