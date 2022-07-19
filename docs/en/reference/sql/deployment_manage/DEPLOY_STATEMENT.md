# 创建 DEPLOYMENT

## Syntax

```sql
CreateDeploymentStmt
						::= 'DEPLOY' [DeployOptions] DeploymentName SelectStmt

DeployOptions（可选）
						::= 'OPTIONS' '(' DeployOptionItem (',' DeployOptionItem)* ')'

DeploymentName
						::= identifier
```
`DeployOptions`的定义详见[DEPLOYMENT属性DeployOptions（可选）](#DEPLOYMENT属性DeployOptions（可选）).

`DEPLOY`语句可以将SQL部署到线上。OpenMLDB仅支持部署[Select查询语句](../dql/SELECT_STATEMENT.md)，并且需要满足[OpenMLDB SQL上线规范和要求](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md)

```SQL
DEPLOY deployment_name SELECT clause
```

### Example: 部署一个SQL到online serving

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


### DEPLOYMENT Property DeployOptions (optional)

```sql
DeployOptions
						::= 'OPTIONS' '(' DeployOptionItem (',' DeployOptionItem)* ')'

DeployOptionItem
						::= LongWindowOption

LongWindowOption
						::= 'LONG_WINDOWS' '=' LongWindowDefinitions
```
Currently only the optimization option for long windows `LONG_WINDOWS` is supported.

#### Long Window Optimization
##### Long Window Optimization Options Format
```sql
LongWindowDefinitions
						::= 'LongWindowDefinition (, LongWindowDefinition)*'

LongWindowDefinition
						::= 'WindowName[:BucketSize]'

WindowName
						::= string_literal

BucketSize (optional, defaults to)
						::= int_literal | interval_literal

interval_literal ::= int_literal 's'|'m'|'h'|'d' (representing seconds, minutes, hours, days)
```
Among them, `BucketSize` is a performance optimization option. It will use `BucketSize` as the granularity to pre-aggregate the data in the table. The default value is `1d`.

An example is as follows:
```sqlite
DEPLOY demo_deploy OPTIONS(long_windows="w1:1d") SELECT col0, sum(col1) OVER w1 FROM t1
    WINDOW w1 AS (PARTITION BY col0 ORDER BY col2 ROWS_RANGE BETWEEN 5d PRECEDING AND CURRENT ROW);
-- SUCCEED: deploy successfully
```

##### Limitation Factor

The current long window optimization has the following limitations:
- Only supports `SelectStmt` involving only one physical table, i.e. `SelectStmt` containing `join` or `union` is not supported
- Only supported aggregation operations: `sum`, `avg`, `count`, `min`, `max`
- Do not allow data in the table when executing the `deploy` command

## Relevant SQL

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[SHOW DEPLOYMENT](../deployment_manage/SHOW_DEPLOYMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

