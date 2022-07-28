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

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

USE db1;
-- SUCCEED: Database changed

CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date);
-- SUCCEED: Create successfully

DEPLOY demo_deploy SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
-- SUCCEED: deploy successfully
```

查看部署详情：

```sql
 --------- -------------------
  DB        Deployment
 --------- -------------------
  demo_db   demo_deploy
 --------- -------------------
1 row in set
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  SQL
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  DEPLOY demo_data_service SELECT
  c1,
  c2,
  sum(c3) OVER (w1) AS w1_c3_sum
FROM
  demo_table1
WINDOW w1 AS (PARTITION BY demo_table1.c1
  ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
;
 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1 row in set
# Input Schema
 --- ------- ------------ ------------
  #   Field   Type         IsConstant
 --- ------- ------------ ------------
  1   c1      kVarchar     NO
  2   c2      kInt32       NO
  3   c3      kInt64       NO
  4   c4      kFloat       NO
  5   c5      kDouble      NO
  6   c6      kTimestamp   NO
  7   c7      kDate        NO
 --- ------- ------------ ------------

# Output Schema
 --- ----------- ---------- ------------
  #   Field       Type       IsConstant
 --- ----------- ---------- ------------
  1   c1          kVarchar   NO
  2   c2          kInt32     NO
  3   w1_c3_sum   kInt64     NO
 --- ----------- ---------- ------------ 
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

