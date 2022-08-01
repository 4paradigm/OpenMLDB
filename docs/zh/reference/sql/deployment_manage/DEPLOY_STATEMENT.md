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

我们可以使用 `SHOW DEPLOYMENT demo_deploy` 命令查看部署的详情，执行结果如下：

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


### DEPLOYMENT属性DeployOptions（可选）

```sql
DeployOptions
						::= 'OPTIONS' '(' DeployOptionItem (',' DeployOptionItem)* ')'

DeployOptionItem
						::= LongWindowOption

LongWindowOption
						::= 'LONG_WINDOWS' '=' LongWindowDefinitions
```
目前只支持长窗口`LONG_WINDOWS`的优化选项。

#### 长窗口优化
##### 长窗口优化选项格式
```sql
LongWindowDefinitions
						::= 'LongWindowDefinition (, LongWindowDefinition)*'

LongWindowDefinition
						::= 'WindowName[:BucketSize]'

WindowName
						::= string_literal

BucketSize（可选，默认为）
						::= int_literal | interval_literal

interval_literal ::= int_literal 's'|'m'|'h'|'d'（分别代表秒、分、时、天）
```
其中`BucketSize`为性能优化选项，会以`BucketSize`为粒度，对表中数据进行预聚合，默认为`1d`。

示例如下：
```sqlite
DEPLOY demo_deploy OPTIONS(long_windows="w1:1d") SELECT col0, sum(col1) OVER w1 FROM t1
    WINDOW w1 AS (PARTITION BY col0 ORDER BY col2 ROWS_RANGE BETWEEN 5d PRECEDING AND CURRENT ROW);
-- SUCCEED: deploy successfully
```

##### 限制条件

目前长窗口优化有以下几点限制：
- 仅支持`SelectStmt`只涉及到一个物理表的情况，即不支持包含`join`或`union`的`SelectStmt`
- 支持的聚合运算仅限：`sum`, `avg`, `count`, `min`, `max`
- 执行`deploy`命令的时候不允许表中有数据

## 相关SQL

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[SHOW DEPLOYMENT](../deployment_manage/SHOW_DEPLOYMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)