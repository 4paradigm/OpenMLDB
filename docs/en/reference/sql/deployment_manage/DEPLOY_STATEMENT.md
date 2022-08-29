# DEPLOY

## Syntax

```sql
CreateDeploymentStmt
						::= 'DEPLOY' [DeployOptions] DeploymentName SelectStmt

DeployOptions（可选）
						::= 'OPTIONS' '(' DeployOptionItem (',' DeployOptionItem)* ')'

DeploymentName
						::= identifier
```
Please refer to [DEPLOYMENT Property DeployOptions (optional)](#DEPLOYMENT Property DeployOptions (optional)) for the definition of `DeployOptions`.

The `DEPLOY` statement is used to deploy SQL to online. It supports to deploy [Select Statement](../dql/SELECT_STATEMENT.md)，and the SQL should meet the requirement [OpenMLDB SQL Requirement](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md)

```SQL
DEPLOY deployment_name SELECT clause
```

### Example: Deploy SQL onto Online

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

We can use `SHOW DEPLOYMENT demo_deploy` command to see the detail of sepcific deployment：

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
  1   c1      Varchar     NO
  2   c2      Int32       NO
  3   c3      Int64       NO
  4   c4      Float       NO
  5   c5      Double      NO
  6   c6      Timestamp   NO
  7   c7      Date        NO
 --- ------- ------------ ------------

# Output Schema
 --- ----------- ---------- ------------
  #   Field       Type       IsConstant
 --- ----------- ---------- ------------
  1   c1          Varchar   NO
  2   c2          Int32     NO
  3   w1_c3_sum   Int64     NO
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

- Only supported aggregation operations: `sum`, `avg`, `count`, `min`, `max`, `count_where`, `min_where`, `max_where`, `sum_where`, `avg_where`

- Do not allow data in the table when executing the `deploy` command

- For `count_where`, `min_where`, `max_where`, `sum_where`, `avg_where`, there are extra limitations：

  1. The main table over should be a memory type table (`storage_mode = 'Memory'`)

  2. The type of `BucketSize` for deployment should be range, e.g.  `long_windows='w1:1d'` supported, whereas `long_windows='w1:100'` is not

  3. The expression for where should be the format of `<column ref> op <const value>` or `<const value> op <column ref>`

     - Supported where op: `>, <, >=, <=, =, !=`

     - The `<column ref>` should not be type of date or timestamp

## Relevant SQL

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[SHOW DEPLOYMENT](../deployment_manage/SHOW_DEPLOYMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

