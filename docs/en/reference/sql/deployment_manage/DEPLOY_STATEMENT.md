# DEPLOY

## Syntax

```sql
CreateDeploymentStmt
				::= 'DEPLOY' [DeployOptionList] DeploymentName SelectStmt

DeployOptionList
				::= DeployOption*
				    
DeployOption
				::= 'OPTIONS' '(' DeployOptionItem (',' DeployOptionItem)* ')'
				    
DeploymentName
				::= identifier
```

Please refer to [DEPLOYMENT Property DeployOptions (optional)](#deployoptions-optional) for the definition of `DeployOptions`.
Please refer to [Select Statement](../dql/SELECT_STATEMENT.md) for the definition of `SelectStmt`.


The `DEPLOY` statement is used to deploy SQL online. OpenMLDB supports to deploy [Select Statement](../dql/SELECT_STATEMENT.md), and the SQL script should meet the requirements in [OpenMLDB SQL Requirement](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md)



**Example**


The following commands deploy a SQL script online under the Online Request mode of cluster version.
```sql
CREATE DATABASE db1;
-- SUCCEED

USE db1;
-- SUCCEED: Database changed

CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date);
-- SUCCEED: Create successfully

DEPLOY demo_deploy SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);

-- SUCCEED
```

We can use `SHOW DEPLOYMENT demo_deploy` command to see the detail of a specific deployment.

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


### DeployOptions (optional)

```sql
DeployOption
						::= 'OPTIONS' '(' DeployOptionItem (',' DeployOptionItem)* ')'

DeployOptionItem
            ::= 'LONG_WINDOWS' '=' LongWindowDefinitions
            | 'SKIP_INDEX_CHECK' '=' string_literal
            | 'RANGE_BIAS' '=' RangeBiasValueExpr
            | 'ROWS_BIAS' '=' RowsBiasValueExpr

RangeBiasValueExpr ::= int_literal | interval_literal | string_literal
RowsBiasValueExpr ::= int_literal | string_literal
```

#### Long Window Optimization
```sql
LongWindowDefinitions
					::= 'LongWindowDefinition (, LongWindowDefinition)*'

LongWindowDefinition
					::= WindowName':'[BucketSize]

WindowName
					::= string_literal

BucketSize
					::= int_literal | interval_literal

interval_literal ::= int_literal 's'|'m'|'h'|'d'
```

`BucketSize` is a performance optimization option. Data will be pre-aggregated according to `BucketSize`. The default value is `1d`.



##### Limitation 

The current long window optimization has the following limitations:
- Only `SelectStmt` involving one physical table is supported, i.e. `SelectStmt` containing `join` or `union` is not supported.

- Supported aggregation operations include: `sum`, `avg`, `count`, `min`, `max`, `count_where`, `min_where`, `max_where`, `sum_where`, `avg_where`.

- The table should be empty when executing the `deploy` command.

- For commands with `where` condition, like `count_where`, `min_where`, `max_where`, `sum_where`, `avg_where`, there are extra limitations：

  1. The main table should be a memory table (`storage_mode = 'Memory'`).

  2. The type of `BucketSize`  should be range type, that is its value should be `interval_literal`. For example, `long_windows='w1:1d'` is supported, whereas `long_windows='w1:100'` is not supported.

  3. The expression for `where` should be the format of `<column ref> op <const value>` or `<const value> op <column ref>`

     - Supported where op: `>, <, >=, <=, =, !=`.

     - The `<column ref>` should not be `date` type or timestamp.

- It requires the data is loaded in the increasing order of the `timestamp` column for getting the best performance boost.

**Example**

```sql
DEPLOY demo_deploy OPTIONS(long_windows="w1:1d") SELECT c1, sum(c2) OVER w1 FROM demo_table1
    WINDOW w1 AS (PARTITION BY c1 ORDER BY c2 ROWS_RANGE BETWEEN 5d PRECEDING AND CURRENT ROW);
-- SUCCEED
```

#### Skip Index Check

By default, the value of `SKIP_INDEX_CHECK` option is `false`. It means that when deploying SQL, it will check whether the existing index
is match the required index. An error will be reported if it does not matched. If this option is set to `true`, the existing index will not be verified and modified when deploying.

**Example**
```sql
DEPLOY demo OPTIONS (SKIP_INDEX_CHECK="TRUE")
    SELECT * FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col1 = t2.col1;
```

#### Synchronization/Asynchronization Settings
When executing deploy, you can set the synchronous/asynchronous mode through the `SYNC` option. The default value of `SYNC` is `true`, that is, the synchronous mode. If the relevant tables involved in the deploy statement have data and needs to add one or more indexs, executing deploy will initiate a job to execute a series of tasks such as loading data. In this case a job id will be returned if the `SYNC` option is set to `false`. You can get the job execution status by `SHOW JOBS FROM NAMESERVER LIKE '{job_id}'`

**Example**
```sql
deploy demo options(SYNC="false") SELECT t1.col1, t2.col2, sum(col4) OVER w1 as w1_col4_sum FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col2 = t2.col2
    WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```

#### BIAS

If you don't want data to be expired by the deployed index, or want data expired later, you can set bias when deploy, which is usually used in the case of data timestamp is not real-time, test, etc. If the index ttl after deploy is abs 3h, but the data timestamp is 3h ago(based on system time), then the data will be eliminated and cannot participate in the calculation. Setting a certain time or permanent bias can make the data stay in the online table for a longer time.

Range bias can be `ms`, `s`, `m`, `h`, `d`, or integer(unit is ms), or `inf`. Rows bias can be integer, or `inf`. 0 means no bias.

Notice that, we only add bias to deployed index, which is new index. It's not the final index. The final index is `bias + new_index` if deployment will create index. And the final index is `merge(old_index, bias + new_index)` if deployment will update index.

And range bias unit is `min`, we'll convert it to `min` and get upper bound. e.g. deployed index ttl is abs 2min, add range bias 20s, the result is `2min + ub(20s) = 3min`, and merge with old index 1min, the final index is `max(1min, 3min) = 3min`.

## Relevant SQL

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[SHOW DEPLOYMENT](../deployment_manage/SHOW_DEPLOYMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)

