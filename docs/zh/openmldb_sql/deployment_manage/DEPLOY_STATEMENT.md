# 创建 DEPLOYMENT

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


`DeployOption`的定义详见[DEPLOYMENT属性DeployOption（可选）](#deployoption可选)。

`SelectStmt`的定义详见[Select查询语句](../dql/SELECT_STATEMENT.md)。

`DEPLOY`语句可以将SQL部署到线上。OpenMLDB仅支持部署Select查询语句，并且需要满足[OpenMLDB SQL上线规范和要求](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md)。



**Example**

在集群版的在线请求模式下，部署上线一个SQL脚本。
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

我们可以使用 `SHOW DEPLOYMENT demo_deploy;` 命令查看部署的详情，执行结果如下：

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


### DeployOption（可选）

```sql
DeployOption
						::= 'OPTIONS' '(' DeployOptionItem (',' DeployOptionItem)* ')'

DeployOptionItem
            ::= 'LONG_WINDOWS' '=' LongWindowDefinitions
            | 'SKIP_INDEX_CHECK' '=' string_literal
            | 'SYNC' '=' string_literal
            | 'RANGE_BIAS' '=' RangeBiasValueExpr
            | 'ROWS_BIAS' '=' RowsBiasValueExpr

RangeBiasValueExpr ::= int_literal | interval_literal | string_literal
RowsBiasValueExpr ::= int_literal | string_literal
```

#### 长窗口优化
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
其中`BucketSize`为用于性能优化的可选项，OpenMLDB会根据`BucketSize`设置的粒度对表中数据进行预聚合，默认为`1d`。


##### 限制条件

目前长窗口优化有以下几点限制：
- `SelectStmt`仅支持只涉及一个物理表的情况，即不支持包含`join`或`union`的`SelectStmt`。

- 支持的聚合运算仅限：`sum`, `avg`, `count`, `min`, `max`, `count_where`, `min_where`, `max_where`, `sum_where`, `avg_where`。

- 执行`deploy`命令的时候不允许表中有数据。

- 对于带 where 条件的运算，如 `count_where`, `min_where`, `max_where`, `sum_where`, `avg_where` ，有额外限制：

  1. 主表必须是内存表 (`storage_mode = 'Memory'`)

  2. `BucketSize` 类型应为范围类型，即取值应为`interval_literal`类，比如，`long_windows='w1:1d'`是支持的, 不支持 `long_windows='w1:100'`。

  3. where 条件必须是 `<column ref> op <const value> 或者 <const value> op <column ref>`的格式。

     - 支持的 where op: `>, <, >=, <=, =, !=`

     - where 关联的列 `<column ref>`，数据类型不能是 date 或者 timestamp

- 为了得到最佳的性能提升，数据需按 `timestamp` 列的递增顺序导入。

**Example**

```sql
DEPLOY demo_deploy OPTIONS(long_windows="w1:1d") SELECT c1, sum(c2) OVER w1 FROM demo_table1
    WINDOW w1 AS (PARTITION BY c1 ORDER BY c2 ROWS_RANGE BETWEEN 5d PRECEDING AND CURRENT ROW);
-- SUCCEED
```

#### 关闭索引类型校验
默认情况下`SKIP_INDEX_CHECK`选项为`false`, deploy SQL时如果存在和期望索引key与ts相同的现有索引，还会校验现有索引和期望索引的TTL类型是否一致，并更新表的索引，如果集群版本是0.8.0或更早的，将不支持更新索引的TTL类型。如果这个选项设置为`true`, deploy的时候不会校验现有索引，也不会修改现有索引的TTL，仅创建新的期望索引。

**Example**
```sql
DEPLOY demo OPTIONS (SKIP_INDEX_CHECK="TRUE")
    SELECT * FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col1 = t2.col1;
```

#### 设置同步/异步
执行deploy的时候可以通过`SYNC`选项来设置同步/异步模式, 默认为`true`即同步模式。如果deploy语句中涉及的相关表有数据，并且需要添加索引的情况下，执行deploy会发起数据加载等任务，如果`SYNC`选项设置为`false`就会返回一个任务id。可以通过`SHOW JOBS FROM NAMESERVER LIKE '{job_id}'`来查看任务执行状态。

**Example**
```sql
deploy demo options(SYNC="false") SELECT t1.col1, t2.col2, sum(col4) OVER w1 as w1_col4_sum FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col2 = t2.col2
    WINDOW w1 AS (PARTITION BY t1.col2 ORDER BY t1.col3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```

#### 设置偏移

如果你并不希望数据根据deploy的索引淘汰，或者希望晚一点淘汰，可以在deploy时设置偏移，常用于数据时间戳并不实时的情况、测试等情况。如果deploy后的索引ttl为abs 3h，但是数据的时间戳是3h前的(以系统时间为基准)，那么这条数据就会被淘汰，无法参与计算。设置一定时间或永久的偏移，则可以让数据更久的停留在在线表中。

时间偏移，单位可以是`ms`、`s`、`m`、`h`、`d`，也可以是整数，单位为`ms`，也可以是`inf`，表示不限制；如果是行数偏移，可以是整数，单位是`row`，也可以是`inf`，表示不限制。0表示不偏移。

注意，我们只将偏移加在deploy的解析索引中，也就是新索引，它们并不是最终索引。最终索引的计算方式是，如果是创建索引，最终索引是`解析索引 + 偏移`；如果是更新索引，最终索引是`merge(旧索引, 新索引 + 偏移)`。

而时间偏移的单位是`min`，我们会在内部将其转换为`min`，并且取上界。比如，新索引ttl是abs 2min，加上偏移20s，结果是`2min + ub(20s) = 3min`，然后和旧索引1min取上界，最终索引ttl是`max(1min, 3min) = 3min`。

## 相关SQL

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[SHOW DEPLOYMENT](../deployment_manage/SHOW_DEPLOYMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)
