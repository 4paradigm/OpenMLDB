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


`DeployOption`的定义详见[DEPLOYMENT属性DeployOption（可选）](#DeployOption可选)。

`SelectStmt`的定义详见[Select查询语句](../dql/SELECT_STATEMENT.md)。

`DEPLOY`语句可以将SQL部署到线上。OpenMLDB仅支持部署Select查询语句，并且需要满足[OpenMLDB SQL上线规范和要求](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md)。



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
默认情况下`SKIP_INDEX_CHECK`选项为`false`, 即deploy SQL时会校验现有的索引和期望的索引类型是否一致，如果不一致会报错。如果这个选项设置为`true`, deploy的时候对已有索引不会校验已有索引，也不会修改已有索引的TTL。

**Example**
```sql
DEPLOY demo OPTIONS (SKIP_INDEX_CHECK="TRUE")
    SELECT * FROM t1 LAST JOIN t2 ORDER BY t2.col3 ON t1.col1 = t2.col1;
```

## 相关SQL

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[SHOW DEPLOYMENT](../deployment_manage/SHOW_DEPLOYMENT.md)

[DROP DEPLOYMENT](../deployment_manage/DROP_DEPLOYMENT_STATEMENT.md)
