# SQL 上线规范和要求

OpenMLDB 的**在线请求模式**能提供实时特征抽取服务。使用[DEPLOY](../deployment_manage/DEPLOY_STATEMENT.md)命令可以将一段SQL命令部署上线。部署成功后，用户可通过 Restful APIs 或者 SDK 实时地对请求样本作特征抽取计算。但是，并非所有的 SQL 都可以部署上线，本文定义了可上线 SQL 的规范要求。

## 在线请求模式支持的语句

OpenMLDB仅支持上线[SELECT查询语句](../dql/SELECT_STATEMENT.md)。

## 在线请求模式 `SELECT` 支持的子句

**部分SELECT查询语句不支持在在线请求模式下执行。** 详见[SELECT查询语句各子句上线情况表](../dql/SELECT_STATEMENT.md#select语句元素)。

下表列出了在线请求模式支持的 `SELECT` 子句。

| SELECT 子句                              | 说明                                                         |
| :--------------------------------------- | :----------------------------------------------------------- |
| 单张表的简单表达式计算                   | 简单的单表查询是对一张表进行列运算、使用运算表达式或单行处理函数（Scalar Function)以及它们的组合表达式作计算。需要遵循[在线请求模式下单表查询的使用规范](#在线请求模式下单表查询的使用规范) |
| [`JOIN` 子句](../dql/JOIN_CLAUSE.md)     | OpenMLDB目前仅支持**LAST JOIN**。需要遵循[在线请求模式下LAST JOIN的使用规范](#在线请求模式下-last-join-的使用规范) |
| [`WINDOW` 子句](../dql/WINDOW_CLAUSE.md) | 窗口子句用于定义一个或者若干个窗口。窗口可以是有名或者匿名的。用户可以在窗口上调用聚合函数进行分析计算。需要遵循[在线请求模式下Window的使用规范](#在线请求模式下window的使用规范) |

## 在线请求模式下 `SELECT` 子句的使用规范

### 在线请求模式下单表查询的使用规范

- 仅支持列运算，表达式，以及单行处理函数（Scalar Function)以及它们的组合表达式运算。
- 单表查询不包含[GROUP BY子句](../dql/JOIN_CLAUSE.md)，[WHERE子句](../dql/WHERE_CLAUSE.md)，[HAVING子句](../dql/HAVING_CLAUSE.md)、[WINDOW子句](../dql/WINDOW_CLAUSE.md)， [LIMIT 子句](../dql/LIMIT_CLAUSE.md)。
- 单表查询只涉及单张表的计算，不涉及[JOIN](../dql/JOIN_CLAUSE.md)多张表的计算。

**Example: 支持上线的简单SELECT查询语句范例**

```sql
-- desc: SELECT所有列
SELECT * FROM t1;
  
-- desc: SELECT 表达式重命名
SELECT COL1 as c1 FROM t1;
 
-- desc: SELECT 表达式重命名2
SELECT COL1 c1 FROM t1;

-- desc: SELECT 列表达式
SELECT COL1 FROM t1;
SELECT t1.COL1 FROM t1;
 
-- desc: SELECT 一元表达式
SELECT -COL2 as COL2_NEG FROM t1;
  
-- desc: SELECT 二元表达式
SELECT COL1 + COL2 as COL12_ADD FROM t1;
 
-- desc: SELECT 类型强转 
SELECT CAST(COL1 as BIGINT) as COL_BIGINT FROM t1;
  
-- desc: SELECT 函数表达式
SELECT substr(COL7, 3, 6) FROM t1;
```

### 在线请求模式下 `LAST JOIN` 的使用规范

1. 仅支持`LAST JOIN`类型。
2. 至少有一个JOIN条件是形如`left_source.column=right_source.column`的EQUAL条件，**并且`right_source.column`列需要命中右表的索引（key 列）**。
3. 带排序LAST JOIN的情况下，`ORDER BY`只支持单列的列引用表达式，列类型为 int64 或 timestamp, **并且列需要命中右表索引的时间列**。满足条件 2 和 3 的情况我们简单称做表能被 LAST JOIN 的 JOIN  条件优化
4. 右表 TableRef
  - 可以指一张物理表, 或者子查询语句
  - 子查询情况, 目前支持
    - 简单列筛选 (`select * from tb` or `select id, val from tb`)
    - 窗口聚合子查询, 例如 `select id, count(val) over w as cnt from t1 window w as (...)`. 
      - OpenMLDB 0.8.4 之前, 如果 LAST JOIN 的右表是窗口聚合子查询, 需要和 LAST JOIN 的左表输入有相同的主表
      - [ALPHA] OpenMLDB >= 0.8.4, 允许 LAST JOIN 下的窗口聚合子查询不带主表. 详细见下面的例子
    - **OpenMLDB >= 0.8.0** 带 WHERE 条件过滤的简单列筛选 ( 例如 `select * from tb where id > 10`)
    - **[ALPHA] OpenMLDB >= 0.8.4** 右表是带 LAST JOIN 的子查询 `subquery`, 要求 `subquery` 最左的表能被 JOIN 条件优化, `subquery`剩余表能被自身 LAST JOIN 的 JOIN 条件优化 
    - **[ALPHA] OpenMLDB >= 0.8.4** LEFT JOIN. 要求 LEFT JOIN 的右表能被 LEFT JOIN 条件优化, LEFT JOIN 的左表能被上层的 LAST JOIN 条件优化

**Example: 支持上线的 `LAST JOIN` 语句范例**
创建两张表以供后续`LAST JOIN`。

```sql
CREATE DATABASE db1;
-- SUCCEED
    
USE db1;
-- SUCCEED: Database changed
    
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
-- SUCCEED

CREATE TABLE t2 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
-- SUCCEED

desc t1;
 --- ---------- ----------- ------ --------- 
  #   Field      Type        Null   Default  
 --- ---------- ----------- ------ --------- 
  1   col0       Varchar     YES             
  2   col1       Int         YES             
  3   std_time   Timestamp   YES             
 --- ---------- ----------- ------ --------- 
 --- -------------------- ------ ---------- ---------- --------------- 
  #   name                 keys   ts         ttl        ttl_type       
 --- -------------------- ------ ---------- ---------- --------------- 
  1   INDEX_0_1639524729   col1   std_time   43200min   kAbsoluteTime  
 --- -------------------- ------ ---------- ---------- --------------- 
```
在刚刚创建的两张表上进行未排序的`LAST JOIN`，`col1`命中了索引。
```sql
 -- last join without order by, 'col1' hit index
 SELECT
   t1.col1 as id,
   t1.col0 as t1_col0,
   t1.col1 + t2.col1 + 1 as test_col1,
 FROM t1
 LAST JOIN t2 ON t1.col1=t2.col1;
```
在刚刚创建的两张表上进行排序的`LAST JOIN`，`col1`命中了索引，`std_time`命中了右表的索引的时间列。
```sql
 -- last join wit order by, 'col1:std_time' hit index
 SELECT
   t1.col1 as id,
   t1.col0 as t1_col0,
   t1.col1 + t2.col1 + 1 as test_col1,
 FROM t1
 LAST JOIN t2 ORDER BY t2.std_time ON t1.col1=t2.col1;
```

右表是带 LAST JOIN 或者 WHERE 条件过滤的情况

```sql
CREATE TABLE t3 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
-- SUCCEED

SELECT
  t1.col1 as t1_col1,
  t2.col1 as t2_col1,
  t2.col0 as t2_col0
FROM t1 LAST JOIN (
  SELECT * FROM t2 WHERE strlen(col0) > 0
) t2 
ON t1.col1 = t2.col1

-- t2 被 JOIN 条件 't1.col1 = tx.t2_co1l' 优化, t3 被 JOIN 条件 't2.col1 = t3.col1'
SELECT
  t1.col1 as t1_col1,
  tx.t2_col1,
  tx.t3_col1
FROM t1 LAST JOIN (
    SELECT t2.col1 as t2_col1, t3.col1 as t3_col1
    FROM t2 LAST JOIN t3
    ON t2.col1 = t3.col1
) tx
ON t1.col1 = tx.t2_col1

-- 右表是 LEFT JOIN
SELECT
  t1.col1 as t1_col1,
  tx.t2_col1,
  tx.t3_col1
FROM t1 LAST JOIN (
    SELECT t2.col1 as t2_col1, t3.col1 as t3_col1
    FROM t2 LEFT JOIN t3
    ON t2.col1 = t3.col1
) tx
ON t1.col1 = tx.t2_col1

-- OpenMLDB 0.8.4 之前, LAST JOIN 窗口子查询需要窗口的子查询主表和当前主表一致
-- 这里都是 t1
SELECT
  t1.col1,
  tx.agg
FROM t1 LAST JOIN (
  SELECT col1, count(col2) over w as agg
  FROM t1 WINDOW w AS (
    UNION t2
    PARTITION BY col2 order by std_time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    INSTANCE_NOT_IN_WINDOW EXCLUDE CURRENT_ROW
  )
)

-- 右表是窗口聚合计算
-- OpenMLDB >= 0.8.4, 允许 t1 LAST JOIN WINDOW (t2). t1 是主表, t2 是一张副表
-- 此 SQL 和上一个例子语义一致
SELECT
  t1.col1,
  tx.agg
FROM t1 LAST JOIN (
  SELECT col1, count(col2) over w as agg
  FROM t2 WINDOW w AS (PARTITION BY col2 order by std_time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
```



### 在线请求模式下Window的使用规范

- 窗口边界仅支持`PRECEDING`和`CURRENT ROW`
- 窗口类型仅支持`ROWS`和`ROWS_RANGE`。
- 窗口`PARTITION BY`只支持列表达式，可以是多列，并且所有列需要命中索引，主表和 union source 的表都需要符合要求
- 窗口`ORDER BY`只支持列表达式，只能是单列，并且列需要命中索引的时间列，主表和 union source 的表都需要符合要求.  从 OpenMLDB 0.8.4 开始, ORDER BY 可以不写, 但需要满足额外的要求, 详见 [WINDOW CLAUSE](../dql/WINDOW_CLAUSE.md)
- 可支持使用 `EXCLUDE CURRENT_ROW`，`EXCLUDE CURRENT_TIME`，`MAXSIZE`，`INSTANCE_NOT_IN_WINDOW`对窗口进行其他特殊限制，详见[OpenMLDB特有的 WindowSpec 元素](#openmldb特有的-windowspec-元素)。
- `WINDOW UNION` source 要求，支持如下格式的子查询:
  - 表引用或者简单列筛选，例如 `t1` 或者 `select id, val from t1`。union source 和 主表的 schema 必须完全一致，并且 union source 对应的 `PARTITION BY`, `ORDER BY` 也需要命中索引
  - **Since OpenMLDB 0.8.0**, 基于 LAST JOIN 的简单列筛选，例如 `UNION (select * from t1 last join t2 ON ...)`。索引要求：
    - last join 查询满足 LAST JOIN 的上线要求，t1, t2 都是物理表
    - `PARTITION BY`, `ORDER BY` 表达式对应的列只能指向 LAST JOIN 的最左边的 table (即 t1), 并且命中索引

