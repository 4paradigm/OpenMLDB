# SQL 上线规范和要求

OpenMLDB 的**在线请求模式**能提供实时特征抽取服务。使用[DEPLOY](../deployment_manage/DEPLOY_STATEMENT.md)命令可以将一段SQL命令部署上线。部署成功后，用户可通过 Restful APIs 或者 SDK 实时地对请求样本作特征抽取计算。但是，并非所有的 SQL 都可以部署上线，本文定义了可上线 SQL 的规范要求。

## 在线请求模式支持的语句

OpenMLDB仅支持上线[SELECT查询语句](../dql/SELECT_STATEMENT.md)。

## 在线请求模式 `SELECT` 支持的子句

**部分SELECT查询语句不支持在在线请求模式下执行。** 详见[SELECT查询语句各子句上线情况表](../dql/SELECT_STATEMENT.md#select语句元素)。

下表列出了在线请求模式支持的 `SELECT` 子句。

| SELECT 子句                                   | 说明                                                                                                                                       |
|:-------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------|
| 单张表的简单表达式计算                                | 简单的单表查询是对一张表进行列运算、使用运算表达式或单行处理函数（Scalar Function)以及它们的组合表达式作计算。需要遵循[在线请求模式下单表查询的使用规范](#在线请求模式下单表查询的使用规范)                                 |
| [`JOIN` 子句](../dql/JOIN_CLAUSE.md)     | OpenMLDB目前仅支持**LAST JOIN**。需要遵循[在线请求模式下LAST JOIN的使用规范](#在线请求模式下last-join的使用规范)                                                           |
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

- 仅支持`LAST JOIN`类型。
- 至少有一个JOIN条件是形如`left_source.column=right_source.column`的EQUAL条件，**并且`rgith_source.column`列需要命中右表的索引（key 列）**。
- 带排序LAST JOIN的情况下，`ORDER BY`只支持单列的列引用表达式，**并且列需要命中右表索引的时间列**。

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
 LAST JOIN t2	ORDER BY t2.std_time ON t1.col1=t2.col1;
```

### 在线请求模式下Window的使用规范

- 窗口边界仅支持`PRECEDING`和`CURRENT ROW`
- 窗口类型仅支持`ROWS`和`ROWS_RANGE`。
- 窗口`PARTITION BY`只支持列表达式，并且列需要命中索引
- 窗口`ORDER BY`只支持列表达式，并且列需要命中索引的时间列
- 可支持使用 `EXCLUDE CURRENT_ROW`，`EXCLUDE CURRENT_TIME`，`MAXSIZE`，`INSTANCE_NOT_IN_WINDOW`对窗口进行其他特殊限制，详见[OpenMLDB特有的 WindowSpec 元素](openmldb特有的-windowspec-元素)。

