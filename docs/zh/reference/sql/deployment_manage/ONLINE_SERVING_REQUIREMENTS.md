# OpenMLDB SQL上线规范和要求

OpenMLDB Online Serving提供实时的特征抽取服务。OpenMLDB的[DEPLOY](../deployment_manage/DEPLOY_STATEMENT.md)命令将一段SQL文本部署到线上去。部署成功后，用户即可通过Restful API或者JDBC API实时地对请求样本作特征抽取计算。并不是所有的SQL都可以部署到线上提供服务的，OpenMLDB对上线的语句和OP是有一套规范的。

## Online Serving 语句

OpenMLDB仅支持上线[SELECT查询语句](../dql/SELECT_STATEMENT.md)。

## Online Serving Op List

值得注意的是，并非所有的SELECT查询语句都可上线，在OpenMLDB中，只有`SELECT`, `WINDOW`, `LAST JOIN` OP是可以上线的，其他OP（包括`WHERE`, `GROUP`, `HAVING`, `LIMIT`）等都是无法上线了。

本节将列出支持Online Serving的OP,并详细阐述这些OP的上线使用规范。

| SELECT语句                                 | 说明                                                         |
| :----------------------------------------- | :----------------------------------------------------------- |
| 单张表简单表达式计算                       | 在Online Serving时，支持**简单的单表查询**。所谓，简单的单表查询是对一张表的进行列、运算表达式和单行处理函数（Scalar Function)以及它们的组合表达式作计算。需要遵循[Online Serving下单表查询的使用规范](#online-serving下单表查询的使用规范) |
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)     | OpenMLDB目前仅支持**LAST JOIN**。在Online Serving时，需要遵循[Online Serving下LAST JOIN的使用规范](#online-serving下last-join的使用规范) |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md) | 窗口子句用于定义一个或者若干个窗口。窗口可以是有名或者匿名的。用户可以在窗口上调用聚合函数来进行一些分析型计算的操作（```sql agg_func() over window_name```)。在Online Serving时，需要遵循[Online Serving下Window的使用规范](#online-serving下window的使用规范) |

## Online Serving下OP的使用规范

### Online Serving下单表查询的使用规范

- 仅支持列，表达式，以及单行处理函数（Scalar Function)以及它们的组合表达式运算
- 单表查询不包含[GROUP BY子句](../dql/JOIN_CLAUSE.md)，[WHERE子句](../dql/WHERE_CLAUSE.md)，[HAVING子句](../dql/HAVING_CLAUSE.md)以及[WINDOW子句](../dql/WINDOW_CLAUSE.md)。
- 单表查询只涉及单张表的计算，不设计[JOIN](../dql/JOIN_CLAUSE.md)多张表的计算。

#### Example: 支持上线的简单SELECT查询语句范例

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

### Online Serving下LAST JOIN的使用规范

- Join type仅支持`LAST JOIN`类型
- 至少有一个JOIN条件是形如`left_table.column=right_table.column`的EQUAL条件，并且`rgith_table.column`列需要命中右表的索引
- 带排序LAST JOIN的情况下，`ORDER BY`只能支持列表达式，并且列需要命中右表索引的时间列

#### Example: 支持上线的简单SELECT查询语句范例



```sql
CREATE DATABASE db1;

USE db1;
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
-- SUCCEED: Create successfully

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
### Online Serving下Window的使用规范

- 窗口边界仅支持`PRECEDING`和`CURRENT ROW`
- 窗口类型仅支持`ROWS`和`ROWS_RANGE`
- 窗口`PARTITION BY`只能支持列表达式，并且列需要命中索引
- 窗口`ORDER BY`只能支持列表达式，并且列需要命中索引的时间列

