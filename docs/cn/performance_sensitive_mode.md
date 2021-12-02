# OpenMLDB 性能敏感模式说明

## 1. 介绍

性能敏感模式是 OpenMLDB 特有的一种执行模式，在该模式下，未经索引优化的查询计划将被判定性能低下，即而被拒绝执行。当关闭性能敏感开关时，这种限制就不再存在。只要符合 OpenMLDB 语法的查询语句都可以被允许执行。

**注意：目前性能敏感模式的配置仅对单机模式有效，集群模式目前默认强制均为性能敏感模式。**

性能敏感模式的判定依赖于建立表时的 `index` 选项：`CREATE TABLE ...(..., index(key=..., ts=...))` 。其中，`key` 代表了索引列，`ts` 代表了将被用来可以做排序 `ORDER BY` 的有序列。性能敏感模式将会影响到以下 SQL 语句，如果其中的条件不能满足，则必须在非性能敏感模式下才能执行。

- `PARTITION BY`：必须命中 `key` 所指定的索引列
- `ORDER BY`：必须命中 `ts` 所指定的有序列
- `WHERE`：必须命中 `key` 所指定的索引列
- `GROUP BY`：必须命中 `key` 所指定的索引列
- `LAST JOIN`：必须命中 `key` 所指定的索引列

## 2. 示例

我们在数据库中建立一张表`t1`，为它配置一个`(key = col2, ts = col4)`的索引：

```sql
CREATE TABLE t1(
    col1 string,
    col2 string,
    col3 double,
    col4 timestamp,
    INDEX(key=col2, ts=col4));
```
**场景1：**

```sql
SELECT col1, col2, SUM(col3) OVER w1 AS w1_sum_col3 FROM t1 
    WINDOW w1 AS (PARTITION BY col1 ORDER BY col4 
    ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);
```
- 性能敏感模式打开时，由于窗口的 `PARTITION BY col1` 未能命中表 `t1` 的索引 `col2`，所以无法进行查询计划优化。最终，系统判定其性能差，拒绝执行。
- 性能敏感模式关闭时，由于没有性能限制，可以正常执行。

**场景2**

```sql
SELECT col1, col2, SUM(col3) OVER w2 AS w2_sum_col3 FROM t1 
    WINDOW w2 AS (PARTITION BY col2 ORDER BY col4 
    ROWS BETWEEN 100 PRECEDING AND CURRENT ROW);
```

- 性能敏感模式打开时，由于窗口的 `PARTITION BY col2` 和 `ORDER BY col4`  成功命中表`t1`的索引 `col2` 和 有序列 `col4`，所以查询计划可以被充分优化。因此，可以正常执行。
- 性能敏感模式关闭时，由于没有性能限制，也可以正常执行。

## 3. 模式配置方式（只针对单机模式）

### 3.1. 默认配置说明
- OpenMLDB 的在线执行时，性能敏感模式默认是打开的。
- OpenMLDB 的控制台中，性能敏感模式默认时打开的。
- OpenMLDB 的离线执行时，性能敏感模式默认是关闭的。

### 3.2. 控制台配置性能敏感

OpenMLDB 的控制台提供了性能敏感模式的配置接口。
- 通过`SET performance_sensitive=true`可以打开性能敏感模式
```sqlite
> CREATE DATABASE db1;
> USE db1;
> SET performance_sensitive=true
```

- 通过`SET performance_sensitive=false`可以关闭性能敏感模式
```sqlite
> CREATE DATABASE db1;
> USE db1;
> SET performance_sensitive=false
```
## 4. 未来工作
OpenMLDB 目前仅在控制台提供针对单机模式的性能敏感的配置。未来将在更多的SDK和客户端提供性能敏感开关配置。
