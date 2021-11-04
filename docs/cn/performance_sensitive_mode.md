# OpenMLDB 性能敏感模式说明

## 概念

性能敏感模式是OpenMLDB特有的一种执行模式，在该模式下，未经索引优化的复杂查询计划将被判定性能低下，即而被拒绝执行。
显然，当关闭性能敏感开关时，这种限制就不再存在。只要符合OpenMLDB语法的查询语句都可以被允许执行。
示例：
我们在数据库中建立一张表`t1`,为它配置一个`(key = col2, ts = col4)`的索引：
```sqlite
CREATE table t1(
    col1 string,
    col2 string,
    col3 double,
    col4 timestamp,
    index(key = col2, ts = col4));
```
### 场景1： 对于下面的复杂SQL查询语句：
   
- 性能敏感模式打开时，由于窗口的`PARTITION BY` key `col1`未能命中表`t1`的索引，所以无法进行查询计划优化。最终，系统判定其性能差，拒绝执行。
- 性能敏感模式关闭时，由于没有性能限制，可以正常执行。
```sql
-- 
SELECT col1, col2, SUM(col3) over w1 as w1_sum_col3 from t1 
    window w1 as (PARTITION BY col1 ORDER BY col4 ROWS 
BETWEEN 100 PRECEDING AND CURRENT ROW);
```
### 场景2： 对于下面的复杂SQL查询语句:
   
- 性能敏感模式打开时，由于窗口的PARTITION `col2`和`ORDER BY` key `col4`成功命中表`t1`的索引，所以查询计划可以被充分优化。因此，可以正常执行。
- 性能敏感模式关闭时，由于没有性能限制，可以正常执行。
```sql
-- 
SELECT col1, col2, SUM(col3) over w2 as w2_sum_col3 from t1 
    window w2 as (PARTITION BY col2 ORDER BY col4 ROWS 
BETWEEN 100 PRECEDING AND CURRENT ROW);
```

## 使用方式

### 默认配置说明
- OpenMLDB的在线执行时，性能敏感模式默认是打开的。
- OpenMLDB的控制台中，性能敏感模式默认时打开的。
- OpenMLDB的离线执行时，性能敏感模式默认是关闭的。

### 控制台配置性能敏感

OpenMLDB的控制台提供了性能敏感模式的配置接口。
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
## 未来工作
OpenMLDB目前仅在控制台提供性能敏感的配置。未来将在更多的SDK和客户端提供性能敏感开关配置。
