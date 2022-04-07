# Limit Clause

Limit 子句用于限制结果条数。OpenMLDB 目前仅支持Limit 接受一个参数，表示返回数据的最大行数；

## Syntax

```sql
LimitClause
         ::= 'LIMIT' int_leteral
```

## SQL语句模版

```SQL
SELECT ... LIMIT ...
```

## 边界说明

| SELECT语句元素 | 边界                 | 说明                                                         |
| :------------- | -------------------- | :----------------------------------------------------------- |
| LIMIT Clause   | 不支持Online Serving | Limit 子句用于限制结果条数。OpenMLDB 目前仅支持Limit 接受一个参数，表示返回数据的最大行数； |

## Example

### SELECT with LIMIT

```SQL
-- desc: SELECT Limit
  SELECT t1.COL1 c1 FROM t1 limit 10;
```

