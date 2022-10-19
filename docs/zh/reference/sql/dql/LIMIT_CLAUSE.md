# Limit Clause

Limit子句用于限制返回的结果条数。目前Limit仅支持接受一个参数，表示返回数据的最大行数。

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

| SELECT语句元素 | 边界 | 说明                                                         |
| :------------- |--| :----------------------------------------------------------- |
| LIMIT Clause   | 单机版和集群版的所有执行模式均支持 | Limit 子句用于限制返回的结果条数。目前Limit仅支持接受一个参数，表示返回数据的最大行数。 |

## Example

### SELECT with LIMIT

```SQL
SELECT t1.COL1 c1 FROM t1 limit 10;
```

