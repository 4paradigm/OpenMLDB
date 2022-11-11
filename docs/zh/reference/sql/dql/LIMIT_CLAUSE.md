# Limit Clause

Limit子句用于限制返回的结果条数。Limit支持接受一个参数，表示返回数据的最大行数, 参数必须是大于或等于 0 的整数，0 表示查询结果为空。

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

Limit 子句不支持在线请求模式。通常来讲 limit 子句需要和 order by 子句共同使用，以确保得到唯一确定的结果。OpenMLDB 尚未支持 order by 子句，因此 Limit 查询可能会返回不一致的结果。

| SELECT语句元素 | 离线模式  | 在线预览模式 | 在线请求模式 | 说明                                                         |
| :------------- | --------- | ------------ | ------------ | :----------------------------------------------------------- |
| LIMIT Clause   | **``✓``** | **``✓``**    | **``x``**    |Limit 子句用于限制返回的结果条数。目前Limit仅支持接受一个参数，表示返回数据的最大行数。 |

## Example

### SELECT with LIMIT

```SQL
SELECT t1.COL1 c1 FROM t1 limit 10;
```

