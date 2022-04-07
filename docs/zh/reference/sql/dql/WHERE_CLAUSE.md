# WHERE Clause

Where 子句用于设置过滤条件，查询结果中只会包含满足条件的数据

## Syntax

```sql
WhereClause
         ::= 'WHERE' Expression
         
```

## SQL语句模版

```SQL
SELECT select_expr [,select_expr...] FROM ... WHERE where_condition
```

## 边界说明

| SELECT语句元素 | 状态                 | 说明                                                         |
| :------------- | -------------------- | :----------------------------------------------------------- |
| WHERE Clause   | Online Serving不支持 | Where 子句用于设置过滤条件，查询结果中只会包含满足条件的数据。 |

## Example

### 简单条件过滤

```SQL
-- desc: SELECT简单过滤
  sql: SELECT COL1 FROM t1 where COL1 > 10;
```

### 复杂条件简单条件过滤

```sql
-- desc: SELECT过滤条件是复杂逻辑关系表达式
  sql: SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20 or COL1 =0;
```

