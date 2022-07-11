# GROUP BY Clause

## Syntax

```SQL
GroupByClause
         ::= 'GROUP' 'BY' ByList
```

## SQL语句模版

```sql
SELECT select_expr [,select_expr...] FROM ... GROUP BY ... 
```

## 边界说明
在单机版中，所有执行模式均支持`GROUP BY`。集群版各执行模式的支持情况如下。
| SELECT语句元素  | 状态          | 说明                                                         |
| :-------------- | ------------- | :----------------------------------------------------------- |
| GROUP BY Clause | 仅支持`Offline`模式，`Online`和`Request`模式均不支持| Group By 子句用于对查询结果集进行分组。分组表达式列表仅支持简单列，如`group by c1,c2,...` ，不支持较复杂的写法。|



## Example

### 1. 按列分组后聚合

```SQL
>SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1;
```

### 2. 按两列分组后聚合

```SQL
>SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1, COL0;
```

