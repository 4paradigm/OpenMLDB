# GROUP BY Clause

所有的group by目前仅仅批模式支持（也就是控制台的调试SQL支持，离线模式还是开发中）

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

| SELECT语句元素  | 状态          | 说明                                                         |
| :-------------- | ------------- | :----------------------------------------------------------- |
| GROUP BY Clause | Online 不支持 | Group By 子句用于对查询结果集进行分组。分组表达式列表仅支持简单列。 |



## Example

### 1. 按列分组后聚合

```SQL
-- desc: 简单SELECT分组KEY
  SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1;
```

### 2. 按两列分组后聚合

```SQL
-- desc: 简单SELECT分组KEY
  SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1, COL0;
```

