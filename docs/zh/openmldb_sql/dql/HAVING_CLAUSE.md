# Having Clause

Having 子句与 Where 子句作用类似.Having 子句过滤 GroupBy 后的各种数据，Where 子句在聚合前进行过滤。

## Syntax

```
HavingClause
         ::= 'HAVING' Expression 
```

## SQL语句模版

```sql
SELECT select_expr [,select_expr...] FROM ... GROUP BY ... HAVING having_condition
```

## 边界说明
在单机版中，所有执行模式均支持`HAVING`。集群版各[执行模式](https://openmldb.ai/docs/zh/main/tutorial/modes.html)的支持情况如下。

| SELECT语句元素                                 | 离线模式  | 在线预览模式 | 在线请求模式 | 说明                                                                   |
| :--------------------------------------------- | --------- | ------------ | ------------ |:---------------------------------------------------------------------|
| HAVING Clause     | **``✓``** |   **``✓(since 0.6.4)``**  |              | Having 子句与 Where 子句作用类似。Having 子句过滤 GroupBy 后的各种数据，Where 子句在聚合前进行过滤。 |


## Example
**1. 分组后按聚合结果过滤**

```SQL
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1 HAVING SUM(COL2) > 1000;
```

**2. 两列分组后按聚合结果过滤**

```sql
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1, COL0 HAVING SUM(COL2) > 1000;
```

**3. 分组后按分组列过滤**

```sql
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1 HAVING COL1 ='a';
```

