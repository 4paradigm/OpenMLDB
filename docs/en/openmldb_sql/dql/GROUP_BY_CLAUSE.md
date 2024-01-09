# GROUP BY Clause

## Syntax

```SQL
GroupByClause
         ::= 'GROUP' 'BY' ByList
```

## SQL Statement Template

```sql
SELECT select_expr [,select_expr...] FROM ... GROUP BY ... 
```

## Description

For the standalone version, `GROUP BY` is supported in all conditions. For the cluster version, the execution modes which support this clause are shown below.

| `SELECT` Statement Elements                                | Offline Mode | Online Preview Mode | Online Request Mode | Note                                                                                                                    |
|:-----------------------------------------------------------|--------------|---------------------|---------------------|:------------------------------------------------------------------------------------------------------------------------|
| GROUP BY Clause            | **``✓``**    |  **``✓(since 0.6.4)``**  |                     | The Group By clause is used to group the query results.The grouping conditions only support grouping on simple columns. |

## Example

**1. Aggregate After Grouping By One Column**

```SQL
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1;
```

**2. Aggregate After Grouping By Two Columns**

```SQL
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1, COL0;
```

