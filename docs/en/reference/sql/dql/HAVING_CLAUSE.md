# Having Clause

Having clause is similar to the Where clause. The Having clause filters data after GroupBy, and the Where clause is used to filter records before aggregation.

## Syntax

```
HavingClause
         ::= 'HAVING' Expression 
```

## SQL Statement Template

```sql
SELECT select_expr [,select_expr...] FROM ... GROUP BY ... HAVING having_condition
```

## Boundary Description

| SELECT statement elements | state          | directions                                                         |
| :------------- | ------------- | :----------------------------------------------------------- |
| HAVING Clause  | Online not supported | Having clause is similar to the Where clause. The Having clause allows you to filter various data after GroupBy, and the Where clause is used to filter records before aggregation. |


| [`HAVING` Clause](../dql/HAVING_CLAUSE.md) | **``✓``** | | | Having 子句与 Where 子句作用类似.Having 子句过滤 GroupBy 后的各种数据，Where 子句在聚合前进行过滤。 |

## Example

### 1. Filter By Aggregation Results After Grouping

```SQL
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1 HAVING SUM(COL2) > 1000;
```

### 2. Filter By Aggregation Result After Grouping By Two Columns

```sql
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1, COL0 HAVING SUM(COL2) > 1000;
```

### 3. Filter By Grouping Column After Grouping

```sql
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1 HAVING COL1 ='a';
```

