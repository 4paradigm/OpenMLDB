# Having Clause

Having clause is similar to the Where clause. The Having clause allows you to filter various data after GroupBy, and the Where clause is used to filter records before aggregation.

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

## Example

### 1. Filter By Aggregation Results After Grouping

```SQL
-- desc: aggregate filtering after grouping
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1 HAVING SUM(COL2) > 1000;
```

### 2. Filter By Aggregation Result After Grouping By Two Columns

```sql
-- desc: aggregate filtering after grouping
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1, COL0 HAVING SUM(COL2) > 1000;
```

### 3. Filter By Grouping Column After Grouping

```sql
-- desc: aggregate filtering after grouping
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1 HAVING COL2 > 1000;
```

