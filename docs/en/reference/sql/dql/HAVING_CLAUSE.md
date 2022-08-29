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

## Description
For the standalone version, `HAVING` is supported in all conditions. For the cluster version, the execution modes, which support this clause, are shown below

| `SELECT` Statement Elements                                | Offline Mode | Online Preview Mode | Online Request Mode | Note                                                                                                                                                               |
|:-----------------------------------------------------------|--------------|---------------------|---------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| HAVING Clause                | **``✓``**    |                     |                     | The Having clause is similar to the Where clause. The Having clause filters data after GroupBy, and the Where clause is used to filter records before aggregation. |                                                                                                                                                                                                                                                                                                                                                                              |

## Example

 **1. Filter By Aggregation Results After Grouping**

```SQL
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1 HAVING SUM(COL2) > 1000;
```

 **2. Filter By Aggregation Result After Grouping By Two Columns**

```sql
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1, COL0 HAVING SUM(COL2) > 1000;
```

 **3. Filter By Grouping Column After Grouping**

```sql
SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1 HAVING COL1 ='a';
```

