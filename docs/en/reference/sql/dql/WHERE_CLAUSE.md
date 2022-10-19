# WHERE Clause

The Where clause is used to set filter conditions, and the query results will only contain data that meets the conditions

## Syntax

```sql
WhereClause
         ::= 'WHERE' Expression
         
```

## SQL Statement Template

```SQL
SELECT select_expr [,select_expr...] FROM ... WHERE where_condition
```

## Description
For the standalone version, `WHERE` is supported in all conditions. For the cluster version, the execution modes which support this clause are shown below.

| `SELECT` Statement Elements | Offline Mode | Online Preview Mode | Online Request Mode | Note                                                                                                                                                                                                                                                                                                                                                       |
|:----------------------------|--------------|---------------------|---------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| WHERE Clause                            | **``✓``**    | **``✓``**           |                     | The Where clause is used to set filter conditions, and only the data that meets the conditions will be included in the query result.                                                                                                                                                                                                                       |

## Example

### Simple Condition Filtering

```SQL
sql: SELECT COL1 FROM t1 where COL1 > 10;
```

### Complex Conditions Filtering

```sql
sql: SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20 or COL1 =0;
```

