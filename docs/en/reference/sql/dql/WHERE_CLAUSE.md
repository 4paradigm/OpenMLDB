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

## Boundary Description

| SELECT statement elements | state                 | illustrate                                                         |
| :------------- | -------------------- | :----------------------------------------------------------- |
| WHERE Clause   | Online Serving not supportED | The Where clause is used to set filter conditions, and only the data that meets the conditions will be included in the query result. |

## Example

### Simple Conditional Filtering

```SQL
-- desc: SELECT simple filter
  sql: SELECT COL1 FROM t1 where COL1 > 10;
```

### Complex Conditions Simple Condition Filtering

```sql
-- desc: The SELECT filter condition is a complex logical relational expression
  sql: SELECT COL1 FROM t1 where COL1 > 10 and COL2 = 20 or COL1 =0;
```

