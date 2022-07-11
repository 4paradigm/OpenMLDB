# Limit Clause

The Limit clause is used to limit the number of results. OpenMLDB currently only supports Limit accepting one parameter, indicating the maximum number of rows of returned data;

## Syntax

```sql
LimitClause
         ::= 'LIMIT' int_leteral
```

## SQL Statement Template

```SQL
SELECT ... LIMIT ...
```

## Boundary Description

| SELECT statement elements | state                 | direction                                                         |
| :------------- | -------------------- | :----------------------------------------------------------- |
| LIMIT Clause   | Online Serving is not supported | The Limit clause is used to limit the number of results. OpenMLDB currently only supports Limit accepting one parameter, indicating the maximum number of rows of returned data； |

## Example

### SELECT with LIMIT

```SQL
-- desc: SELECT Limit
  SELECT t1.COL1 c1 FROM t1 limit 10;
```

