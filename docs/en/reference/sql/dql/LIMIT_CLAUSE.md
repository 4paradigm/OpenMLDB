# Limit Clause

The Limit clause is used to limit the number of results. OpenMLDB currently only supports one parameter to limit the maximum number of rows of returned data.

## Syntax

```sql
LimitClause
         ::= 'LIMIT' int_leteral
```

## SQL Statement Template

```SQL
SELECT ... LIMIT ...
```

## Description
For the standalone version, `LIMIT` is supported in all conditions. For the cluster version, the execution modes, which support this clause, are shown below.

| `SELECT` Statement Elements                                | Offline Mode | Online Preview Mode | Online Request Mode | Note                                                                                                                                                          |
|:-----------------------------------------------------------|--------------|---------------------|---------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| LIMIT Clause                | **``✓``**    | **``✓``**           | **``✓``**           | The Limit clause is used to limit the number of results. OpenMLDB currently only supports one parameter to limit the maximum number of rows of returned data. |


## Example

### SELECT with LIMIT

```SQL
  SELECT t1.COL1 c1 FROM t1 limit 10;
```

