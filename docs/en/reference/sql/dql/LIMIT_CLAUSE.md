# Limit Clause

The Limit clause is used to limit the number of results. Limit accept a non-negative integral followed as limit count, 0 produce empty set.

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

Limit clause is not supported in online preview mode. By practise, limit clause should used together with order by clause, to constrains the result rows into a unique order. OpenMLDB do not support order by clause yet, it is exepcted to get inconsistent results for the same query.

| `SELECT` Statement Elements                                | Offline Mode | Online Preview Mode | Online Request Mode | Note                                                                                                                                                          |
|:-----------------------------------------------------------|--------------|---------------------|---------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| LIMIT Clause                | **``✓``**    | **``x``**           | **``✓``**           | The Limit clause is used to limit the number of results. OpenMLDB currently only supports one parameter to limit the maximum number of rows of returned data. |


## Example

### SELECT with LIMIT

```SQL
  SELECT t1.COL1 c1 FROM t1 limit 10;
```

