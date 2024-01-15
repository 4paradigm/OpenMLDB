# WITH CLAUSE

OpenMLDB WITH CLAUSE support starts in 0.7.2 as an experimental feature, and may stabilized in feature release.

It is highly inspired by [WITH CLAUSE in BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause), but only supports non recursive with clause.

A WITH clause contains one or more common table expressions (CTEs). A CTE acts like a temporary table that you can reference within a single query expression. Each CTE binds the results of a subquery to a table name, which can be used elsewhere in the same query expression, but rules apply.

## Syntax

```
'WITH' non_recursive_cte [, ... ]

non_recursive_cte
         ::= cte_name 'AS' '(' SelectStmt ')'
```

## Description

WITH clause can be treated as another form of subquery, same online request mode restriction applied for subquery and WITH clause.

| `SELECT` Statement Elements   | Offline Mode | Online Preview Mode | Online Request Mode | Note   |
|:--------------------------|--------------|---------------------|---------------------|:-------|
| WITH Clause                | **``✓``**    | **``✓``**           | **``✓``**           |  |

## CTE rules

1. CTEs can be referenced inside the query expression that contains the  `WITH`  clause.
   This means the CTEs can be referenced by the outermost query contains the `WITH` clause,
   as well as subqueries inside the outermost query.
2. Each CTE in the same `WITH` clause must have a unique name.
3. A local CTE can overrides an outer CTE or table with the same name.

## Example

1. CTE overrides table name

   ```sql
   WITH t1 as (select col1 + 1 as id, std_ts from t1)
   select * from t1;
   ```

2. nested WITH clause
   ```sql
   WITH q1 AS (my_query)
   SELECT *
   FROM
     (WITH q2 AS (SELECT * FROM q1),  # q1 resolves to my_query
           q3 AS (SELECT * FROM q1),  # q1 resolves to my_query
           q1 AS (SELECT * FROM q1),  # q1 (in the query) resolves to my_query
           q4 AS (SELECT * FROM q1)   # q1 resolves to the WITH subquery on the previous line.
       SELECT * FROM q1)              # q1 resolves to the third inner WITH subquery.
  ```
