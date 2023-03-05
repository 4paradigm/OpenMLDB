# With Clause

OpenMLDB 在 0.7.2 开始支持 WITH 子句，目前作为一个实验特性，可能在未来版本中稳定。

WITH 子句借鉴了 [WITH Clause in BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#with_clause) 的语法，但仅支持非递归形式。

一个 WITH 子句包括一个或多个 common table expressions (CTEs). 在同一个查询语句内, CTE 的作用类似一张临时表, 可以被后续子句引用。

## Syntax

```
'WITH' non_recursive_cte [, ... ]

non_recursive_cte
         ::= cte_name 'AS' '(' SelectStmt ')'
```

## Description

WITH 子句实现了层次化的数据模型，在实现上等价于子查询，因此和子查询有相同的上线要求。

| SELECT语句元素 | 离线模式  | 在线预览模式 | 在线请求模式 | 说明  |
|:--------------------------|--------------|---------------------|---------------------|:-------|
| WITH Clause                | **``✓``**    | **``✓``**           | **``✓``**           |  |


## CTE rules

1. 一个 `CTE` 语句，可以被 1.和该 CTE 同一层的 `SelectStmt` 2.该 CTE 之后的其它 CTEs 引用。
2. 同一层的 WITH 子句下有多个 CTE, 每个 `cte_name` 必须唯一。
3. 一个查询语句包含多层 WITH 子句，更里层的 `CTE` 覆盖外层 `cte_name` 相同的 `CTE`。

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
