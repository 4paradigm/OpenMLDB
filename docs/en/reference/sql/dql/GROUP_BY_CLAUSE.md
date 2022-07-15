# GROUP BY Clause

All -group by- currently only has supports in batch mode (that is, console debugging SQL support, offline mode is still under development)

## Syntax

```SQL
GroupByClause
         ::= 'GROUP' 'BY' ByList
```

## SQL Statement Template

```sql
SELECT select_expr [,select_expr...] FROM ... GROUP BY ... 
```

## Boundary Description

| SELECT statement elements  | state          | directions                                                         |
| :-------------- | ------------- | :----------------------------------------------------------- |
| GROUP BY Clause | Online not supported | Group By clause is used to group the query result set. Grouping expression lists only support simple columns. |



## Example

### 1. Aggregate After Grouping By Column

```SQL
-- desc: simple SELECT grouping KEY
  SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1;
```

### 2. Aggregate After Grouping By Two Columns

```SQL
-- desc: simple SELECT grouping KEY
  SELECT COL1, SUM(COL2), AVG(COL2) FROM t1 group by COL1, COL0;
```

