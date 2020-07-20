## Simple SELECT语法

### `SELECT` Clause

Select clause can be: 

```sql
SELECT 
    [ * | expression [ [ AS ] output_name ] [, ...] ]
    FROM from_item 
    
    [ WINDOW window_name AS ( window_definition ) [, ...] ]
    [ LIMIT count]
```

#### FeSQL 1.0不支持

1. Where语句（2.0计划中）

```
select * FROM from_item [ WHERE condition ]
```

2. 多表查询，不支持

```SQL
 select *  FROM from_item [from_item, ...] 
```

3. GroupBy，不支持

```SQL
select * FROM from_item [ GROUP BY grouping_element [, ...] ]
```

4. Having语句不支持

```SQL
select * FROM from_item [ HAVING condition [, ...] ]
```

5. Order By语句不支持

```SQL
select * FROM from_item [ ORDER BY expression [ ASC | DESC | USING operator ] [ NULLS { FIRST | LAST } ] [, ...] ]
```

6. Union语句不支持

```SQL
select * FROM from_item [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]
```

7. OFFSET, FETCH, FOR语句不支持

```SQL
select * FROM from_item [ OFFSET start [ ROW | ROWS ] ]
select * FROM from_item [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]
select * FROM from_item  [ FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ OF table_name [, ...] ] [ NOWAIT | SKIP LOCKED ] [...] ]
```

## Parameters

### `expression`

expression can be one of:

```SQL
expression: 	
 		 constant | [ table_name. ] column | variable   
     | { unary_operator } expression   
     | expression { binary_operator } expression 
     | scalar_function
     | aggregate_windowed_function  
```



### `FROM` Clause

from_item can be one of:

```SQL
FROM 
    table_name [ [ AS ] alias 
    from_item join_type from_item ON join_condition
```

#### FeSQL 1.0不支持：

1. join（2.0计划中）

```SQL
FROM 
    from_item join_type from_item ON join_condition


```

> join_type is one of:
> - `[ INNER ] JOIN`
> - `LEFT [ OUTER ] JOIN`
> - `RIGHT [ OUTER ] JOIN`
> - `FULL [ OUTER ] JOIN`
> - `CROSS JOIN`

### 

### `WINDOW` Clause

The optional `WINDOW` clause has the general form

```
WINDOW window_name AS ( window_definition ) [, ...]
```

where **window_name** is a name that can be referenced from `OVER` clauses or subsequent window definitions, and **window_definition** is

```
[ PARTITION BY expression [, ...] ]
[ ORDER BY expression [, ...] ]
[ frame_clause ]
```

The `PARTITION By `

1. 支持简单的表达式
2. 不能是输出列或者输出值

The `ORDER BY`

1. 支持简单的表达式
2. 不能是输出列或者输出值

The `frame_clause`

The *frame_clause* can be one of

```SQL
{ RANGE } frame_start
{ RANGE } BETWEEN frame_start AND frame_end
```

#### **FeSQL 1.0不支持**

1. ROWS作为**frame**

```SQL
{ ROWS } frame_start
{ ROWS } BETWEEN frame_start AND frame_end
```

 `frame_start` and `frame_end` can be one of

```SQL
UNBOUNDED PRECEDING
value PRECEDING
CURRENT ROW
value FOLLOWING
UNBOUNDED FOLLOWING
```

### `LIMIT` Clause

The `LIMIT` clause consists of two independent sub-clauses:

```SQL
LIMIT { count }
```

#### FeSQL 1.0不支持：

1. OFFSET:

```SQL
OFFSET start
```



## Examples

To sum the column `len` of all films and group the results by `kind`:

```SQL
SELECT name, kind, trim(name) AS name_trim FROM film limit 5;

   kind   | name_trim
----------+-------
 Action   | abc
 Comedy   | bcd
 Drama    | efgh
 Musical  | ijk
 Romantic | nopq
```



to be continue