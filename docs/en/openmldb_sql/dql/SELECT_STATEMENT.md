# SELECT Overview

## Syntax Notation

- Square brackets `[ ]`: Optional clause.
- Curly braces with vertical bars `{ a | b | c }`: Logical OR. Select one option.
- Ellipsis `...`: Preceding item can repeat.

## Syntax

```yacc
query_statement:
  query [ CONFIG ( { key = value }[, ...] )]

query:
  [ WITH {non_recursive_cte}[, ...] ]
  { select | ( query ) | set_operation }
  [ ORDER BY ordering_expression ]
  [ LIMIT count ]

select:
  SELECT select_list
  [ FROM from_item ]
  [ WHERE bool_expression ]
  [ GROUP BY group_by_specification ]
  [ HAVING bool_expression ]
  [ window_clause ]

set_operation:
  query set_operator query

non_recursive_cte:
  cte_name AS ( query )

set_operator:
  UNION { ALL | DISTINCT }

from_item:
  table_name [ as_alias ]
  | { join_operation | ( join_operation ) }
  | ( query ) [ as_alias ]
  | cte_name [ as_alias ]

as_alias:
  [ AS ] alias_name

join_operation:
  condition_join_operation

condition_join_operation:
  from_item LEFT [ OUTER ] JOIN from_item join_condition
  | from_item LAST JOIN [ ORDER BY ordering_expression ] from_item join_condition

join_condition:
  ON bool_expression

window_clause:
  WINDOW named_window_expression [, ...]

named_window_expression:
  named_window AS { named_window | ( window_specification ) }

window_specification:
  [ UNION ( from_item [, ...] ) ]
  PARTITION BY expression [ ORDER BY ordering_expression ]
  window_frame_clause [ window_attr [, ...] ]

window_frame_clause:
  frame_units BETWEEN frame_bound AND frame_bound [ MAXSIZE numeric_expression ] )

frame_unit:
  ROWS 
  | ROWS_RANGE

frame_boud:
  { UNBOUNDED | numeric_expression | interval_expression } [ OPEN ] PRECEDING
  | CURRENT ROW

window_attr:
  EXCLUDE CURRENT_TIME
  | EXCLUDE CURRENT_ROW
  | INSTANCE_NOT_IN_WINDOW

// each item in select list is one of:
// - *
// - expression.*
// - expression
select_list:
  { select_all | select_expression } [, ...]

select_all:
  [ expression. ]*

select_expression:
  expression [ [ AS ] alias ]
```

## SELECT Statement


| `SELECT` Statement and Related Clauses                    | Offline Mode | Online Preview Mode | Online Request Mode | Note                                                                                                                                                                                                                                                                                                                                                                                                                    |
|:-----------------------------------------------|--------------|---------------------|---------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`SELECT Clause`](#selectexprlist)             | **``✓``**    | **``✓``**           | **``✓``**           | A list of projection operations, generally including column names, expressions, or ‘*’ for all columns.                                                                                                                                                                                                                                                                                                                 |
| [`FROM Clause`](#tablerefs)                    | **``✓``**    | **``✓``**           | **``✓``**           | The FROM clause indicates the data source.<br />The data source can be one table (`select * from t;`) or multiple tables that LAST JOIN together (see [JOIN CLAUSE](../dql/JOIN_CLAUSE.md)) or no table ( `select 1+1;`), see [NO_TABLE SELECT](../dql/NO_TABLE_SELECT_CLAUSE.md)                                                                                                                                       |
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)         | **``✓``**    | **``x``**           | **``✓``**           | The JOIN clause indicates that the data source comes from multiple joined tables. OpenMLDB currently only supports LAST JOIN. For Online Request Mode, please follow [the specification of LAST JOIN under Online Request Mode](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#the-usage-specification-of-last-join-under-online-serving)                                                                          |
| [`WHERE` Clause](../dql/WHERE_CLAUSE.md)       |             | **``✓``**           |                     | The WHERE clause is used to set filter conditions, and only the data that meets the conditions will be included in the query result.                                                                                                                                                                                                                                                                                    |
| [`GROUP BY` Clause](../dql/GROUP_BY_CLAUSE.md) | **``✓``**    |  **``✓``**  |                     | The GROUP BY clause is used to group the query results.The grouping conditions only support simple columns.                                                                                                                                                                                                                                                                                                             |
| [`HAVING` Clause](../dql/HAVING_CLAUSE.md)     | **``✓``**    |  **``✓``**  |                     | The HAVING clause is similar to the WHERE clause. The HAVING clause filters data after GROUP BY, and the WHERE clause is used to filter records before aggregation.                                                                                                                                                                                                                                                     |                                                                                                                                                                                                                                                                                                                                                                              |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md)     | **``✓``**    |  **``✓``**  | **``✓``**           | The WINDOW clause is used to define one or several windows. Windows can be named or anonymous. Users can call aggregate functions on the window to perform analysis (```sql agg_func() over window_name```). For Online Request Mode, please follow the [specification of WINDOW Clause under Online Request Mode](../deployment_manage/ONLINE_REQUEST_REQUIREMENTS.md#window-usage-specification-under-online-serving) |
| [`LIMIT` Clause](../dql/LIMIT_CLAUSE.md)       | **``✓``**    | **``✓``**           |                     | The LIMIT clause is used to limit the number of results. OpenMLDB currently only supports one parameter to limit the maximum number of rows of returned data.                                                                                                                                                                                                                                                           |
| `ORDER BY` Clause                              |              |                     |                     | Standard SQL also supports the ORDER BY keyword, however OpenMLDB does not support this keyword currently. For example, the query `SELECT * from t1 ORDER BY col1;` is not supported in OpenMLDB.                                                                                                                                                                                                                       |

```{warning}
The `SELECT` running in online mode or the stand-alone version may not obtain complete data.
The largest number of bytes to scan is limited, namely `scan_max_bytes_size`, default value is unlimited. But if you set the value of `scan_max_bytes_size` to a specific value, the `SELECT` statement will only scan the data within the specified size. If the select results are truncated, the message of `reach the max byte ...` will be recorded in the tablet's log, but there will be no error.

Even if the `scan_max_bytes_size` is set to unlimited, the `SELECT` statement may failed, e.g. client errors `body_size=xxx from xx:xxxx is too large`, ` Fail to parse response from xx:xxxx by baidu_std at client-side`. We don't recommend to use `SELECT` in online mode or the stand-alone version. If you want to get the count of the online table, please use `SELECT COUNT(*) FROM table_name;`.
```

## CONFIG clause

`query_statement` is able to take optional CONFIG clause, as `CONFIG ( key_string = value_expr, ...)`, which make extra configuration over current query. Supported keys and values are:

| key_string | value_expr type | Note |
| ---------- | ---------  | ---- |
| execute_mode | string | SQL `execute_mode`, choose one: `online`, `request`, `offline`. default to value of system variable `execute_mode`. You can view it via SQL `show variables` |
| values | Any valid expression | See [SQL request query in RAW SQL](#sql-request-query-in-raw-sql) |

## SQL request query in raw SQL

OpenMLDB >= 0.9.0 make it possible for a query statement to run as request mode without extra request row info passed in , for example from one of the parameter in JAVA SDK. Those request row informations are
instead allowed inside CONFIG clause, as `execute_mode` and `values`. When CONFIG `execute_mode = 'request'`，it firstly parse request row value in CONFIG `values`.
CONFIG `values` supports two formats：
1. Parentheses `()` surrounded expression list, representing single request row. For example `(1, "str", timestamp(1000) )` 
2. Square brackets `[]` surrounded parentheses expression lists, say it is surrounding N parentheses expressions, representing N request rows. For example `[ (1, "str", timestamp(1000)), (2, "foo", timestamp(5000)) ]`

Parentheses `()` expression is the minimal unit to a request row, every expression inside parentheses should match exactly to the data type of request table schema, which current SQL contains.

```sql
-- table t1 of schema ( id int, val string, ts timestamp )

-- executing SQL as request mode, with request row (10, "foo", timestamp(4000))
SELECT id, count (val) over (partition by id order by ts rows between 10 preceding and current row)
FROM t1
CONFIG (execute_mode = 'online', values = (10, "foo", timestamp (4000)))
```

