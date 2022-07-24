# WINDOW Clause

## Syntax

```sql
WindowClauseOptional
         ::= ( 'WINDOW' WindowDefinition ( ',' WindowDefinition )* )?
WindowDefinition
         ::= WindowName 'AS' WindowSpec

WindowSpec
         ::= '(' WindowSpecDetails ')'   
         
WindowSpecDetails
         ::= [ExistingWindowName] [WindowUnionClause] WindowPartitionClause WindowOrderByClause WindowFrameClause [WindowExcludeCurrentTime] [WindowInstanceNotInWindow]


WindowUnionClause
				 :: = ( 'UNION' TableRefs)

WindowPartitionClause
         ::= ( 'PARTITION' 'BY' ByList ) 

WindowOrderByClause
         ::= ( 'ORDER' 'BY' ByList )    


WindowFrameClause
         ::= ( WindowFrameUnits WindowFrameExtent [WindowFrameMaxSize]) 

WindowFrameUnits
         ::= 'ROWS'
           | 'ROWS_RANGE'         

WindowFrameExtent
         ::= WindowFrameStart
           | WindowFrameBetween
WindowFrameStart
         ::= ( 'UNBOUNDED' | NumLiteral | IntervalLiteral ) ['OPEN'] 'PRECEDING'
           | 'CURRENT' 'ROW'
WindowFrameBetween
         ::= 'BETWEEN' WindowFrameBound 'AND' WindowFrameBound
WindowFrameBound
         ::= WindowFrameStart
           | ( 'UNBOUNDED' | NumLiteral | IntervalLiteral ) ['OPEN'] 'FOLLOWING'  
           
WindowExcludeCurrentTime 
				::= 'EXCLUDE' 'CURRENT_TIME'      

WindowInstanceNotInWindow
				:: = 'INSTANCE_NOT_IN_WINDOW'
```

*Window call function* is similar to aggregate functions. The difference is that the window call function does not need to pack the query results into a single line when output the results. Instead, each line is separated when using WINDOW clause. 
However, the window caller can scan all rows that may be part of the current row's group, depending on the grouping specification of the window caller (the `PARTITION BY` on columns).
The syntax for calling a function over a window is shown bellow:

```
function_name ([expression [, expression ... ]]) OVER ( window_definition )
function_name ([expression [, expression ... ]]) OVER window_name
function_name ( * ) OVER ( window_definition )
function_name ( * ) OVER window_name
```

## SQL Statement Template

- ROWS WINDOW SQL template

```sqlite
SELECT select_expr [, select_expr ...], window_function_name(expr) OVER window_name, ... FROM ... WINDOW AS window_name (PARTITION BY ... ORDER BY ... ROWS BETWEEN ... AND ...)

```

- ROWS RANGE WINDOW SQL Template

```sql
SELECT select_expr [,select_expr...], window_function_name(expr) OVER window_name, ... FROM ... WINDOW AS window_name (PARTITION BY ... ORDER BY ... ROWS_RANEG BETWEEN ... AND ...)
```

## Boundary Description

| `SELECT` Statement Elements                            | Offline Mode | Online Preview Mode | Online Request Mode | Note                                                                                                                                                                                                                                                                                                                                                       |
|:-------------------------------------------------------|--------------|---------------------|---------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| WINDOW Clause                                          | **``✓``**    |                     | **``✓``**           | The window clause is used to define one or several windows. Windows can be named or anonymous. Users can call aggregate functions on the window to perform analysis (```sql agg_func() over window_name```). During Online Serving, please follow [Online Serving下Window的使用规范](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.html#online-servingwindow) |

## Basic WINDOW SPEC Syntax Elements

### Window Partition Clause And Window OrderBy Clause

```sql
WindowPartitionClause
         ::= ( 'PARTITION' 'BY' ByList )

WindowOrderByClause
         ::= ( 'ORDER' 'BY' ByList )            
```

The `PARTITION BY` option groups the rows of the query into *partitions*, which are processed separately in the window function. `PARTITION BY` and the query level `GROUP BY` clause do similar work, except that its expressions can only be used as expressions and not as output column names or numbers. OpenMLDB requires that `PARTITION BY` must be configured. And currently **only supports grouping by column**, cannot support grouping by operation and function expression.

The `ORDER BY` option determines the order in which the rows in the partition are processed by the window function. It does a similar job as a query-level `ORDER BY` clause, but again it cannot be used as an output column name or number. Likewise, OpenMLDB requires that `ORDER BY` must be configured. And currently **only supports sorting by column**, and cannot support sorting by operation and function expression.

### Window Frame Units

```sql
WindowFrameUnits
         ::= 'ROWS'
           | 'ROWS_RANGE' 
```

WindowFrameUnits defines the frame type of the window. OpenMLDB supports two types of window frames: ROWS and ROWS_RANGE

The SQL standard RANGE class window OpenMLDB system does not currently support it. Their direct comparison differences are shown in the figure below

![Figure 1: window frame type](../dql/images/window_frame_type.png)

- ROWS: The window is drawn into the window by rows, and the window is slid out according to the number of rows
- ROWS_RANGE: The window is drawn into the window by rows, and slides out of the window according to the time interval
- RANGE: The window is divided into the window according to the time granularity (may slide in multiple data rows at the same time at a time), and slide out of the window according to the time interval

### Window Frame Extent

```sql
WindowFrameExtent
         ::= WindowFrameStart
           | WindowFrameBetween
WindowFrameBetween
         ::= 'BETWEEN' WindowFrameBound 'AND' WindowFrameBound
WindowFrameBound
         ::= ( 'UNBOUNDED' | NumLiteral | IntervalLiteral ) ['OPEN'] 'PRECEDING'
           | 'CURRENT' 'ROW'
```

 **WindowFrameExtent** defines the upper and lower bounds of a window. The window type can be defined by `ROWS` or `ROWS_RANGE`.

- `CURRENT ROW` is the row currently being computed.
- `UNBOUNDED PRECEDING` indicates the upper bound of this window is unlimited.
- `expr PRECEDING`
  - When the window is `ROWS` type, `expr` must be a positive integer, which indicates the upper boundary is the `expr`th row before current row.
  - When the window type is `ROWS_RANGE`,`expr` should be a time interval, like `10s`, `10m`,`10h`, `10d`. The upper bound is the `expr` ahead of the time of current row.
- By default, OpenMLDB uses closed interval. To change this, you can use keyword `OPEN`.

```{Note}
Standard SQL also supports `FOLLOWING` boundary, but OpenMLDB doesn't support it currently.
````

#### Example
**1. Named Window**

```SQL
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
```

**2. Anonymous Window**

```SQL
SELECT id, pk1, col1, std_ts,
sum(col1) OVER (PARTITION BY pk1 ORDER BY std_ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as w1_col1_sum
from t1;
```

**3. ROWS Window**

The following `WINDOW` clause defines a `ROWS` window containing preceding 1000 rows and current row. The window will contain a maximum of 1001 rows.
```SQL
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW);
```


**4. ROWS RANGE Window**

The following `WINDOW` clause defines a `ROWS_RANGE` window containing preceding 10s rows and current row.
```SQL
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW);
```

## WindowSpec Elements specifically designed by OpenMLDB

### **Window With Union**

```sql
WindowUnionClause
				 :: = ( 'UNION' TableRefs)
```

#### Example
**1. Window With `UNION` On 2 Tables**

```SQL
SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (UNION t2 PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW);
```

![Figure 2: window union one table](../dql/images/window_union_1_table.png)

**2. Window With `UNION` on Multiple Tables**

```SQL
SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (UNION t2, t3 PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW);
```

![Figure 3: window union two tables](../dql/images/window_union_2_table.png)

**3. Window With `UNION` but Exclude Instance Table**

```SQL
SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (UNION t2 PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW INSTANCE_NOT_IN_WINDOW);
```

![Figure 4: window union one table with instance_not_in_window](../dql/images/window_union_1_table_instance_not_in_window.png)


**4. Window With `UNION` Composed Of Subquery**

```SQL
SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS
(UNION (select c1 as col1, c2 as col2, 0.0 as col3, 0.0 as col4, c5 as col5, "NA" as col6 from t2),
(select c1 as col1, c2 as col2, 0.0 as col3, 0.0 as col4, c5 as col5, "NA" as col6 from t3)
PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW);
```

### **Window Exclude Current Time**

```
WindowExcludeCurrentTime 
				::= 'EXCLUDE' 'CURRENT_TIME'  
```

#### Example
**1. ROWS WINDOW with EXCLUDE CURRENT TIME**

The following `WINDOW` clause defines a `ROWS` window containing preceding 1000 rows and current row. Any other rows in the window will not have the same time as the `CURRENT ROW`.
```SQL
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW EXCLUDE CURRENT_TIME);
```

**2.ROW RANGE WINDOW with EXCLUDE CURRENT TIME**

The following `WINDOW` clause defines a `ROWS_RANGE` window containing preceding 10s rows and current row. Any other rows in the window will not have the same time as the `CURRENT ROW`.
```SQL
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW EXCLUDE CURRENT_TIME);
```

![Figure 5: window exclude current time](../dql/images/window_exclude_current_time.png)

### Window Frame Max Size

The keyword `MAXSIZE` is used to limit the number of rows in the window.
```sql
WindowFrameMaxSize
				:: = MAXSIZE NumLiteral
```

![Figure 6: window config max size](../dql/images/window_max_size.png)

####Example
**1. ROWS RANGE WINDOW with MAXSIZE**

The following `WINDOW` clause defines a `ROWS_RANGE` window containing preceding 10s rows and current row. There are at most 3 rows in the window.
```sql
-- desc: window ROWS_RANGE, 前10s到当前条，同时限制窗口条数不超过3条
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW MAXSIZE 3);
```

```{seealso}
Aggregate functions that can be used in window computation, refer to [Built-in Functions](../functions_and_operators/Files/udfs_8h.md)
````
