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

*Window call function* implements functionality similar to aggregate functions. The difference is that the window call function does not need to pack the query results into a single line of output—in the query output, each line is separated. However, the window caller can scan all rows that may be part of the current row's group, depending on the grouping specification of the window caller (the `PARTITION BY` column). The syntax for calling a function from a window is one of the following:

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

| SELECT statement elements | state                   | illustrate                                                         |
| :------------- | ---------------------- | :----------------------------------------------------------- |
| WINDOW Clause  | Online Training not supported | The window clause is used to define one or several windows. Windows can be named or anonymous. Users can call aggregate functions on the window to perform some analytical calculations (```sql agg_func() over window_name```). <br />OpenMLDB currently only supports historical windows, not future windows (ie, does not support window boundaries of type `FOLLOWING`). <br />OpenMLDB windows only support `PARTITION BY` columns, not `PARTITION BY` operations or function expressions. <br />OpenMLDB windows only support `ORDER BY` columns, not `ORDER BY` operations or function expressions. <br />In Online Serving, you need to follow [3.2 Window usage specification under Online Serving](../deployment_manage/ONLINE_SERVING_REQUIREMENTS.md#online-serving window usage specification) |

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

 **WindowFrameExtent**定义了窗口的上界和下界。框架类型可以用 `ROWS`或`ROWS_RANGE`声明；

- CURRENT ROW: 表示当前行
- UNBOUNDED PRECEDING: 表示无限制上界
- `expr` PRECEDING
  - 窗口类型为ROWS时，`expr`必须为一个正整数。它表示边界为当前行往前`expr`行。
  - 窗口类型为ROWS_RANGE时,`expr`一般为时间区间（例如`10s`, `10m`,`10h`, `10d`)，它表示边界为当前行往前移expr时间段（例如，10秒，10分钟，10小时，10天）
- OpenMLDB支持默认边界是闭合的。但支持OPEN关键字来修饰边界开区间
- 请注意：标准SQL中，还支持FOLLOWING的边界，当OpenMLDB并不支持。

#### **Example: 有名窗口（Named Window）**

```SQL
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
```

#### **Example: 匿名窗口**

```SQL
SELECT id, pk1, col1, std_ts,
sum(col1) OVER (PARTITION BY pk1 ORDER BY std_ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as w1_col1_sum
from t1;
```

#### **Example: ROWS窗口**

```SQL
-- ROWS example
-- desc: window ROWS, 前1000条到当前条
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW);
```
#### **Example: ROWS RANGE窗口**

```SQL
-- ROWS example
-- desc: window ROWS_RANGE, 前10s到当前条
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW);
```

## OpenMLDB特有的WINDOW SPEC元素

### Window With Union

```sql
WindowUnionClause
				 :: = ( 'UNION' TableRefs)
```

#### **Example: Window with union 一张副表**

```SQL
SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (UNION t2 PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW);
```

![Figure 2: window union one table](../dql/images/window_union_1_table.png)

#### **Example: Window with union 多张副表**

```SQL
SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (UNION t2, t3 PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW);
```

![Figure 3: window union two tables](../dql/images/window_union_2_table.png)

#### **Example: Window with union 样本表不进入窗口**

```SQL
SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (UNION t2 PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW INSTANCE_NOT_IN_WINDOW);
```

![Figure 4: window union one table with instance_not_in_window](../dql/images/window_union_1_table_instance_not_in_window.png)

#### **Example: Window with union 列筛选子查询**

```SQL
SELECT col1, col5, sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS
(UNION (select c1 as col1, c2 as col2, 0.0 as col3, 0.0 as col4, c5 as col5, "NA" as col6 from t2),
(select c1 as col1, c2 as col2, 0.0 as col3, 0.0 as col4, c5 as col5, "NA" as col6 from t3)
PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW);
```

### Window Exclude Current Time

```
WindowExcludeCurrentTime 
				::= 'EXCLUDE' 'CURRENT_TIME'  
```

#### **Example: ROWS窗口EXCLUDE CURRENT TIME**

```SQL
-- ROWS example
-- desc: window ROWS, 前1000条到当前条, 除了current row以外窗口内不包含当前时刻的其他数据
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW EXCLUDE CURRENT_TIME);
```

#### **Example: ROW RANGE窗口EXCLUDE CURRENT TIME**

```SQL
-- ROWS example
-- desc: window ROWS, 前10s到当前条，除了current row以外窗口内不包含当前时刻的其他数据
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW EXCLUDE CURRENT_TIME);
```

![Figure 5: window exclude current time](../dql/images/window_exclude_current_time.png)

### Window Frame Max Size

OpenMLDB在定义了元素，来限定窗口内条数。具体来说，可以在窗口定义里使用**MAXSIZE**关键字，来限制window内允许的有效窗口内最大数据条数。

```sql
WindowFrameMaxSize
				:: = MAXSIZE NumLiteral
```

![Figure 6: window config max size](../dql/images/window_max_size.png)

#### **Example: ROW RANGE 窗口MAXSIZE**

```sql
-- ROWS example
-- desc: window ROWS_RANGE, 前10s到当前条，同时限制窗口条数不超过3条
SELECT sum(col2) OVER w1 as w1_col2_sum FROM t1
WINDOW w1 AS (PARTITION BY col1 ORDER BY col5 ROWS_RANGE BETWEEN 10s PRECEDING AND CURRENT ROW MAXSIZE 3);
```

```{seealso}
Aggregate functions that can be used in window calculation, refer to [Built-in Functions](../functions_and_operators/Files/udfs_8h.md)
````
