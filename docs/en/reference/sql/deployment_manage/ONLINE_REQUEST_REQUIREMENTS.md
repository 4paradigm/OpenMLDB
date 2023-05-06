# Online Specifications and Requirements for SQL

OpenMLDB can provide real-time feature extraction services under *online request* mode. The [DEPLOY](../deployment_manage/DEPLOY_STATEMENT.md) command can deploy a SQL script feature extraction on the requested sample online. If the deployment is successful, users can perform real-time feature extraction through the Restful API or JDBC API. Note that only some SQL commands can be deployed to provide services online. To deploy these SQL commands please follow the specifications below.

## Supported Statements under Online Request Mode 

Online request mode only supports [SELECT query statement](../dql/SELECT_STATEMENT.md).

## Supported `SELECT` Clause by Online Request Mode 

It is worth noting that not all SELECT query statements can be deployed online, see [SELECT Statement](../dql/SELECT_STATEMENT.md#select-statement) for detail. 

The following table shows the `SELECT` clause supported under online request mode.

| SELECT Clause                                   | Note                                                                                                                                                                                                                                                                                                                                                              |
|:------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Simple calculation on single table | The so-called simple single-table query is to process the column of a table, or use operation expressions, single-row processing function (Scalar Function) and their combined expressions on the table. You need to follow the [specifications of Single-table query under Online Request mode](#specifications-of-single-table-query-under-online-request-mode) |
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)          | OpenMLDB currently only supports **LAST JOIN**. For Online Request mode, please follow [the specifications of LAST JOIN under Online Request mode](#specifications-of-last-join-under-online-request-mode)                                                                                                                                                        |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md)      | The window clause is used to define one or several windows. Windows can be named or anonymous. Aggregate functions can be called on the window to perform some analytical computations. For Online Request mode, please follow the [specifications of WINDOW under Online Request mode](#specifications-of-window-under-online-request-mode)                           |
| [`LIMIT` Clause](../dql/LIMIT_CLAUSE.md)   | The LIMIT clause is used to limit the number of results. OpenMLDB currently only supports one parameter to limit the maximum number of rows of returned data.                                                                                                                                                                                                                                                                                                                  |

## Specifications of `SELECT` Clause Supported by Online Request Mode

### Specifications of Single-table Query under Online Request Mode

- Only column computations, expressions, and single-row processing functions (Scalar Function) and their combined expressions are supported. 
- Single table query does not contain [GROUP BY clause](../dql/JOIN_CLAUSE.md), [WHERE clause](../dql/WHERE_CLAUSE.md), [HAVING clause](../dql/HAVING_CLAUSE.md) and [WINDOW clause](../dql/WINDOW_CLAUSE.md).
- Single table query only involves the computation of a single table, and does not include the computation of [joined](../dql/JOIN_CLAUSE.md) multiple tables.

**Example**

```sql
-- desc: SELECT all columns
SELECT * FROM t1;
  
-- desc: rename expression 1
SELECT COL1 as c1 FROM t1;
 
-- desc: rename expression 2
SELECT COL1 c1 FROM t1;

-- desc: SELECT on column expression
SELECT COL1 FROM t1;
SELECT t1.COL1 FROM t1;
 
-- desc: unary expression
SELECT -COL2 as COL2_NEG FROM t1;
  
-- desc: binary expression
SELECT COL1 + COL2 as COL12_ADD FROM t1;
 
-- desc: type cast
SELECT CAST(COL1 as BIGINT) as COL_BIGINT FROM t1;
  
-- desc: function expression
SELECT substr(COL7, 3, 6) FROM t1;
```

### Specifications of LAST JOIN under Online Request Mode

- Only `LAST JOIN` is supported.
- At least one JOIN condition is an EQUAL condition like `left_table.column=right_table.column`, and the `rgith_table.column` needs to be indexed as a `KEY` of the right table.
- In the case of LAST JOIN with sorting, `ORDER BY` only supports column expressions, all columns of type int16, int32, int64, timestamp, and the column needs to be indexed as a timestamp (TS) of the right table.
- Right TableRef
  - refer to a physical table name or subquery
  - for subquery, limits to
    - Simple Projection (`select * from tb` or `select id, val from tb`)
    - Window subquery, e.g `select id, count(val) over w as cnt from t1 window w as (...)`.  Here the left TableRef of last join and window subquery must have the same request table, request table is the left most physical table of SQL syntax tree
    - **Since OpenMLDB 0.8.0** WHERE claused simple projection ( 例如 `select * from tb where id > 10`)

**Example**

```sql
CREATE DATABASE db1;
-- SUCCEED
    
USE db1;
-- SUCCEED: Database changed
    
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
-- SUCCEED

CREATE TABLE t2 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
-- SUCCEED

desc t1;
 --- ---------- ----------- ------ --------- 
  #   Field      Type        Null   Default  
 --- ---------- ----------- ------ --------- 
  1   col0       Varchar     YES             
  2   col1       Int         YES             
  3   std_time   Timestamp   YES             
 --- ---------- ----------- ------ --------- 
 --- -------------------- ------ ---------- ---------- --------------- 
  #   name                 keys   ts         ttl        ttl_type       
 --- -------------------- ------ ---------- ---------- --------------- 
  1   INDEX_0_1639524729   col1   std_time   43200min   kAbsoluteTime  
 --- -------------------- ------ ---------- ---------- --------------- 
```
### Specifications of WINDOW under Online Request Mode

- Window boundary: only `PRECEDING` and `CURRENT ROW` are supported.
- Window type: only `ROWS` and `ROWS_RANGE` are supported.
- `PARTITION BY` only supports column expressions, and the column needs to be indexed as a `KEY`.
- `ORDER BY` only support column expressions, and the column needs to be indexed as a timestamp (`TS`).
- Other supported keywords: `EXCLUDE CURRENT_ROW`, `EXCLUDE CURRENT_TIME`, `MAXSIZE` and `INSTANCE_NOT_IN_WINDOW`. See [WindowSpec elements specifically designed by OpenMLDB](../dql/WINDOW_CLAUSE.md#windowspec-elements-specifically-designed-by-openmldb) for detail.

