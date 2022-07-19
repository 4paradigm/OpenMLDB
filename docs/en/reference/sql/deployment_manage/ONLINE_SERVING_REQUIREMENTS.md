# SQL On-Line Specifications and Requirements

OpenMLDB Online Serving provides real-time feature extraction services. The [DEPLOY](../deployment_manage/DEPLOY_STATEMENT.md) command of OpenMLDB deploys a piece of SQL text to the wire. After the deployment is successful, users can perform feature extraction calculations on the request samples in real time through the Restful API or JDBC API. Not all SQL can be deployed to provide services online. OpenMLDB has a set of specifications for online statements and OP.

## Online Serving Statement

OpenMLDB only supports online [SELECT query statement](../dql/SELECT_STATEMENT.md).

## Online Serving Op List

It is worth noting that not all SELECT query statements can be online. In OpenMLDB, only `SELECT`, `WINDOW`, `LAST JOIN` OP can be online, other OP (including `WHERE`, `GROUP`, `HAVING`, `LIMIT`) are all unable to go online.

This section will list the OPs that support Online Serving, and elaborate on the online usage specifications of these OPs.

| SELECT Statement                                  | description                                                         |
| :----------------------------------------- | :----------------------------------------------------------- |
| Single sheet simple expression calculation                       | During Online Serving, **simple single-table query** is supported. The so-called simple single-table query is to calculate the column, operation expression, single-row processing function (Scalar Function) and their combined expressions of a table. You need to follow the [Usage Specifications for Online Serving Order Form Query] (#online-serving Order Form Query Usage Specification) |
| [`JOIN` Clause](../dql/JOIN_CLAUSE.md)     | OpenMLDB currently only supports **LAST JOIN**. In Online Serving, you need to follow [The usage specification of LAST JOIN under Online Serving] (#online-serving usage specification of last-join) |
| [`WINDOW` Clause](../dql/WINDOW_CLAUSE.md) | The window clause is used to define one or several windows. Windows can be named or anonymous. Users can call aggregate functions on the window to perform some analytical calculations (```sql agg_func() over window_name```). In Online Serving, you need to follow the [Usage Specifications of Window under Online Serving] (#the usage specification of window under online-serving) |

## OP's usage specification under Online Serving

### Online Serving Order Form Query Usage Specifications

- Only supports column, expression, and single-row processing functions (Scalar Function) and their combined expression operations
- Single table query does not contain [GROUP BY clause](../dql/JOIN_CLAUSE.md), [WHERE clause](../dql/WHERE_CLAUSE.md), [HAVING clause](../dql/ HAVING_CLAUSE.md) and [WINDOW clause](../dql/WINDOW_CLAUSE.md).
- Single table query only involves the calculation of a single table, and does not design the calculation of multiple tables [JOIN](../dql/JOIN_CLAUSE.md).

#### Example: Example of Simple SELECT Query Statement that Supports Online

```sql
-- desc: SELECT all columns
SELECT * FROM t1;
  
-- desc: SELECT expression renamed
SELECT COL1 as c1 FROM t1;
 
-- desc: SELECT expression rename 2
SELECT COL1 c1 FROM t1;

-- desc: SELECT column expression
SELECT COL1 FROM t1;
SELECT t1.COL1 FROM t1;
 
-- desc: SELECT unary expression
SELECT -COL2 as COL2_NEG FROM t1;
  
-- desc: SELECT binary expression
SELECT COL1 + COL2 as COL12_ADD FROM t1;
 
-- desc: SELECT type cast
SELECT CAST(COL1 as BIGINT) as COL_BIGINT FROM t1;
  
-- desc: SELECT function expression
SELECT substr(COL7, 3, 6) FROM t1;
```

### The Usage Specification of LAST JOIN Under Online Serving

- Join type only supports `LAST JOIN` type
- At least one JOIN condition is an EQUAL condition of the form `left_table.column=right_table.column`, and the `rgith_table.column` column needs to hit the index of the right table
- In the case of LAST JOIN with sorting, `ORDER BY` can only support column expressions, and the column needs to hit the time column of the right table index

#### Example: Example of Simple SELECT Query Statement that Supports Online



```sql
CREATE DATABASE db1;

USE db1;
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
-- SUCCEED: Create successfully

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
### Window Usage Specification Under Online Serving

- Window borders only support `PRECEDING` and `CURRENT ROW`
- Window types only support `ROWS` and `ROWS_RANGE`
- The window `PARTITION BY` can only support column expressions, and the column needs to hit the index
- The window `ORDER BY` can only support column expressions, and the column needs to hit the time column of the index

