# SHOW TABLES

```sql
SHOW TABLES;
```

`SHOW TABLES`语句用于显示当前数据库下用户有权访问的表。

## Example

```sql
CREATE DATABASE db1;
--SUCCEED: Create database successfully

USE db1;
--SUCCEED: Database changed

CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully

CREATE TABLE t2(col0 STRING);
-- SUCCEED: Create successfully

SHOW TABLES;
 -------- 
  Tables  
 -------- 
  t1      
  t2      
 -------- 
2 rows in set
```

## 相关SQL语句

[CREATE DATABASE](../ddl/CREATE_DATABASE_STATEMENT.md)

[CREATE TABLE](../ddl/CREATE_TABLE_STATEMENT.md)

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

