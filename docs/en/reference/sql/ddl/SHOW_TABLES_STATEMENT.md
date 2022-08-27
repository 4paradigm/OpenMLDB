# SHOW TABLES

```sql
SHOW TABLES;
```

The `SHOW TABLES` statement is used to display the tables that the user has access to in the current database.

## Example

```sql
CREATE DATABASE db1;
--SUCCEED

USE db1;
--SUCCEED: Database changed

CREATE TABLE t1(col0 STRING);
-- SUCCEED

CREATE TABLE t2(col0 STRING);
-- SUCCEED

SHOW TABLES;
 -------- 
  Tables  
 -------- 
  t1      
  t2      
 -------- 
2 rows in set
```

## Relevant SQL Statements

[CREATE DATABASE](../ddl/CREATE_DATABASE_STATEMENT.md)

[CREATE TABLE](../ddl/CREATE_TABLE_STATEMENT.md)

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

