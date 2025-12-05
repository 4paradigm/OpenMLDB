# DROP TABLE

```
DROP TABLE [IF EXISTS] table_name
```

The `DROP TABLE` statement is used to drop a specified table.

## Example: Delete a Table in the Current Database

Create database `db1` and set it as the current database:

```sql
CREATE DATABASE db1;
-- SUCCEED

USE db1;
-- SUCCEED: Database changed
```

Create two tables `t1` and `t2` in the database:

```sql
CREATE TABLE t1(col0 STRING);
-- SUCCEED

CREATE TABLE t2(col0 STRING);
-- SUCCEED
```

View the tables of current database:

```sql
SHOW TABLES;
 -------- 
  Tables  
 -------- 
  t1      
  t2      
 -------- 
2 rows in set

```

Delete table t1:

```sql
DROP TABLE t1;
-- Drop table t1? yes/no
-- yes
-- SUCCEED
```

Look at the tables of `db1` again:

```sql
SHOW TABLES;
 -------- 
  Tables  
 --------      
  t2      
 -------- 
1 rows in set

```

Use the `IF EXISTS` option to delete a table that does not exist
```sql
DROP TABLE IF EXISTS t3;
-- Drop table t3? yes/no
-- yes
-- SUCCEED
```

## Related SQL Statements

[CREATE TABLE](../ddl/CREATE_TABLE_STATEMENT.md)

[SHOW TABLES](../ddl/SHOW_TABLES_STATEMENT.md)