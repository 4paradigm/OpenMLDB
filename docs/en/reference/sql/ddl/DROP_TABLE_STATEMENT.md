# DROP TABLE

```
DROP TABLE table_name
```

The `DROP TABLE` statement is used to drop a specified table.

## Example: Delete a Table in the Current Database

Create a database and set it as the current database:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

USE db1;
-- SUCCEED: Database changed
```

Create two tables `t1` and `t2` in the database:

```sql
CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully

CREATE TABLE t2(col0 STRING);
-- SUCCEED: Create successfully
```

View the tables under the database:

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
-- SUCCEED: Drop successfully
```

Look at the tables under the database again:

```sql
SHOW TABLES;
 -------- 
  Tables  
 --------      
  t2      
 -------- 
1 rows in set

```



## Related SQL Statements

[CREATE TABLE](../ddl/CREATE_TABLE_STATEMENT.md)

[SHOW TABLES](../ddl/SHOW_TABLES_STATEMENT.md)