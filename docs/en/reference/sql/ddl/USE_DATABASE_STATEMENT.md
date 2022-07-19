# USE DATABASE

## Syntax

```sql
UseDatabaseStmt ::=
    'USE' DBName

DBName ::=
    Identifier
```

The `USE` statement selects the current database for a user session.

## SQL Statement Template

```sql
USE database_name;
```

## Example:

Create a database `db1`:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

CREATE DATABASE db2;
-- SUCCEED: Create database successfully
```

Then select `db1` as the current database:

```sql
USE db1;
-- SUCCEED: Database changed
```

Create two tables:

```sql
CREATE TABLE t1(col0 string);
-- SUCCEED: Create successfully

CREATE TABLE t1(col0 string);
-- SUCCEED: Create successfully

SHOW TABLES;
 -------- 
  Tables  
 -------- 
  t1      
  t2      
 -------- 
```

Then select `db2` as the current database and view the tables under the current library:

```sql
USE db2;
-- SUCCEED: Database changed

SHOW TABLES;
 -------- 
  Tables  
 -------- 
0 row in set
```

## Related Statements

[DROP DATABASE](./DROP_DATABASE_STATEMENT.md)

[SHOW DATABASES](./SHOW_STATEMENT.md#show-databases)

[SHOW TABLES](./SHOW_STATEMENT.md#show-tables)