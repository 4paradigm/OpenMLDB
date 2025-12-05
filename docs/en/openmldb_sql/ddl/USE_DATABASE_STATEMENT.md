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
-- SUCCEED

CREATE DATABASE db2;
-- SUCCEED
```

Then select `db1` as the current database:

```sql
USE db1;
-- SUCCEED: Database changed
```

Create two tables:

```sql
CREATE TABLE t1(col0 string);
-- SUCCEED

CREATE TABLE t2(col0 string);
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

Then select `db2` as the current database and view the tables in `db2`:

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

[SHOW DATABASES](./SHOW_DATABASES_STATEMENT.md)

[SHOW TABLES](./SHOW_TABLES_STATEMENT.md)