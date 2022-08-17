# DROP DATABASE

## Syntax

```
DROP DATABASE database_name
```

The `DROP DATABASE` statement is used to drop a database.

## **Example**

The following SQL commands create two databases and view all databases.

```sql
CREATE DATABASE db1;
-- SUCCEED

CREATE DATABASE db2;
-- SUCCEED

SHOW DATABASES;
 ----------- 
  Databases  
 ----------- 
  db1        
  db2        
 ----------- 
```
The following SQL command deletes the database `db1` and list the rest of the databases.

```sql
DROP DATABASE db1;

SHOW DATABASES;
 -----------
  Databases
 -----------
  db2
 -----------

1 rows in set
```

## Related Terms

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[CREATE DATABASE](./CREATE_DATABASE_STATEMENT.md)

[SHOW DATABASES](./SHOW_DATABASES_STATEMENT.md)


