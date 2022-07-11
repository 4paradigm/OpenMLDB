# DROP DATABASE

## Syntax

```
DROP DATABASE database_name
```

The `DROP DATABASE` statement is used to drop a database.

## **Example**

Create a database and set it as the current database:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

CREATE DATABASE db2;
-- SUCCEED: Create database successfully
```

Check out the database list:

```sql
SHOW DATABASES;
 ----------- 
  Databases  
 ----------- 
  db1        
  db2        
 ----------- 
```

drop database `db1`

```sql
DROP DATABASE db1;
```

Check out the database list again:

```sql
SHOW DATABASES;
 ----------- 
  Databases  
 -----------        
  db2        
 ----------- 
```

## Related Terms

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[CREATE DATABASE](./CREATE_DATABASE_STATEMENT.md)

[SHOW DATABASES](../ddl/SHOW_STATEMENT.md#show-databases)

