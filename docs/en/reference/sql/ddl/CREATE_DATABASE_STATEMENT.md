# CREATE DATABASE

## Syntax

```sql
CreateDatabaseStmt ::=
    'CREATE' 'DATABASE' DBName

DBName ::=
    Identifier
```

**Description**

The `CREATE DATABASE` statement is used to create a new database on OpenMLDB. The database name must be unique. If a database with the same name already exists, an error will occur.

## **Example**

The following SQl command creates a database named `db1`. If a database with the same name already exists, an error will be thrown.

```sql
CREATE DATABASE db1;
-- SUCCEED
```

Then create a database named `db2`:

```sql
CREATE DATABASES db2;
-- SUCCEED
```

Show database list:

```sql
SHOW DATABASES;
```

```
 ----------- 
  Databases  
 ----------- 
  db1        
  db2        
 ----------- 
2 rows in set
```

Repeatedly creating a database with the same name will throw an error:

```sql
CREATE DATABASE db1;
-- ERROR: Create database failed for database already exists
```



## Related Sentences

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[DROP DATABASE](./DROP_DATABASE_STATEMENT.md)

[SHOW DATABASES](./SHOW_DATABASES_STATEMENT.md)
