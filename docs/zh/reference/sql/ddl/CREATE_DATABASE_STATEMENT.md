# CREATE DATABASE

## Syntax

```sql
CreateDatabaseStmt ::=
    'CREATE' 'DATABASE' DBName

DBName ::=
    Identifier
```

**Description**

`CREATE DATABASE` 语句用于在 OpenMLDB 上创建新数据库。数据库名必须是唯一的，重复创建同名数据库，会发生错误。

## **Example**

创建一个名字为`db1`的数据库。如果同名数据库已存在，则会抛出错误。

```sql
CREATE DATABASE db1;
-- SUCCEED
```

再创建一个名字为`db2`的数据库：

```sql
CREATE DATABASES db2;
-- SUCCEED
```

显示数据库列表:

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

重复创建同名数据库，会抛出错误：

```sql
CREATE DATABASE db1;
-- ERROR: Create database failed for database already exists
```



## 相关语句

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[DROP DATABASE](./DROP_DATABASE_STATEMENT.md)

[SHOW DATABASES](./SHOW_DATABASES_STATEMENT.md)
