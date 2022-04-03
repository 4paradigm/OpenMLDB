# DROP DATABASE

## Syntax

```
DROP DATABASE database_name
```

 `DROP DATABASE` 语句用于删除一个数据库。

## **Example**

创建一个数据库，并设置为当前数据库:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

CREATE DATABASE db2;
-- SUCCEED: Create database successfully
```

查看数据库列表：

```sql
SHOW DATABASES;
 ----------- 
  Databases  
 ----------- 
  db1        
  db2        
 ----------- 
```

删除数据库`db1`

```sql
DROP DATABASE db1;
```

再次查看数据库列表：

```sql
SHOW DATABASES;
 ----------- 
  Databases  
 -----------        
  db2        
 ----------- 
```

## 相关语句

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[CREATE DATABASE](./CREATE_DATABASE_STATEMENT.md)

[SHOW DATABASES](../ddl/SHOW_STATEMENT.md#show-databases)

