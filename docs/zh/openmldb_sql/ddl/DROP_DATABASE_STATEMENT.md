# DROP DATABASE

## Syntax

```
DROP DATABASE database_name
```

 `DROP DATABASE` 语句用于删除一个数据库。

## **Example**

创建数据库db1和db2:

```sql
CREATE DATABASE db1;
-- SUCCEED

CREATE DATABASE db2;
-- SUCCEED
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

2 rows in set
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

1 rows in set
```

## 相关语句

[USE DATABASE](../ddl/USE_DATABASE_STATEMENT.md)

[CREATE DATABASE](./CREATE_DATABASE_STATEMENT.md)

[SHOW DATABASES](./SHOW_DATABASES_STATEMENT.md)

