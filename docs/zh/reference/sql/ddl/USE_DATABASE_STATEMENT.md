# USE DATABASE

## Syntax

```sql
UseDatabaseStmt ::=
    'USE' DBName

DBName ::=
    Identifier
```

`USE` 语句可为用户会话选择当前数据库。

## SQL语句模版

```sql
USE database_name;
```

## Example:

创建一个数据库`db1`:

```sql
CREATE DATABASE db1;
-- SUCCEED

CREATE DATABASE db2;
-- SUCCEED
```

然后选择`db1`作为当前数据库：

```sql
USE db1;
-- SUCCEED: Database changed
```

创建两张表:

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

然后选择`db2`作为当前数据库，并查看当前库下的表：

```sql
USE db2;
-- SUCCEED: Database changed

SHOW TABLES;
 -------- 
  Tables  
 -------- 
0 row in set
```

## 相关语句

[DROP DATABASE](./DROP_DATABASE_STATEMENT.md)

[SHOW DATABASES](./SHOW_DATABASES_STATEMENT.md)

[SHOW TABLES](./SHOW_TABLES_STATEMENT.md)