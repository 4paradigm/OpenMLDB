# DROP TABLE

```
DROP TABLE table_name
```

`DROP TABLE`语句用于删除指定的一张表。

## Example: 删除当前数据库中的一张表

创建一个数据库，并设置为当前数据库:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

USE db1;
-- SUCCEED: Database changed
```

在数据库中创建两张表`t1`和`t2`：

```sql
CREATE TABLE t1(col0 STRING);
-- SUCCEED: Create successfully

CREATE TABLE t2(col0 STRING);
-- SUCCEED: Create successfully
```

查看数据库下的表：

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

删除表t1:

```sql
DROP TABLE t1;
-- Drop table t1? yes/no
-- yes
-- SUCCEED: Drop successfully
```

再次查看数据库下的表：

```sql
SHOW TABLES;
 -------- 
  Tables  
 --------      
  t2      
 -------- 
1 rows in set

```



## 相关SQL语句

[CREATE TABLE](../ddl/CREATE_TABLE_STATEMENT.md)

[SHOW TABLES](../ddl/SHOW_TABLES_STATEMENT.md)