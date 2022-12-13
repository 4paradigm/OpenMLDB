# DESC 

## Syntax

```sql
DescStmt ::=
    'DESC' TableName

TableName ::=
    Identifier ('.' Identifier)?
```

`DESC` 语句可为用户展示表的详细信息。

## SQL语句模版

```sql
DESC table_name;
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
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
--SUCCEED

desc t1;
 --- ---------- ----------- ------ ---------
  #   Field      Type        Null   Default
 --- ---------- ----------- ------ ---------
  1   col0       Varchar     YES
  2   col1       Int         YES
  3   std_time   Timestamp   YES
 --- ---------- ----------- ------ ---------
 --- -------------------- ------ ---------- ---------- ---------------
  #   name                 keys   ts         ttl        ttl_type
 --- -------------------- ------ ---------- ---------- ---------------
  1   INDEX_0_1658136511   col1   std_time   43200min   kAbsoluteTime
 --- -------------------- ------ ---------- ---------- ---------------
 --------------
  storage_mode
 --------------
  Memory
 --------------

```



## 相关语句

[DROP DATABASE](./DROP_DATABASE_STATEMENT.md)

[SHOW DATABASES](./SHOW_DATABASES_STATEMENT.md)

[SHOW TABLES](./SHOW_TABLES_STATEMENT.md)

