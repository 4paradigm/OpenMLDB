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

创建一张自定义索引的表:

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
 --------------- --------------
  compress_type   storage_mode
 --------------- --------------
  NoCompress      Memory
 --------------- --------------

```

有离线数据的表:

```sql
 --- ------- ----------- ------ ---------
  #   Field   Type        Null   Default
 --- ------- ----------- ------ ---------
  1   c1      Varchar     YES
  2   c2      Int         YES
  3   c3      BigInt      YES
  4   c4      Float       YES
  5   c5      Double      YES
  6   c6      Timestamp   YES
  7   c7      Date        YES
 --- ------- ----------- ------ ---------
 --- -------------------- ------ ---- ------ ---------------
  #   name                 keys   ts   ttl    ttl_type
 --- -------------------- ------ ---- ------ ---------------
  1   INDEX_0_1705743486   c1     -    0min   kAbsoluteTime
 --- -------------------- ------ ---- ------ ---------------
 ---------------------------------------------------------- ------------------------------------------ --------- ---------
  Data path                                                  Symbolic paths                             Format    Options
 ---------------------------------------------------------- ------------------------------------------ --------- ---------
  file:///tmp/openmldb_offline_storage/demo_db/demo_table1   file:///work/taxi-trip/data/data.parquet   parquet
 ---------------------------------------------------------- ------------------------------------------ --------- ---------

 --------------- --------------
  compress_type   storage_mode
 --------------- --------------
  NoCompress      Memory
 --------------- --------------
```


## 相关语句

[DROP DATABASE](./DROP_DATABASE_STATEMENT.md)

[SHOW DATABASES](./SHOW_DATABASES_STATEMENT.md)

[SHOW TABLES](./SHOW_TABLES_STATEMENT.md)

