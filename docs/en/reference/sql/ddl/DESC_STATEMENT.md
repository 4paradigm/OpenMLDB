# DESC 

## Syntax

```sql
DescStmt ::=
    'DESC' TableName

TableName ::=
    Identifier ('.' Identifier)?
```

The `DESC` statement can display table details to the user.

## SQL Statement Template

```sql
DESC table_name;
```

## Example:

create a database`db1`:

```sql
CREATE DATABASE db1;
-- SUCCEED: Create database successfully

CREATE DATABASE db2;
-- SUCCEED: Create database successfully
```

Then select `db1` as the current database:

```sql
USE db1;
-- SUCCEED: Database changed
```

Create two tables:

```sql
CREATE TABLE t1 (col0 STRING, col1 int, std_time TIMESTAMP, INDEX(KEY=col1, TS=std_time, TTL_TYPE=absolute, TTL=30d));
--SUCCEED: Create successfully

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
  1   INDEX_0_1639524729   col1   std_time   43200min   kAbsoluteTime  
 --- -------------------- ------ ---------- ---------- --------------- 

```



## Related Terms 

[DROP DATABASE](./DROP_DATABASE_STATEMENT.md)

[SHOW DATABASES](./SHOW_STATEMENT.md#show-databases)

[SHOW TABLES](../ddl/SHOW_STATEMENT.md)

