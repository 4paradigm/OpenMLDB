# SHOW CREATE TABLE

`SHOW CREATE TABLE` 用来显示指定表的建表语句

**Syntax**

```sql
SHOW CREATE TABLE table_name;
```

**Example**

```sql
show create table t1;
 ------- ---------------------------------------------------------------
  Table   Create Table
 ------- ---------------------------------------------------------------
  t1      CREATE TABLE `t1` (
          `c1` varchar,
          `c2` int,
          `c3` bigInt,
          `c4` timestamp,
          INDEX (KEY=`c1`, TS=`c4`, TTL_TYPE=ABSOLUTE, TTL=0m)
          ) OPTIONS (PARTITIONNUM=8, REPLICANUM=2, STORAGE_MODE='HDD');
 ------- ---------------------------------------------------------------

1 rows in set
```