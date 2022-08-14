# SHOW TABLE STATUS

`SHOW TABLE STATUS` is used to show the information about tables in current database or all databases, excluding hidden databases.
If no database is used, `SHOW TABLE STATUS` will display information about all tables in the database, excluding hidden databases.
If a specific database is used, the statement will only display information about the tables in current used database.

```sql
SHOW TABLE STATUS;
```


## Meaning of the Output

| Column            | Note                                                                                              |
| ----------------- |---------------------------------------------------------------------------------------------------|
| Table_id          | It shows the unique id of the table.                                                              |
| Table_name        | It shows the name of the table.                                                                   |
| Database_name     | It shows the name of the database, which the table belongs to.                                    |
| Storage_type      | It shows the storage type of the table. There are three types of value: `memory`,`ssd` and `hdd`. |
| Rows              | It shows the number of rows in this table.                                                        |
| Memory_data_size  | It shows the memory usage of the table in bytes.                                                  |
| Disk_data_size    | It shows the disk usage of the table in bytes.                                  |
| Partition         | Partiton 数量                                                                                       |
| Partition_unalive | Unalive partition 数量                                                                              |
| Replica           | Replica 数量                                                                                        |
| Offline_path      | 表对应 offline 数据路径, `NULL` if not exists                                                            |
| Offline_format    | 表对应 offline 数据格式, `NULL` if not exists                                                            |
| Offline_deep_copy | 表对应 offline 数据是否使用 deep copy, `NULL` if not exits                                                 |



## Example

```sql
> USE db;
> SHOW TABLE STATUS;
 ---------- ------------ --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- -------------- ---------------- ------------------- 
  Table_id   Table_name   Database_name   Storage_type   Rows   Memory_data_size   Disk_data_size   Partition   Partition_unalive   Replica   Offline_path   Offline_format   Offline_deep_copy  
 ---------- ------------ --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- -------------- ---------------- ------------------- 
  6          t1           db              memory         2      479                0                8           0                   3         NULL           NULL             NULL               
 ---------- ------------ --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- -------------- ---------------- ------------------- 
```

