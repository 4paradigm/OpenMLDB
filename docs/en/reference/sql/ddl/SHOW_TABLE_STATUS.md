# SHOW TABLE STATUS

`SHOW TABLE STATUS` is used to show information about tables in a given database or all databases, excluding hidden databases.
If no database is used, `SHOW TABLE STATUS` will display information about all tables in all databases, excluding hidden databases.
If a database is specified, the statement will only display information about the tables in the given database.

```sql
SHOW TABLE STATUS;
```


## Output Information

| Column            | Note                                                                                                                                   |
| ----------------- |----------------------------------------------------------------------------------------------------------------------------------------|
| Table_id          | It shows the unique id of the table.                                                                                                   |
| Table_name        | It shows the name of the table.                                                                                                        |
| Database_name     | It shows the name of the database, which the table belongs to.                                                                         |
| Storage_type      | It shows the storage type of the table. There are three types of value: `memory`,`ssd` and `hdd`.                                      |
| Rows              | It shows the number of rows in this table.                                                                                             |
| Memory_data_size  | It shows the memory usage of the table in bytes.                                                                                       |
| Disk_data_size    | It shows the disk usage of the table in bytes.                                                                                         |
| Partition         | It shows the number of partitons of the table.                                                                                         |
| Partition_unalive | It shows the number of the unalive partitions of the table.                                                                            |
| Replica           | It shows the number of replicas of the table.                                                                                              |
| Offline_path      | It shows the path of the offline data for this table and is valid only for offline tables. The `NULL` value means the path is not set. |
| Offline_format    | It shows the offline data format of the table and is valid only for offline tables. The `NULL` value means it is not set.              |
| Offline_deep_copy | It indicates whether deep copy is used on the table and is valid only for offline tables. The `NULL` value means it is not set.        |



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

