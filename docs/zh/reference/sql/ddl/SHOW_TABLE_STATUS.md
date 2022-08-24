# SHOW TABLE STATUS

```sql
SHOW TABLE STATUS
```

展示当前使用的数据库或者所有数据库下 tables 的详细信息。如果未使用任何 database, `SHOW TABLE STATUS`展示所有数据库里 tables 的信息，不包括隐藏数据库；如果使用了特定 database, 只展示当前数据库下 tables 的信息。



Column Information

| Column            | Description                                               |
| ----------------- |-----------------------------------------------------------|
| Table_id          | 表唯一 id                                                    |
| Table_name        | 表名                                                        |
| Database_name     | 数据库名                                                      |
| Storage_type      | 存储类型， `memory`,`ssd`,`hdd`                                |
| Rows              | 表的 rows count                                             |
| Memory_data_size  | 表内存占用（单位 bytes)                                           |
| Disk_data_size    | 表磁盘占用 （单位 bytes)                                          |
| Partition         | Partiton 数量                                               |
| Partition_unalive | Unalive partition 数量                                      |
| Replica           | Replica 数量                                                |
| Offline_path      | 表对应 offline 数据路径，仅对离线表生效。 `NULL` 表示未设置该项。                 |
| Offline_format    | 表对应 offline 数据格式，仅对离线表生效。 `NULL`  表示未设置该项。            |
| Offline_deep_copy | 表对应 offline 数据是否使用 deep copy，仅对离线表生效。 `NULL`  表示未设置该项。|



# Example

```sql
> USE db;
> SHOW TABLE STATUS;
 ---------- ------------ --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- -------------- ---------------- ------------------- 
  Table_id   Table_name   Database_name   Storage_type   Rows   Memory_data_size   Disk_data_size   Partition   Partition_unalive   Replica   Offline_path   Offline_format   Offline_deep_copy  
 ---------- ------------ --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- -------------- ---------------- ------------------- 
  6          t1           db              memory         2      479                0                8           0                   3         NULL           NULL             NULL               
 ---------- ------------ --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- -------------- ---------------- ------------------- 
```

