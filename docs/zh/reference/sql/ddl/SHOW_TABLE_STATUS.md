# SHOW TABLE STATUS

`SHOW TABLE STATUS`命令可以展示当前使用的数据库或者所有数据库下表的详细信息。如果未使用任何数据库（即未执行`USE DATABASE`命令），`SHOW TABLE STATUS`命令将展示所有数据库里表的信息，不包括隐藏数据库；如果使用了特定数据库，将只展示当前数据库下表的信息。

```sql
SHOW TABLE STATUS;
```


## 输出信息

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




## Example


```sql
> USE db;
--SUCCEED: Database changed    
> SHOW TABLE STATUS;
 ---------- ------------ --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- -------------- ---------------- ------------------- 
  Table_id   Table_name   Database_name   Storage_type   Rows   Memory_data_size   Disk_data_size   Partition   Partition_unalive   Replica   Offline_path   Offline_format   Offline_deep_copy  
 ---------- ------------ --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- -------------- ---------------- ------------------- 
  6          t1           db              memory         2      479                0                8           0                   3         NULL           NULL             NULL               
 ---------- ------------ --------------- -------------- ------ ------------------ ---------------- ----------- ------------------- --------- -------------- ---------------- ------------------- 
```

