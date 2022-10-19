# SHOW COMPONENTS
显示当前 OpenMLDB 系统的各个组件信息。

```sql
SHOW COMPONENTS;
```

## 输出信息说明

| Column       | Note                                                                    |
| ------------ |-------------------------------------------------------------------------|
| Endpoint     | 组件端点，同 `--endpoint` flag                                                |
| Role         | 组件角色，有 `tablet`、`nameserver`、`taskmanager`、`apiserver`， 同 `--role` flag |
| Connect_time | 组件连接时间，以毫秒时间戳形式展示                                                       |
| Status       | 组件状态， `online`、 `offline`或`NULL`                                        |
| Ns_role      | Namserver 的角色，`master`或 `standby`                                       |


```{note}
`SHOW COMPONETS` 目前仍有部分未完善的功能：
- 不能展示 api server 信息
- 只能展示单个 task manager master 的信息，不能展示其他 slave 节点
- 单机版 nameserver 的 connect time 不准确
```

## Example

```sql
SHOW COMPONENTS;
 ---------------- ------------ --------------- -------- --------- 
  Endpoint         Role         Connect_time    Status   Ns_role  
 ---------------- ------------ --------------- -------- --------- 
  127.0.0.1:9520   tablet       1654759517890   online   NULL     
  127.0.0.1:9521   tablet       1654759517942   online   NULL     
  127.0.0.1:9522   tablet       1654759517919   online   NULL     
  127.0.0.1:9622   nameserver   1654759519015   online   master   
  127.0.0.1:9623   nameserver   1654759521016   online   standby  
  127.0.0.1:9624   nameserver   1654759523030   online   standby  
 ---------------- ------------ --------------- -------- --------- 
```

