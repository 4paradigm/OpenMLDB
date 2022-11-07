# 备份与恢复

## 高可用说明

* nameserver不能全挂。如果全挂了不能做故障转移和恢复
* 单副本情况下不保证高可用
* 两副本两节点情况下，tablet最多只能挂一个节点
* 两副本多节点情况下，同一分片所在的两个tablet节点不能同时挂
* 三副本三节点情况下，挂一个tablet节点能自动做故障转移和数据恢复。挂两个tablet节点不保证自动故障转移和数据恢复，但是读写在分钟级别内可以恢复正常
* 三副本多节点情况下，同一分片所在的两个tablet挂掉的话不保证该分片自动故障自动转移和数据恢复，但是读写在分钟级别内可以恢复正常
* 如果挂的节点里边有leader分片并且一直有写流量可能会丢少量数据

## 宕机与恢复

OpenMLDB高可用可配置为自动模式和手动模式. 在自动模式下如果节点宕机和恢复时系统会自动做故障转移和数据恢复, 否则需要用手动处理  

通过修改auto\_failover配置可以切换模式, 默认是开启自动模式。通过以下方式可以获取配置状态和修改配置

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> confget
  key                 value
-----------------------------
  auto_failover       true
> confset auto_failover false
set auto_failover ok
>confget
  key                 value
-----------------------------
  auto_failover       false
```

**auto_failover开启的话如果一个节点下线了, showtable的is\_alive状态就会变成no，如果节点包含某个分片的leader，分片内会重新选主**

```
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  leader    0min       no        kNoCompress    0        0           0.000
  flow    4   0    172.27.128.33:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       no        kNoCompress    0        0           0.000
```

auto_failover关闭时，节点下线和恢复时需要手动操作下。有两个命令offlineendpoint和recoverendpoint

如果节点发生故障，需要执行offlineendpoint下线节点

命令格式: offlineendpoint endpoint

endpoint是发生故障节点的endpoint。该命令会下线节点，对该节点下所有分片执行如下操作:

* 如果是主, 执行重新选主
* 如果是从, 找到主节点然后从主节点中删除当前endpoint副本

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showtablet
  endpoint            state           age
-------------------------------------------
  172.27.128.31:8541  kTabletHealthy  3h
  172.27.128.32:8541  kTabletOffline  7m
  172.27.128.33:8541  kTabletHealthy  3h
> offlineendpoint 172.27.128.32:8541
offline endpoint ok
>showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  leader    0min       no        kNoCompress    0        0           0.000
  flow    4   0    172.27.128.33:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       no        kNoCompress    0        0           0.000
```

执行完offlineendpoint后每一个分片都会分配新的leader。如果某个分片执行失败可以单独对这个分片执行changleader，命令格式为：changeleader table\_name pid

如果节点已经恢复，就可以执行recoverendpoint来恢复数据

命令格式: recoverendpoint endpoint

endpoint是状态已经变为healthy节点的endpoint

```
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showtablet
  endpoint            state           age
-------------------------------------------
  172.27.128.31:8541  kTabletHealthy  3h
  172.27.128.32:8541  kTabletHealthy  7m
  172.27.128.33:8541  kTabletHealthy  3h
> recoverendpoint 172.27.128.32:8541
recover endpoint ok
> showopstatus
  op_id  op_type              name  pid  status  start_time      execute_time  end_time        cur_task
-----------------------------------------------------------------------------------------------------------
  54     kUpdateTableAliveOP  flow  0    kDone   20180824195838  2s            20180824195840  -
  55     kChangeLeaderOP      flow  0    kDone   20180824200135  0s            20180824200135  -
  56     kOfflineReplicaOP    flow  1    kDone   20180824200135  1s            20180824200136  -
  57     kUpdateTableAliveOP  flow  0    kDone   20180824200212  0s            20180824200212  -
  58     kRecoverTableOP      flow  0    kDone   20180824200623  1s            20180824200624  -
  59     kRecoverTableOP      flow  1    kDone   20180824200623  1s            20180824200624  -
  60     kReAddReplicaOP      flow  0    kDoing  20180824200624  4s            -               kLoadTable
  61     kReAddReplicaOP      flow  1    kDoing  20180824200624  4s            -               kLoadTable
```

执行showopstatus查看任务运行进度，如果status是doing状态说明任务还没有运行完

```
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showopstatus
  op_id  op_type              name  pid  status  start_time      execute_time  end_time        cur_task
---------------------------------------------------------------------------------------------------------
  54     kUpdateTableAliveOP  flow  0    kDone   20180824195838  2s            20180824195840  -
  55     kChangeLeaderOP      flow  0    kDone   20180824200135  0s            20180824200135  -
  56     kOfflineReplicaOP    flow  1    kDone   20180824200135  1s            20180824200136  -
  57     kUpdateTableAliveOP  flow  0    kDone   20180824200212  0s            20180824200212  -
  58     kRecoverTableOP      flow  0    kDone   20180824200623  1s            20180824200624  -
  59     kRecoverTableOP      flow  1    kDone   20180824200623  1s            20180824200624  -
  60     kReAddReplicaOP      flow  0    kDone   20180824200624  9s            20180824200633  -
  61     kReAddReplicaOP      flow  1    kDone   20180824200624  9s            20180824200633  -
> showtable
>showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.33:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       yes       kNoCompress    0        0           0.000
```

showtable如果都变成yes表示已经恢复成功。如果某些分片恢复失败可以单独执行recovertable，命令格式为：recovertable table\_name pid endpoint

**注：执行recoverendpoint前必须执行过一次offlineendpoint**

### 一键恢复
如果集群是同时下线后重启的，autofailover无法自动恢复数据，需要执行一键恢复脚本。参考[OpenMLDB运维工具](./openmldb_ops.md)

### 手动数据恢复

适用场景: 一张表分片所有副本所在的节点都挂了
#### 1 启动各节点的进程
#### 2 关闭autofailover
在ns client执行confset命令
```
confset auto_failover false
```
#### 3 恢复数据
手动恢复数据时需要一个分片一个分片的恢复，恢复步骤如下:
1. 用ns client执行showtable 表名
2. 修改表分片alive状态为no，用nsclient执行. updatetablealive table_name pid endppoint is_alive
   ```
   updatetablealive t1 * 172.27.2.52:9991 no
   ```
3. 针对表中的每个分片执行如下步骤: 
    * 恢复leader。如果有多个leader，选择offset较大的那个
        * 用tablet client连到leader所在节点，执行loadtable命令. loadtable tablename tid pid ttl seg_cnt, seg_cnt为8 
            ```
            loadtable test 1 1 144000 8
            ```
        * 查看load进度, 执行gettablestatus查看load进度。gettablestatus tid pid。等待stat由TableLoading状态变为TableNormal状态
            ```
            gettablestatus 1 1
            ```
        * 修改leader alive状态为yes. 用nsclient执行
            ```
            updatetablealive table_name pid 172.27.2.52:9991 yes
            ```
    * 用recovertable其他副本，用nsclient执行 recovertable table_name pid endpoint
        ```
        recovertable table1 1 172.27.128.31:9527 
        ```

#### 4 恢复autofailover
在ns client执行confset命令
```
confset auto_failover true
```