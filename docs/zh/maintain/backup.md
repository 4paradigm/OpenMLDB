# 高可用和恢复

## 最佳实践

对于生产环境，推荐以下高可用相关配置：

| 组件/参数   | 部署数量 | 说明                                                         |
| ----------- | ---- | ------------------------------------------------------------ |
| Nameserver  | = 2  | 部署时确定                                                   |
| TaskManager | = 2  | 部署时确定                                                   |
| Tablet      | >= 3 | 部署时确定，可扩缩容                                         |
| 物理节点    | >= 3 | 部署时确定，推荐和 tablet 数目一致，可扩缩容                 |
| 副本数目    | = 3  | 表粒度参数，创建表格命令（`CREATE TABLE`）时指定参数（如果 tablet 数目 >= 3，则副本数目默认为 3） |
| 守护进程    | 启用 | 部署时确定（见 [安装部署](../deploy/install_deploy.md) - 守护进程使用方式） |

最佳实践相关说明：

- Nameserver 和 TaskManager 在系统运行态只需要一个实例，并且意外下线的实例会被守护进程自动拉起，所以一般情况下部署两个实例已经足够
- Tablet 和副本数目一起决定了数据和服务的高可用性。一般情况下，推荐副本数目为 3 已经足够，增大副本数目可以进一步增大数据完整性的概率，但是由于存储资源需要相应增加，一般情况下并不推荐。Tablet 的数目和物理节点数目推荐保持一致，都至少为 3 个。
- OpenMLDB 的高可用机制是以数据分片为最小单位。在最佳实践配置下，每一个分片都有三个副本，且必定分布在三个不同的 tablets 上，其中有一个主分片（leader），两个为从分片（followers）。则当该分片对应的 tablets 离线时，不同的 tablets 离线数目，对该分片的高可用会造成不同的影响，如下表格总结：

| 该分片对应的 tablet 离线数目 | 服务可用性 | 该分片的数据完整性                                           | 离线 tablet 自动恢复（非机器重启） |
| ---------------------------- | ---------- | ------------------------------------------------------------ | ---------------------------------- |
| 1                            | ✓          | - 如果下线的是从分片，则数据保持完整性<br />- 如果下线的是主分片，并且有写流量，会造成少量数据丢失（部分写入数据可能未及时同步到从分片） | ✓                                  |
| 2                            | ✓          | - 如果下线的均为从分片，则数据保持完整性<br />- 如果下线中有一个主分片，并且有写流量，会造成少量数据丢失 | ✘                                  |
| 3                            | ✘          | ✘                                                            | ✘                                  |

- Tablet 离线可能会包含机器重启、网络断开、进程被杀掉几种不同情况。其中如果是机器重启，则需要手动执行 OpenMLDB 服务端启动脚本以后才能恢复；其他情况可以按照上表所述判断是否会自动恢复。
- **由于一个 tablet 一般会存放多个数据分片，而且必然会有主分片，因此如果从数据库整体来总结：（1）当一个 tablet 下线，服务不受影响，但如果有写流量，则有可能造成小部分数据丢失，离线的 tablet 可以自动恢复；（2）如果同一时间两个 tablet 下线，则服务不受影响，但如果有写流量，可能造成小部分数据丢失，并且离线的 tablet 可能无法自动恢复；（3）如果多于两个 tablet 下线，则服务和数据完整性均大概率无法保证。**
- 关于 tablet 、副本、分片的概念和架构设计，可参照文档 [在线模块架构](../reference/arch/online_arch.md) 中关于存储引擎的描述。

此外，如果用户对于高可用要求不高，并且机器资源紧张，可以考虑减少副本和 tablet 数目：

- 1 副本，1 个 tablets：无任何高可用机制
- 2 副本，2 个 tablets：（1）当一个 tablet 下线，可以保证服务不受影响，可能小部分数据丢失，离线的 tablet 可以自动恢复；（2）两个 tablet 同时下线，数据完整性和服务均无法保证。

## 节点恢复

OpenMLDB高可用可配置为自动模式和手动模式。在自动模式下如果节点宕机和恢复时系统会自动做故障转移和数据恢复, 否则需要用手动处理。通过修改 `auto_failover` 配置可以切换模式, 默认是开启自动模式。通过以下方式可以获取配置状态和修改配置：

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

**注意，auto_failover 开启的话如果一个节点下线了, showtable 的 is_alive 状态就会变成 no，如果节点包含某个分片的 leader，分片内会重新选主**

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

当 auto_failover 关闭时，节点下线和恢复时需要手动操作。有两个命令 `offlineendpoint` 和 `recoverendpoint` 可以完成该工作。

### 节点下线命令 `offlineendpoint`

如果节点发生故障，首先需要执行 `offlineendpoint` 下线节点，其命令格式为：

```
offlineendpoint $endpoint
```
`$endpoint` 是发生故障节点的endpoint。该命令会下线节点，对该节点下所有分片执行如下操作:

* 如果是主分片，执行重新选主分片
* 如果是从分片，找到主分片所在节点，然后从节点中删除当前 endpoint 副本

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

执行完 `offlineendpoint` 后每一个分片都会分配新的 leader。如果某个分片执行失败可以单独对这个分片执行 `changleader`，命令格式为：`changeleader $table_name $pid` （参照文档 [运维 CLI](cli.md#changeleader)）。

### 节点恢复命令 `recoverendpoint`

如果节点已经恢复，就可以执行 `recoverendpoint` 来恢复数据。命令格式: 

```
recoverendpoint $endpoint
```

`$endpoint` 是状态已经变为 `kTabletHealthy` 的节点的endpoint

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

执行 `showopstatus` 查看任务运行进度，如果 status 是 `kDoing` 状态说明任务还没有运行完

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

`showtable` 如果都变成 `yes` 表示已经恢复成功。如果某些分片恢复失败可以单独执行 `recovertable`，命令格式为：`recovertable $table_name $pid $endpoint`（参考文档 [运维 CLI](cli.md#recovertable)）。

**注：执行 `recoverendpoint` 前必须执行过一次 `offlineendpoint`  。**

## 一键数据恢复
如果集群是同时下线后重启的，autofailover 无法自动恢复数据，需要执行一键恢复脚本，执行自动化运维工具的命令 `recoverdata`。参考 [OpenMLDB运维工具](./openmldb_ops.md) 。使用样例如：

```bash
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=recoverdata
```

## 手动数据恢复

如果一键恢复失败，可以尝试以下的手动数据恢复命令。
### 1 启动各节点的进程
### 2 关闭 autofailover
在 ns client 执行 `confset`命令
```
confset auto_failover false
```
### 3 恢复数据
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

### 4 恢复 autofailover
在ns client执行 `confset` 命令
```
confset auto_failover true
```
