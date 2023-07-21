# 在离线数据同步

在离线数据同步，指将在线数据同步到离线地址，离线地址指大容量持久化存储地址，用户可以自行指定，不一定是OpenMLDB表中的离线数据地址。当前仅支持**磁盘表**同步，仅支持写入到**hdfs集群**。

开启在离线同步功能，需要部署两种组件，DataCollector和SyncTool。一期仅支持单个SyncTool，DataCollector需要在**每台部署TabletServer的机器**上**至少**部署一台。举例说明，一台机器上可以存在多个TabletServer，同步任务将使用该机器上的一个DataCollector，如果你添加了更多的DataCollector，它们不会工作，直到运行的DataCollector下线，将由下一个DataCollector代替以继续工作。

虽然SyncTool仅支持单体运行，但它再启动时可恢复工作进度，无需额外操作。

## 部署方式

由于SyncTool有状态，如果先启动它，可能会在无DataCollector的情况下尝试分配同步任务。所以，请保证先启动所有DataCollector，再启动SyncTool。

在离线同步要求集群的TabletServer版本>0.7.3，如果你的集群版本低于0.7.3，请先升级。DataCollector在部署包中的bin目录中，SyncTool在根目录`synctool`中，两者都可通过`bin/start.sh`启动。

### DataCollector

#### 配置(重点)

更新`<package>/conf/data_collector.flags`配置文件。配置中请填写正确的zk地址和路径，以及配置无端口冲突`endpoint`（endpoint与TabletServer保持一致，如果TabletServer使用本机的公网IP，DataCollector endpoint使用127.0.0.1地址，无法自动转换）。

需要注意的是，请慎重选择`collector_datadir`。我们将在同步中对TabletServer的磁盘数据进行硬链接，所以`collector_datadir`需要与TabletServer的数据地址`hdd_root_path`/`ssd_root_path`在同一磁盘上，否则报错`Invalid cross-device link`。

`max_pack_size`默认1M，如果同步任务过多，容易出现`[E1011]The server is overcrowded`，请再适当调小此配置。也可适当调整`socket_max_unwritten_bytes`，增大写缓存容忍度。

#### 启动

```
./bin/start.sh start data_collector
```
#### 状态确认

启动后使用以下命令，可以获得实时的DataCollector RPC状态页面。如果失败，查询日志。
```
curl http://<data_collector>/status
```
当前我们无法查询 `DataCollector ` 列表，将来会在相关工具中提供支持。

### SyncTool

#### 配置
- 请更新`<package>/conf/synctool.properties`配置，在start时它将覆盖`<package>/synctool/conf/synctool.properties`。
- 当前只支持直写到HDFS，可通过properties文件配置`hadoop.conf.dir`或环境变量`HADOOP_CONF_DIR`来配置HDFS连接，请保证SyncTool的OS启动用户拥有HDFS路径(路径由每个同步任务创建时指定)的写权限。

#### 启动
```
./bin/start.sh start synctool
```

SyncTool目前只支持单进程运行，如果启动多个，它们互相独立，需要用户自行判断是否有重复的同步任务。SyncTool实时保存进度，如果下线，可原地启动恢复任务进度。

SyncTool负责同步任务的管理和数据收集、写入离线地址。首先，说明一下任务关系，SyncTool收到用户的“表同步任务”，将会被分割为多个“分片同步任务”（后续简称为子任务）进行创建和管理。

任务管理中，如果DataCollector掉线或出错，将会让DataCollector重新创建任务。如果重新赋予任务时，找不到合适的DataCollector，将会标记任务失败。如果不这样，SyncTool将会一直尝试赋予新任务，同步任务进度停滞，错误也不明显，所以为了及时发现问题，这种情况将标记子任务为失败。

由于创建表同步任务时不支持从某个点开始，所以当前情况下，不适合删除任务再创建，如果目的地一样，会有较多重复数据。可以考虑更换同步目的地，或者重启SyncTool（SyncTool recover时不会检查子任务是否曾经failed，会当作init状态开始任务）。

#### SyncTool Helper

创建、删除与查询同步任务，使用`<package>/tools/synctool_helper.py`。

```bash
# create 
python tools/synctool_helper.py create -t db.table -m 1 -ts 233 -d /tmp/hdfs-dest [ -s <sync tool endpoint> ] 
# delete
python tools/synctool_helper.py delete -t db.table [ -s <sync tool endpoint> ] 
# task status
python tools/synctool_helper.py status [ -s <sync tool endpoint> ] 
# sync tool status for dev
python tools/synctool_helper.py tool-status [ -f <properties path> ]
```

Mode配置填写0/1/2，分别对应全量同步FULL，按时间增量同步INCREMENTAL_BY_TIMESTAMP，完全增量同步FULL_AND_CONTINUOUS三种模式。如果是Mode 1，使用`-ts`配置起始时间，小于该时间的数据将不会被同步。

Mode 0 没有严格的终止时间点，当每个子任务同步完当前的数据后，就会结束，结束会删除整个表任务，如果使用helper查询status没有该表的同步任务，则视为该表同步任务完成。Mode 1/2 都不会停止，将永远运行。

status结果说明：

执行命令status将会打印每个partition task的状态，如果你只关注整体情况，可以只查看`table scope`之后的内容，它展示了表级别的同步任务状态，如果有某表存在`FAILED`子任务，也会提示。

对于每个子任务而言，注意其'status'字段，如果它是刚启动，还未收到DataCollector的第一次数据同步，将会是INIT状态。收到第一次数据同步后，将变为RUNNING状态。（我们尤其关注DataCollector和SyncTool的初始状态，所以，特别设置INIT状态。）如果同步任务是随着SyncTool重启而恢复，将直接进入RUNNING状态。任务在过程中可能出现REASSIGNING状态，这是中间状态，不代表任务已经不可用。在Mode 0，可能出现SUCCESS状态，表示任务已完成。当一张表的所有子任务都完成时，SyncTool将自动清理掉该表的任务，使用helper将查询不到该表的任务。

只有FAILED表示该子任务失败，不会重试，也不会删除该任务。确认失败原因且修复后，可以删除再重建同步任务。如果不想要丢失已导入的进度，可以重启SyncTool，让SyncTool恢复任务继续同步（但可能出现更多的重复数据）。

## 功能边界

DataCollector中对表的进度（snapshot进度）标记方式没有唯一性，如果子task中途shutdown，用当前进度创建任务，可能有一段重复数据。
SyncTool HDFS先写入再持久化进度，如果此时SyncTool shutdown，由于进度没更新，将会重复写入一段数据。由于此功能必然有重复隐患，此处暂时不做额外工作。
