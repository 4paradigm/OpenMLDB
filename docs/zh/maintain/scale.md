# 扩缩容

## 扩容

随着业务的发展, 当前集群的拓扑不能满足要求就需要动态的扩容。扩容就是把一部分分片从现有tablet节点迁移到新的tablet节点上从而减小内存占用

### 1 启动一个新的tablet节点
按照如下步骤启动tablet节点，参考[部署文档](../deploy/install_deploy.md)
- 检查时间和时区，关闭THP和swap
- 下载部署包到新的节点并解压
- 修改conf/tablet.flags配置文件，zk_cluster和zk_root_path和集群中其他节点保持一致。修改endpoint。
- 启动tablet
  ```bash
    bash bin/start.sh start tablet
  ```
启动后查看新增节点是否加入集群。如果执行showtablet命令列出了新节点endpoint说明已经加入到集群中

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> showtablet
  endpoint            state           age
-------------------------------------------
  172.27.128.31:8541  kTabletHealthy  15d
  172.27.128.32:8541  kTabletHealthy  15d
  172.27.128.33:8541  kTabletHealthy  15d
  172.27.128.37:8541  kTabletHealthy  1min
```

### 2 迁移副本

副本迁移用到的命令是migrate。命令格式: migrate src\_endpoint table\_name partition des\_endpoint  

**一旦表创建好了，不能新增和减少分片，只能迁移分片。迁移的时候只能迁移从分片, 不能迁移主分片**

```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> use demo_db
> showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.33:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       yes       kNoCompress    0        0           0.000
> migrate 172.27.128.33:8541 flow 0 172.27.128.37:8541
> showopstatus flow
  op_id  op_type     name  pid  status  start_time      execute_time  end_time        cur_task
------------------------------------------------------------------------------------------------
  51     kMigrateOP  flow  0    kDone   20180824163316  12s           20180824163328  -
> showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.37:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       yes       kNoCompress    0        0           0.000
```
**说明**: 迁移副本还可以用删除副本然后新增副本的方式来操作。

## 缩容

缩容就是减小现有集群的节点数。

### 1 选定需要下线的节点
### 2 将需要下线节点上的分片迁移到其他节点上
* 执行showtable命令查看表分片分布
* 执行migrage命令将副本迁移到其他节点。如果要下线的节点上有leader，需要执行changeleader命令将leader切换到其他节点上才能执行migrate
### 3 下线节点
执行停止命令
```bash
bash bin/start.sh stop tablet
```
如果该节点部署有nameserver也需要把nameserver停掉
```bash
bash bin/start.sh stop nameserver
```
**注**：保持高可用至少需要两个nameserver节点
