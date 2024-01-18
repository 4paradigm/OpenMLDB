# 运维 CLI

* [ns client](#ns-client)
* [tablet client](#tablet-client)

## NS Client
连接到ns client 需要指定zk\_cluster、zk\_root\_path和role。其中zk\_cluster是zk地址，zk\_root\_path是集群在zk的根路径，role是启动的角色需指定为ns_client

```bash
$ ./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/onebox --role=ns_client
```
### use

use命令可以切换到某个database下
```
> use demodb
```
### showtable

showtable可以查看所有表，也可以指定看某一个表

命令格式：showtable \[table\_name\]

```
172.24.4.55:6531 demo_db> showtable
   name           tid  pid  endpoint          role    is_alive  offset  record_cnt  memused  diskused
------------------------------------------------------------------------------------------------------
  aaa            22   0    172.24.4.55:9971  leader  yes       0       0           0.000    8.646 K
  auto_VCeOIIKA  25   0    172.24.4.55:9971  leader  yes       8       4           498.000  9.128 K
  t1             21   0    172.24.4.55:9971  leader  yes       1       1           301.000  9.353 K
172.24.4.55:6531 demo_db> showtable auto_VCeOIIKA
   name           tid  pid  endpoint          role    is_alive  offset  record_cnt  memused  diskused
------------------------------------------------------------------------------------------------------
  auto_VCeOIIKA  25   0    172.24.4.55:9971  leader  yes       8       4           498.000  9.128 K
```
### showtablet

查看tablet信息(如果使用的serverName和自动获取本地ip功能，则endpoint为serverName, real_endpoint为“-”)

```
> showtablet
  endpoint             real_endpoint     state           age
--------------------------------------------------------------
  6534708411798331392  172.17.0.12:9531  kTabletHealthy  1d
  6534708415948603392  172.17.0.13:9532  kTabletHealthy  1d
  6534708420092481536  172.17.0.14:9533  kTabletHealthy  14h
```

### addreplica

添加副本

命令格式: addreplica table\_name pid\_group endpoint

* table\_name 表名
* pid\_group 分片id集合. 可以有以下几种情况
    * 单个分片
    * 多个分片, 分片间用逗号分割. 如1,3,5
    * 分片区间, 区间为闭区间. 如1-5表示分片1,2,3,4,5
* endpoint 要添加为副本的节点endpoint

```
> showtable test1
  name  tid  pid  endpoint            role      ttl   is_alive  compress_type  offset  record_cnt  memused
------------------------------------------------------------------------------------------------------------
  test1  13   0    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   1    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   2    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   3    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
> addreplica test1 0 172.27.128.33:8541
AddReplica ok
> showtable test1
  name  tid  pid  endpoint            role      ttl   is_alive  compress_type  offset  record_cnt  memused
------------------------------------------------------------------------------------------------------------
  test1  13   0    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   0    172.27.128.33:8541  follower  0min  yes       kNoCompress    0       0           0.000
  test1  13   1    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   2    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   3    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
> addreplica test1 1,2,3 172.27.128.33:8541
AddReplica ok
> addreplica test1 1-3 172.27.128.33:8541
AddReplica ok
```

### delreplica

删除副本

命令格式: delreplica table\_name pid\_group endpoint

* table\_name 表名
* pid\_group 分片id集合. 可以有以下几种情况
    * 单个分片
    * 多个分片, 分片间用逗号分割. 如1,3,5
    * 分片区间, 区间为闭区间. 如1-5表示分片1,2,3,4,5
* endpoint 要删除副本的endpoint

```
> showtable test1
  name  tid  pid  endpoint            role      ttl   is_alive  compress_type  offset  record_cnt  memused
------------------------------------------------------------------------------------------------------------
  test1  13   0    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   0    172.27.128.33:8541  follower  0min  yes       kNoCompress    0       0           0.000
  test1  13   1    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   1    172.27.128.33:8541  follower  0min  yes       kNoCompress    0       0           0.000
  test1  13   2    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   2    172.27.128.33:8541  follower  0min  yes       kNoCompress    0       0           0.000
  test1  13   3    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   3    172.27.128.33:8541  follower  0min  yes       kNoCompress    0       0           0.000
> delreplica test1 0 172.27.128.33:8541
DelReplica ok  
> showtable test1
  name  tid  pid  endpoint            role      ttl   is_alive  compress_type  offset  record_cnt  memused
------------------------------------------------------------------------------------------------------------
  test1  13   0    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   1    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   1    172.27.128.33:8541  follower  0min  yes       kNoCompress    0       0           0.000
  test1  13   2    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   2    172.27.128.33:8541  follower  0min  yes       kNoCompress    0       0           0.000
  test1  13   3    172.27.128.31:8541  leader    0min  yes       kNoCompress    0       0           0.000
  test1  13   3    172.27.128.33:8541  follower  0min  yes       kNoCompress    0       0           0.000
> delreplica test1 1,2,3 172.27.128.33:8541
DelReplica ok  
> delreplica test1 1-3 172.27.128.33:8541
DelReplica ok  
```

### migrate 

副本迁移

命令格式: migrate src\_endpoint table\_name pid\_group des\_endpoint

* src\_endpoint 需要迁出的节点
* table\_name 表名
* pid\_group 分片id集合. 可以有以下几种情况
    * 单个分片
    * 多个分片, 分片间用逗号分割. 如1,3,5
    * 分片区间, 区间为闭区间. 如1-5表示分片1,2,3,4,5
* des\_endpoint 迁移的目的节点

```
> migrate 172.27.2.52:9991 table1 1 172.27.2.52:9992
partition migrate ok
> migrate 172.27.2.52:9991 table1 1-5 172.27.2.52:9992
partition migrate ok
> migrate 172.27.2.52:9991 table1 1,2,3 172.27.2.52:9992
partition migrate ok
```
**注**: 该命令只能迁移从分片；如果需要迁移主分片，首先使用 `changeleader` 命令进行主从切换

### confget

获取配置信息，目前只支持auto\_failover

命令格式: confget \[conf\_name\]

* conf\_name 配置项名字，可选的

```
> confget
  key                 value
-----------------------------
  auto_failover       false
> confget auto_failover
  key            value
------------------------
  auto_failover  false
```

### confset

修改配置信息，目前只支持auto\_failover

命令格式: confset conf\_name value

* conf\_name 配置项名字
* value 配置项设置的值

```
> confset auto_failover true
set auto_failover ok
```

### offlineendpoint

下线节点。此命令是异步的返回成功后可通过showopstatus查看运行状态

命令格式: offlineendpoint endpoint [concurrency]

* endpoint是发生故障节点的endpoint。该命令会对该节点下所有分片执行如下操作:
  * 如果是主, 执行重新选主
  * 如果是从, 找到主节点然后从主节点中删除当前endpoint副本
  * 修改is_alive状态为no
* concurrency 控制任务执行的并发数. 此配置是可选的, 默认为2(name_server_task_concurrency配置可配), 最大值为name_server_task_max_concurrency配置的值

```bash
> offlineendpoint 172.27.128.32:8541
offline endpoint ok
>showtable
  name    tid  pid  endpoint            role      ttl       is_alive  compress_type  offset   record_cnt  memused
----------------------------------------------------------------------------------------------------------------------
  flow    4   0    172.27.128.32:8541  leader    0min       no        kNoCompress    0        0           0.000
  flow    4   0    172.27.128.33:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   0    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.33:8541  leader    0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.31:8541  follower  0min       yes       kNoCompress    0        0           0.000
  flow    4   1    172.27.128.32:8541  follower  0min       no        kNoCompress    0        0           0.000
```

该命令执行成功后所有分片都会有yes状态的leader

### recoverendpoint

恢复节点数据。此命令是异步的返回成功后可通过showopstatus查看运行状态

命令格式: recoverendpoint endpoint [need_restore] [concurrency]

* endpoint是要恢复节点的endpoint
* need_restore 表拓扑是否要恢复到最初的状态, 此配置是可选的, 默认为false. 如果设置为true, 一个分片在该节点下为leader, 执行完recoverendpoint恢复数据后依然是leader
* concurrency 控制任务执行的并发数. 此配置是可选的, 默认为2(name_server_task_concurrency配置可配), 最大值为name_server_task_max_concurrency配置的值

```
> recoverendpoint 172.27.128.32:8541
recover endpoint ok
> recoverendpoint 172.27.128.32:8541 true
recover endpoint ok
> recoverendpoint 172.27.128.32:8541 true 3
recover endpoint ok
```

**注:**

1. **执行此命令前确保节点已经上线\(showtablet命令查看\)**

### changeleader

对某个指定的分片执行主从切换。此命令是异步的返回成功后可通过showopstatus查看运行状态

命令格式: changeleader table\_name pid [candidate\_leader]

* table\_name 表名
* pid 分片id
* candidate\_leader 候选leader. 该参数是可选的。如果不加该参数则要求分片中不存在alive是yes的leader，如果设置成auto即使其他节点alive状态是yes也能切换

```
> changeleader flow 0
change leader ok
> changeleader flow 0 172.27.128.33:8541
change leader ok
> changeleader flow 0 auto
change leader ok
```

### recovertable

恢复某个分片数据。此命令是异步的返回成功后可通过showopstatus查看运行状态

命令格式: recovertable table\_name pid endpoint

* table\_name 表名
* pid 分片id
* endpoint 要恢复分片所在的节点endpoint

```
> recovertable flow 1 172.27.128.31:8541
recover table ok
```

### cancelop

取消一个正在执行或者待执行的操作. 取消后任务就状态变成kCanceled

命令格式: cancelop op\_id

* op\_id 需要取消的操作id

```
> cancelop 5
Cancel op ok!
```

### deleteop

删除op。可以指定op\_id删除一个op, 也可以指定op类型删除对应类型的所有op

命令格式: deleteop op\_id / op\_status

* op\_id 需要删除的op id
* op\_status 需要删除的op的状态。 可以指定的状态有done, failed和canceled

```
> deleteop 5
Delete op ok!
> deleteop done
Delete op ok!
```

### showopstatus

显示操作执行信息

命令格式: showopstatus \[table\_name pid\]

* table\_name 表名 
* pid 分片id

```
> showopstatus
  op_id  op_type                name   pid  status  start_time      execute_time  end_time        cur_task
------------------------------------------------------------------------------------------------------------
  51     kMigrateOP             flow   0    kDone   20180824163316  12s           20180824163328  -
  52     kRecoverTableOP        flow   0    kDone   20180824195252  1s            20180824195253  -
  53     kRecoverTableOP        flow   1    kDone   20180824195252  1s            20180824195253  -
  54     kUpdateTableAliveOP    flow   0    kDone   20180824195838  2s            20180824195840  -
  55     kChangeLeaderOP        flow   0    kDone   20180824200135  0s            20180824200135  -
  56     kOfflineReplicaOP      flow   1    kDone   20180824200135  1s            20180824200136  -
  57     kReAddReplicaOP        flow   0    kDone   20180827114331  12s           20180827114343  -
  58     kAddReplicaOP          test1  0    kDone   20180827205907  8s            20180827205915  -
  59     kDelReplicaOP          test1  0    kDone   20180827210248  4s            20180827210252  -
> showopstatus flow
  op_id  op_type                name  pid  status  start_time      execute_time  end_time        cur_task
-----------------------------------------------------------------------------------------------------------
  51     kMigrateOP             flow  0    kDone   20180824163316  12s           20180824163328  -
  52     kRecoverTableOP        flow  0    kDone   20180824195252  1s            20180824195253  -
  53     kRecoverTableOP        flow  1    kDone   20180824195252  1s            20180824195253  -
  54     kUpdateTableAliveOP    flow  0    kDone   20180824195838  2s            20180824195840  -
  55     kChangeLeaderOP        flow  0    kDone   20180824200135  0s            20180824200135  -
  56     kOfflineReplicaOP      flow  1    kDone   20180824200135  1s            20180824200136  -
  57     kUpdateTableAliveOP    flow  0    kDone   20180824200212  0s            20180824200212  -
>showopstatus flow 1
  op_id  op_type                name  pid  status  start_time      execute_time  end_time        cur_task
-----------------------------------------------------------------------------------------------------------
  53     kRecoverTableOP        flow  1    kDone   20180824195252  1s            20180824195253  -
  56     kOfflineReplicaOP      flow  1    kDone   20180824200135  1s            20180824200136  -
```
### updatetablealive

修改分片alive状态

命令格式: updatetablealive table\_name pid endpoint is\_alive

* table\_name 表名
* pid 分片id, 如果要修改某个表的所有分片将pid指定为*
* endpoint 节点endpoint
* is\_alive 节点状态, 只能填yes或者no

```
> updatetablealive test * 172.27.128.31:8541 no
update ok
> updatetablealive test 1 172.27.128.31:8542 no
update ok
```
**注: 此命令不可当做故障恢复使用. 一般用于切流量，操作方式为将某个节点表的alive状态改为no读请求就不会落在该节点上**

### showns

显示nameserver节点及其角色(如果使用的serverName和自动获取本地ip功能，则endpoint为serverName, real_endpoint为“-”)

命令格式: showns

```
>showns
  endpoint          real_endpoint  role
-------------------------------------------
  172.24.4.55:6531  -              leader
```

### exit

退出客户端

```
> exit
bye
```

### quit 

退出客户端

```
> quit
bye
```

## Tablet Client

连接到tablet client需要指定endpoint和role. 其中endpoint是需要连接tablet的endpoint，role是启动的角色需指定为client

```bash
$ ./openmldb --endpoint=172.27.2.52:9520 --role=client
```

### loadtable

1、加载已有表

命令格式: loadtable table\_name tid pid ttl segment\_cnt

* table\_name 表名
* tid 指定table的id
* pid 指定table的分片id
* ttl 指定ttl
* segment\_cnt 指定segment\_cnt, 一般设置为8

```
> loadtable table1 1 0 144000 8
```
已有表如果在内存中则loadtable会失败
### changerole

改变表的leader角色

命令格式: changerole tid pid role \[term\]

* tid 指定table的id
* pid 指定table的分片id
* role 要修改的角色, 取值为\[leader, follower\]
* term 设定term, 该项是可选的, 默认为0

```
> changerole 1 0 follower
ChangeRole ok
> changerole 1 0 leader
ChangeRole ok
> changerole 1 0 leader 1002
ChangeRole ok
```

### gettablestatus

获取表信息

命令格式: gettablestatus \[tid pid\]

* tid 指定table的id
* pid 指定table的分片id

```
> gettablestatus
  tid   pid  offset   mode            state            enable_expire  ttl               ttl_offset  memused  compress_type
----------------------------------------------------------------------------------------------------------------------------
  1     0    4        kTableLeader    kSnapshotPaused  true           144000min         0s          1.313 K  kNoCompress
  2     0    4        kTableLeader    kTableNormal     false          0min              0s          689.000  kNoCompress
> gettablestatus 2 0
  tid  pid  offset  mode          state         enable_expire  ttl   ttl_offset  memused  compress_type
---------------------------------------------------------------------------------------------------------
  2    0    4       kTableLeader  kTableNormal  false          0min  0s          689.000  kNoCompress
```

### getfollower

查看从节点信息

命令格式: getfollower tid pid

* tid 指定table的id
* pid 指定table的分片id

```
> getfollower 4 1
  #  tid  pid  leader_offset  follower            offset
-----------------------------------------------------------
  0  4    1    5923724        172.27.128.31:8541  5920714
  1  4    1    5923724        172.27.128.32:8541  5921707
```

### exit

退出客户端

```
> exit
bye
```

### quit

退出客户端

```
> quit
bye
```
