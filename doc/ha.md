# rtidb 高可用说明

## 新增节点
1. 在主节点运行pausesnapshot
cmd: pausesnapshot tid pid
> pausesnapshot 1 123

2. 获取主节点的状态为paused时将文件拷贝到新增节点的snapshot目录下
cmd: gettablestatus tid pid
> gettablestatus 1 123

3. 启动从节点的rtidb程序

4. 在从节点上运行loadsnapshot命令
cmd: loadsnapshot tid pid
> loadsnapshot 1 123

5. 在从节点上运行loadtable命令
cmd: loadtable name tid pid ttl
> loadtable table1 1 123 1000

6. 获取从节点的状态为normal时加载完成(正在加载的状态为loading)
cmd: gettablestatus
> gettablestatus 1 123

7. 在主节点上运行addreplica命令，建立主从关系
 cmd: addreplica tid pid endpoint(从节点的地址)
> addreplica 1 123 127.0.0.1:8893


## 从节点变为主节点
cmd: changerole tid pid leader
> changerole 1 123 leader

## 单节点故障恢复


### 删除binlogs目录下面所有文件

需要删除binlogs下面文件 不然会影响恢复数据

### 启动rtidb 

```
cd bin && sh start.sh

```

### 找出需要恢复的snapshot

```
cd snapshots 
```

下面去找要恢复的分片数据，结构为`tid-pid`的文件夹 比如 1-0文件夹代表tid为1 pid 为 0

### 加载snapshot

```
./bin/rtidb --endpoint=0.0.0.0:9527 --role=client
# 假设加载tid 为1 pid为0的分片数据
>loadsnapshot 1 0 
```

### 加载table

```
#如果上面进入客户端 就不用再执行这一步
./bin/rtidb --endpoint=0.0.0.0:9527 --role=client
# 假设加载表格名称为 tablename  , tid为 1 pid为0 ，ttl为144000 的表
>loadtable tablename 1 0 144000
```
### 检查table的加载进度

```
#如果上面进入客户端 就不用再执行这一步
./bin/rtidb --endpoint=0.0.0.0:9527 --role=client
#获取从节点的状态为normal时加载完成(正在加载的状态为loading)
>gettablestatus 1 0 
tid  pid  offset  mode            state         ttl
----------------------------------------------------------
1    0    1       kTableFollower  kTableNormal  144000
#kTableLoading 表示正在加载
#kTableNormal 表示加载成功
```

### 改变表格状态为可写状态

当数据加载完成后，执行一下操作

```
#如果上面进入客户端 就不用再执行这一步
./bin/rtidb --endpoint=0.0.0.0:9527 --role=client
>changerole 1 0 leader
#检查是否能够写入，如果OK，则正常
>put 1 0 test 1 test0
```
到这里操作完成

