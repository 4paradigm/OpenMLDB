# rtidb cli使用说明

## tablet客户端命令
loadtable  创建表并加载sdb和binlog的数据
drop  删除表
gettablestatus  获取表的信息

[create](#create) 创建单维表
<a href="#put">put</a>
<a href="#get">get</a>
<a href="#scan">scan</a>

<a href="#screate">screate</a> 创建多维表
<a href="#sput">sput</a>
<a href="#sget">sget</a>
<a href="#sscan">sscan</a>
<a href="#showschema">showschema</a> 查看指定多维表的schema

addreplica  添加副本
delreplica  删除副本

makesnapshot  生成snapshot
pausesnapshot  暂停makesnapshot功能
recoversnapshot  恢复makesnapshot功能
sendsnapshot  给指定endpoint发送snapshot, 包括table.meta, MANIFEST和sdb文件

changerole  切换leader或者follower
setexpire  设置是否要开启过期删除

exit  退出当前会话

## nameserver客户端命令

showtablet  获取所有tablet的健康状态信息
showopstatus  获取所有操作信息

create  创建表
drop  删除表
showtable  获取表信息
showschema  获取多维表的schema信息

confget 获取当前conf信息
confset 重新设置conf

makesnapshot
addreplica
delreplica
changeleader
offlineendpoint
recoverendpoint
recovertable

migrate  分片迁移

gettablepartition  获取nameserver某个table partition的信息并下载到当前目录下
**settablepartition命令慎用**
settablepartition  用指定的文件覆盖nameserver中某个table partition的信息

<span id="create">create</span>  创建单维表
命名格式
create table_name tid pid ttl segment_cnt is_leader(optional) follower1 follower2 ...
* create 为创建命令
* table_name 为要创建表名称
* tid 指定table 的id
* pid 指定table 的分片id
* ttl 指定过期时间,默认过期类型为AbsoluteTime. 如果过期类型设置LatestTime,格式为latest:保留条数(ex: latest:10, 保留最近10条)
* segment_cnt 为表的segement 个数, 建议为8~1024
* is_leader

例子
```
> create t1 1 0 144000 8
```

### 创建多版本kv表

创建命名格式
create table_name tid pid latest:count segment_cnt
* create 为创建命令
* table_name 为要创建表名称
* tid 指定table 的id
* pid 指定table 的分片id
* latest:count 指定保留几条记录，例如latest:1 代表保留最新的一条记录
* segment_cnt 为表的segement 个数，建议为8~1024
例子
```
create t1 1 0 latest:2 8
```

## tablet schema相关操作

### 创建一个带schema的leader表保留最新两条记录配置

```
>screate tx 1 0 latest:2 true card:string:index merchant:string:index amt:double
```

### 创建一个带schema的follower表保留最新两条记录配置

```
>screate tx 1 0 latest:2 false card:string:index merchant:string:index amt:double
```

### 相关操作示例

```
Welcome to rtidb with version 1.1.0
#创建一个带schema 的leader表
>screate tx 1 0 0 8 true card:string:index merchant:string:index amt:double
Create table ok
>showschema 1 0
  #  name      type    index
------------------------------
  0  card      string  yes
  1  merchant  string  yes
  2  amt       double  no
>sput 1 0 1 card0 merchant0 1.1
Put ok
>sput 1 0 2 card0 merchant1 110.1
Put ok
>sscan 1 0 card0 card 3 0


  pk     ts  card   merchant   amt
---------------------------------------------------
  card0  2   card0  merchant1  110.09999999999999
  card0  1   card0  merchant0  1.1000000000000001
>sscan 1 0 merchant0 merchant 2 0

  pk         ts  card   merchant   amt
-------------------------------------------------------
  merchant0  1   card0  merchant0  1.1000000000000001
>sscan 1 0 merchant1 merchant 2 0

  pk         ts  card   merchant   amt
-------------------------------------------------------
  merchant1  2   card0  merchant1  110.09999999999999
```

### cluster开启时从ns_client创建表

首先准备如下格式的表元数据文件
```
name : "test1"
ttl: 144000
seg_cnt: 8
table_partition {
  endpoint: "0.0.0.0:9993"
  pid_group: "0-9"
  is_leader: true
}
table_partition {
  endpoint: "0.0.0.0:9994"
  pid_group: "3-7"
  is_leader: false 
}
table_partition {
  endpoint: "0.0.0.0:9995"
  pid_group: "3-7"
  is_leader: false 
}

```
上面的配置表示再0.0.0.0:9993创建pid为0到9的leader节点, 在0.0.0.0:9994和0.0.0.0:9995上创建pid为3-7的follower节点
其中table_partition的结构可以重复多次

如果要创建的表是多维表，元数据文件格式如下
```
name : "test3"
ttl: 100
ttl_type : "kLatestTime"
seg_cnt: 8
table_partition {
  endpoint: "127.0.0.1:9521"
  pid_group: "1-3"
  is_leader: true
}
table_partition {
  endpoint: "127.0.0.1:9522"
  pid_group: "1-2"
  is_leader: false
}
column_desc {
  name : "card"
  type : "string"
  add_ts_idx : true
}
column_desc {
  name : "demo"
  type : "string"
  add_ts_idx : true
}
column_desc {
  name : "value"
  type : "string"
  add_ts_idx : false
}
```
column_desc用来描述维度信息，有多少个维度就创建多少个column_desc结构

然后再nameserver的client上运行如下命令

```
1 启动client
./rtidb --endpoint=ip:port --role=ns_client
如 ./rtidb --endpoint=127.0.0.1:7561 --role=ns_client

2 创建表
>create table_meta_path
如果表元数据信息保存在了./table_meta.txt，则运行如下命令
>create ./table_meta.txt
```

### cluster开启时从ns_client新增分片副本

```
1 makesnapshot 如果主节点下面已经有snapshot文件可以跳过这步
cmd makesnapshot table_name pid
> makesnapshot test1 1

2 新增分片
cmd addreplica 表名 分片id 新副本所在节点的ip:port
新增副本时主节点下面必须有snapshot, 如果没有的话先运行makesnapshot
>addreplica table_name pid endpoint

```

### cluster开启时从ns_client删除表

```
> drop tablename
```

### cluster开启时从ns_client获取表的信息

```
列出所有表的信息
> showtable
列出test1表的信息
> showtable test1
```
