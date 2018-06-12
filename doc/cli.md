# rtidb cli使用说明

## tablet客户端命令
create 创建单维表  
put get scan  

screate 创建多维表  
sput sget sscan  
showschema 查看指定多维表的schema  

loadtable  创建表并加载sdb和binlog的数据  
drop  删除表  
gettablestatus  获取表的信息  

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
settablepartition  用指定的文件覆盖nameserver中某个table partition的信息  


## tablet命令用法
create  创建单维表  
命名格式: create table_name tid pid ttl segment_cnt is_leader(optional) follower1 follower2 ...  
* table_name 为要创建表名称
* tid 指定table的id
* pid 指定table的分片id
* ttl 指定过期时间,默认过期类型为AbsoluteTime. 如果过期类型设置LatestTime,格式为latest:保留条数(ex: latest:10, 保留最近10条)
* segment_cnt 为表的segement 个数, 建议为8~1024
* is_leader 指定是否为leader. 默认为leader
* follower 指定follower

```
创建一个leader表
> create t1 1 0 144000 8
创建一个leader表, 指定follwer为172.27.128.31:9991, 172.27.128.31:9992
> create t1 1 0 144000 8 false 172.27.128.31:9991 172.27.128.31:9992
创建一个follower表, 过期类型设为保留最近10条
> create t1 1 0 latest:10 8 false
```

put 插入数据  
命令格式: put tid pid pk ts value  
* tid 指定table的id
* pid 指定table的分片id
* pk 要插入的key
* ts 指定插入时的timestamp.
* value 对应的value

get 获取指定key的数据  
命令格式: get tid pid pk ts
* tid 指定table的id
* pid 指定table的分片id
* pk 指定查询的key
* ts 指定查询的timestamp. 如果ts设置为0, 返回最新的一条数据

scan 获取指定key在一个时间区间里的数据  
命令格式: scan tid pid pk starttime endtime
* tid 指定table的id
* pid 指定table的分片id
* pk 指定查询的key
* starttime 指定查询时的起始时间
* endtime 指定查询区间的结束时间

```
> put 1 0 test1 1522390533000 value1
Put ok
> put 1 0 test1 1522390534000 value2
Put ok
> put 1 0 test2 1522390534000 value3
Put ok
> get 1 0 test1 1522390534000
value :value2
> scan 1 0 test1 1522390534000 1522390532000
#   Time    Data
1   1522390534000   value2
2   1522390533000   value1
```

screate 创建多维表  
命令格式: screate table_name tid pid ttl segment_cnt is_leader index1 index2 ...
* table_name 为要创建表名称
* tid 指定table 的id
* pid 指定table 的分片id
* ttl 指定过期时间,默认过期类型为AbsoluteTime. 如果过期类型设置LatestTime,格式为latest:保留条数(ex: latest:10, 保留最近10条)
* segment_cnt 为表的segement 个数，建议为8~1024
* is_leader 指定是否为leader. 默认为leader
* index 指定维度信息. 格式为: name:type(:index), 如card:string:index

sput 多维put  
命令格式: sput tid pid ts key1 key2 value
* tid 指定table的id
* pid 指定table的分片id
* ts 指定插入时的timestamp
* key1 指定插入时维度1的key
* key2 指定插入时维度2的key
* value 对应的value

sget 多维get  
命令格式: sget tid pid key key_name ts
* tid 指定table的id
* pid 指定table的分片id
* key 指定查询的key
* idx_name 指定key的维度
* ts 查询时的ts(如果为0返回最新的数据)

sscan 多维scan  
命令格式: sscan tid pid key key_name start_time end_time  
* tid 指定table的id
* pid 指定table的分片id
* key 指定查询的key
* idx_name 指定key的维度
* start_time 指定查询的起始时间
* end_time 指定查询时的结束时间

showschema 查看指定多维表的schema  
命令格式: showschema tid pid  

```
创建一个带schema的leader表
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

loadtable 创建表并加载sdb和binlog的数据  
命令格式: loadtable table_name tid pid ttl segment_cnt 
* table_name 为要创建表名称
* tid 指定table的id
* pid 指定table的分片id
* ttl 指定过期时间. 过期类型从table_meta文件中载入
* segment_cnt 为表的segement 个数. 建议为8~1024

drop 删除表  
命令格式: drop tid pid  

gettablestatus 获取表的状态  
命令格式: gettablestatus tid(optional) pid(optional)  
如果没有指定tid和pid就返回所有表的信息, 如果指定了tid和pid就返回该分片的信息  
```
>gettablestatus
  tid  pid  offset  mode          state         enable_expire  ttl        ttl_offset  memused
-----------------------------------------------------------------------------------------------
  1    0    2       kTableLeader  kTableNormal  false          0min       0s          913.000
  10   0    3       kTableLeader  kTableNormal  false          144000min  0s          391.000
  10   1    2       kTableLeader  kTableNormal  true           144000min  0s          337.000
  10   2    0       kTableLeader  kTableNormal  true           144000min  0s          0.000
  10   3    0       kTableLeader  kTableNormal  true           144000min  0s          0.000
  10   4    0       kTableLeader  kTableNormal  true           144000min  0s          0.000
>gettablestatus 1 0
  tid  pid  offset  mode          state         enable_expire  ttl   ttl_offset  memused
------------------------------------------------------------------------------------------
  1    0    2       kTableLeader  kTableNormal  false          0min  0s          913.000
```

addreplica 添加副本(在leader所在的tablet上运行)  
命令格式: addreplica tid pid endpoint
* tid 指定table的id
* pid 指定table的分片id
* endpoint 指定要添加的endpoint  
delreplica 删除副本  
命令格式: delreplica tid pid endpoint  

makesnapshot 生成snapshot  
命令格式: makesnapshot tid pid  
pausesnapshot 暂停makesnapshot功能  
命令格式: pausesnapshot tid pid  
recoversnapshot 恢复makesnapshot功能  
命令格式: recoversnapshot tid pid  
sendsnapshot 给指定endpoint发送snapshot, 包括table.meta, MANIFEST和sdb文件. 注: 发送前必须pausesnapshot, 发送完再运行recoversnapshot  
命令格式: sendsnapshot tid pid endpoint  

changerole 切换leader或者follower  
命令格式: changerole tid pid role  
* tid 指定table的id
* pid 指定table的分片id
* role 只能是leader或者follower
setexpire 设置是否要开启过期删除  
命令格式: set_expire tid pid is_expire  
* tid 指定table的id
* pid 指定table的分片id
* is_expire 指定是否要开启过期删除. 如果开启的话设置为true, 否则设置为false


## nameserver命令用法

create 创建表  
命令格式: 
(1) create table_name ttl partition_num replica_num [colum_name1:type:index colum_name2:type ...]
    * table_name 表名
    * ttl 过期时间.默认是按时间过期, 如果想指定按条数过期设置ttl为: latest:record_num
    * partition_num 分片数
    * replica_num 副本数. 如果设置为3, 那么就是一主两从
    * 可选 指定schema信息
```
>create table1 120 8 3
Create table ok
>create table2 latest:10 8 3 card:string:index mcc:string:index value:float
Create table ok
```
(2) create table_meta_path  
首先准备如下格式的表元数据文件. 其中name和ttl是必填的
```
name : "test1"
ttl: 144000
ttl_type : "kAbsoluteTime"
seg_cnt: 8
table_partition {
  endpoint: "172.27.128.31:9527"
  pid_group: "0-9"
  is_leader: true
}
table_partition {
  endpoint: "172.27.128.32:9527"
  pid_group: "3-7"
  is_leader: false 
}
table_partition {
  endpoint: "172.27.128.33:9527"
  pid_group: "3-7"
  is_leader: false 
}

```
上面的配置表示再172.27.128.31:9527创建pid为0到9的leader节点, 在172.27.128.32:9527和172.27.128.33:9527上创建pid为3-7的follower节点
其中table_partition的结构可以重复多次

如果要创建的表是多维表，元数据文件格式如下
```
name : "test3"
ttl: 100
ttl_type : "kLatestTime"
seg_cnt: 8
table_partition {
  endpoint: "172.27.128.31:9527"
  pid_group: "0-3"
  is_leader: true
}
table_partition {
  endpoint: "172.27.128.32:9527"
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
ttl_type可以设置kAbsoluteTime(按时间过期)和kLatestTime(按条数过期), 默认为kAbsoluteTime  
table_partition 可以不用指定  
partition_num用来设置分片数. 只有不指定table_partition时生效. 此项配置是可选的, 默认值是32可由配置文件partition_num来修改  
replica_num用来设置副本数. 只有不指定table_partition时生效. 此项配置是可选的, 默认值是3, 表示3副本部署时会部一主两从. 可由配置文件replica_num来修改
column_desc用来描述维度信息，有多少个维度就创建多少个column_desc结构  
type字段标识当前列的数据类型. 支持的数据类型有int32, uint32, int64, uint64, float, double, string  
```
如果表元数据信息保存在了./table_meta.txt，则运行如下命令
>create ./table_meta.txt
```
drop 删除表  
命令格式: drop table_name  
showtable 列出表的分布和状态信息  
命令格式: showtable table_name(optional)  
注: table_name可选的, 如果不指定name就返回所有表的信息  
showshema 获取多维表的schema信息  
命令格式: showschema table_name  

makesnapshot 做snapshot  
命令格式: makesnapshot table_name pid  
addreplica 添加副本  
命令格式: addreplica name pid endpoint  
delreplica 删除副本  
命令格式: delreplica name pid endpoint  

offlineendpoint  auto_failover关闭时指定endpoint做failover  
命令格式: offlineendpoint endpoint  
recoverendpoint  auto_recover_table关闭时指定endpoint加入到集群中  
命令格式: recoverendpoint endpoint  
changeleader 对某个表的分片执行重新选主操作  
命令格式: changeleader table_name pid  
recovertable 恢复某个节点上的分片  
命令格式: recovertable table_name pid endpoint  

migrate 分片迁移  
命令格式: migrate src_endpoint table_name partition des_endpoint  
```
将172.27.2.52:9991节点中table1的1到10分片迁移到172.27.2.52:9992里
>migrate 172.27.2.52:9991 table1 1-10 172.27.2.52:9992
```

gettablepartition 获取nameserver某个table partition的信息并下载到当前目录下  
命令格式: gettablepartition table_name pid  
settablepartition 用指定的文件覆盖nameserver中某个table partition的信息  
命令格式: settablepartition partition_file_path  

showtablet 获取所有tablet的健康状态信息  
命令格式: showtablet  
showopstatus 获取所有操作信息  
命令格式: showopstatus
```
>showtablet
  endpoint        state           age
------------------------------------------
  127.0.0.1:9520  kTabletHealthy  3.000h
  127.0.0.1:9521  kTabletHealthy  3.000h
  127.0.0.1:9522  kTabletHealthy  3.000h
>showopstatus
  op_id  op_type              status  start_time      execute_time  end_time        cur_task
----------------------------------------------------------------------------------------------
  2      kUpdateTableAliveOP  kDone   20180330175434  0s            20180330175434  -
```
