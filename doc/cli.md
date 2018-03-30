# rtidb cli使用说明


## 创建多副本表

### 从tablet client创建表

假设集群有一下节点
* leader
* follower1
* follower2

```
# 在slave1 上创建表信息
./rtidb --endpoint=slave1 --role=client
>create t1 1 1 0 8 false slave1 slave2

# 在slave2 上创建表信息
./rtidb --endpoint=slave2 --role=client
>create t1 1 1 0 8 false slave1 slave2

# 在leader 上创建表信息
./rtidb --endpoint=leader --role=client
>create t1 1 1 0 8 true slave1 slave2

```

### 创建时序表命令

创建命名格式

create table_name tid pid ttl segment_cnt
* create 为创建命令
* table_name 为要创建表名称
* tid 指定table 的id
* pid 指定table 的分片id
* ttl 指定过期时间， 单位为分钟
* segment_cnt 为表的segement 个数，建议为8~1024
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
>screate tx 1 0 latest:2 8 true card:string:index merchant:string:index amt:double
```

### 创建一个带schema的follower表保留最新两条记录配置

```
>screate tx 1 0 latest:2 8 false card:string:index merchant:string:index amt:double
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
