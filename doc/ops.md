# RTIDB运维文档

## 分布式运维

### 连接ns_client
连接ns_client有两种方式
* 无需指定nameserver主节点  
  ex: ./bin/rtidb --zk_cluster=172.27.128.31:6181,172.27.128.32:6181 --zk_root_path=/onebox --role=ns_client  
  zk_cluster: 指定zookeeper服务的ip和port  
  zk_root_path: 指定rtidb集群在zookeeper中的根目录  
  role: 指定启动角色为nameserver client
* 指定nameserver主节点  
  ex: ./bin/rtidb --endpoint=172.27.128.31:9992 --role=ns_client  
  endpoint: 指定nameserver的主节点. 如果提供的endpoint不是主节点, 执行其他命令时就会提示连接的不是主节点  
  role: 指定启动角色为nameserver client

### 命令帮助及用法  
help和help cmd  
```
>help put
desc: insert data into table
usage: put table_name pk ts value
usage: put table_name ts key1 key2 ... value1 value2 ...
ex: put table1 key1 1528872944000 value1
ex: put table2 1528872944000 card0 mcc0 1.3
```

### 创建表
命令格式: create table_meta_path  
```
> create ./table_meta.txt
```
创建表分为单维表和多维表. 区别就是table_meta文件
* 创建单维表  
文件格式为proto的文件  
name 表示要创建的表名    
ttl_type 表示过期类型  
  1 如果指定的ttl_type为kAbsoluteTime, 对应ttl配置为过期时间, **单位是分钟**  
  2 如果指定的ttl_type为kLatestTime, 对应ttl配置为过期条数. 如果配成100表示保留最近100条   
partition_num指定分片数, 此项可以用不设置默认值为16  
replica_num指定副本数, 此项可以不用设置默认为3   
```
name : "test3"
ttl: 100
ttl_type : "kLatestTime"
partition_num: 16
replica_num: 3
```

* 创建多维表  
多维表的创建和单维表类似, 只是多了column_desc的结构  
column_desc描述每个字段(维度)信息.   
name为字段名  
type为字段类型. 可以指定int32, uint32, int64, uint64, float, double, string  
add_ts_idx指定是否是索引列. 如果设置为true, 可以按此列来get和scan  
也可以用table_partition自己设定分片的分布情况. endpoint指定要在该endpoint创建, pid_group指定分片的区间, is_leader表示角色(true为leader, false为follower).  
```
name : "test3"
ttl: 100
ttl_type : "kLatestTime"
table_partition {
  endpoint: "172.27.128.31:9520"
  pid_group: "0-3"
  is_leader: true
}
table_partition {
  endpoint: "172.27.128.31:9521"
  pid_group: "4-6"
  is_leader: true
}
table_partition {
  endpoint: "172.27.128.32:9522"
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

### 添加副本
命令格式: addreplica table_name pid endpoint  
```
ex: 给表名为name1分片id为0添加一个副本, 副本的endpoint为172.27.128.31:9991
>addreplica name1 0 172.27.128.31:9991
```

### 删除副本
命令格式: delreplica table_name pid endpoint  
```
ex: 删除表名为name1分片id为0 endpoint为172.27.128.31:9991的副本
>delreplica name1 0 172.27.128.31:9991
```

### 扩容
随着业务的发展, 当前集群的拓扑不能满足要求就需要动态的扩容.  
扩容时新增机器的tablet的配置除了endpoint和其他机器保持一致. 先启动服务然后做副本迁移  
副本迁移是从已有节点中把一部分部分迁移到新的节点上. 用到的命令时migrate  
命令格式: migrate src_endpoint table_name partition des_endpoint  
**注: 迁移的时候只能迁移从, 不能迁移主** 
```
查看表的分片信息
>showtable flow_trans
name        tid  pid  endpoint            role      seg_cnt  ttl  is_alive
------------------------------------------------------------------------------
  flow_trans  10   0    172.27.128.32:9991  follower  8        0    yes
  flow_trans  10   0    172.27.128.33:9991  follower  8        0    yes
  flow_trans  10   0    172.27.128.31:9991  leader    8        0    yes
  flow_trans  10   1    172.27.128.33:9991  follower  8        0    yes
  flow_trans  10   1    172.27.128.31:9991  leader    8        0    yes
  flow_trans  10   1    172.27.128.32:9991  follower  8        0    yes
  flow_trans  10   2    172.27.128.32:9991  follower  8        0    yes
  flow_trans  10   2    172.27.128.33:9991  follower  8        0    yes
  flow_trans  10   2    172.27.128.31:9991  leader    8        0    yes
  flow_trans  10   3    172.27.128.33:9991  follower  8        0    yes
  flow_trans  10   3    172.27.128.31:9991  leader    8        0    yes
  flow_trans  10   3    172.27.128.32:9991  follower  8        0    yes
将172.27.128.32:9991节点中flow_trans的分片1迁移到172.27.2.52:9991里
>migrate 172.27.128.32:9991 flow_trans 1 172.27.2.52:9992
也可以一次迁移多个分片, 如将172.27.128.32:9991节点中flow_trans的1到3分片迁移到172.27.2.52:9991里
>migrate 172.27.128.32:9991 flow_trans 1-3 172.27.2.52:9992
```

### 机器下线与恢复
由机器宕机、tablet服务挂掉以及网络断开等原因造成节点下线并且auto_failover没有开启的情况下就需要手动操作(运行命令: confget 可以查看,如果需要修改用confset)  
命令格式: offlineendpoint endpoint  
该命令会对所有分片执行如下操作:
* 如果是主, 执行重新选主
* 如果是从, 找到主节点然后从主节点中删除当前endpoint副本
```
>offlineendpoint 172.27.2.52:9991
```

#### 节点恢复
如果机器重新恢复了(节点重启等)可以执行recoverendpoint来恢复该节点在不可用之前的状态(包括恢复数据)  
命令格式: recoverendpoint endpoint  
```
>recoverendpoint 172.27.2.52:9991
```
如果表的某个分片恢复失败可以单独恢复失败的分片  
命令格式: recovertable table_name pid endpoint
```
>recovertable name1 0 172.27.2.52:9991
```
在恢复的过程中执行showopstatus可以查看进度  
命令格式: 
*showopstatus  
*showopstatus table_name  
*showopstatus table_name pid  
