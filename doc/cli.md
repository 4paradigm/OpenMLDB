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
>create t1 1 1 0 false slave1 slave2

# 在slave2 上创建表信息
./rtidb --endpoint=slave2 --role=client
>create t1 1 1 0 false slave1 slave2

# 在leader 上创建表信息
./rtidb --endpoint=leader --role=client
>create t1 1 1 0 true slave1 slave2

```

## tablet schema相关操作

```
>screate t1 1 0 0 8 card:string amt:double merchant:string apprv_cde:int32
Create table ok
>showschema 1 0
  index  name       type
----------------------------
  0      card       string
  1      amt        double
  2      merchant   string
  3      apprv_cde  int32
>sput 1 0 9527 1 9527 1.2 711 0
Put ok
>sput 1 0 9527 2 9527 3.2 711 1
Put ok
>sscan 1 0 9527 3 0


  pk    ts  card  amt                 merchant  apprv_cde
-----------------------------------------------------------
  9527  2   9527  3.2000000000000002  711       1
  9527  1   9527  1.2                 711       0
>
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
