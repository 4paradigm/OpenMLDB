# rtidb cli使用说明


## 创建多副本表

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
