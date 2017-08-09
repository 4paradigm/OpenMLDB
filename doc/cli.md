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
