# 版本升级

升级过程对服务的影响:
* 如果创建的表是单副本，那么在升级过程中是不可以读写的  
* 如果创建的表是多副本，落在升级节点的读请求会有短暂的失败，对写请求会有少量的数据丢失。如果不能容忍短暂的读失败，那么在停止每一个tablet节点前执行下offlineendpoint。如果不能容忍少量写丢失，需要在升级过程中停掉写操作。

## 1. 升级nameserver

* 停止nameserver 
    ```bash
    bash bin/start.sh stop nameserver
    ```
* 备份旧版本bin和conf目录
* 下载新版本bin和conf
* 对比配置文件diff并修改必要的配置，如endpoint、zk\_cluster等
* 启动nameserver
    ```bash
    bash bin/start.sh start nameserver
    ```
* 对剩余nameserver重复以上步骤

## 2. 升级tablet

* 停止tablet
    ```bash
    bash bin/start.sh stop tablet
    ```
* 备份旧版本bin和conf目录
* 下载新版本bin和conf
* 对比配置文件diff并修改必要的配置，如endpoint、zk\_cluster等
* 启动tablet
    ```bash
    bash bin/start.sh start tablet
    ```
* 如果auto\_failover关闭时得连上ns client执行如下操作恢复数据。其中**命令后面的endpoint为重启节点的endpoint**
  * offlineendpoint endpoint 
  * recoverendpoint endpoint

```
$ ./bin/openmldb --zk_cluster=172.27.128.31:8090,172.27.128.32:8090,172.27.128.33:8090 --zk_root_path=/openmldb_cluster --role=ns_client
> offlineendpoint 172.27.128.32:8541
offline endpoint ok
> recoverendpoint 172.27.128.32:8541
recover endpoint ok
```

### 升级结果确认
* showopstatus命令查看所有操作是否为kDone, 如果有kFailed的任务查看日志排查原因
* showtable查看所有分片状态是否为yes

一个tablet节点升级完成后，对其他tablet重复上述步骤。\(**必须等到数据同步完才能升级下一个节点**\)

所有节点升级完成后恢复写操作, 执行showtable命令查看主从offset是否增加

## 3. 升级java client

* 更新pom文件中java client版本号
* 更新依赖包
