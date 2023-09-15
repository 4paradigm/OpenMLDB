# 更新配置文件

```{note}
下文均使用常规后台进程模式启动组件，如果想要使守护进程模式启动组件，请使用`bash bin/start.sh restart <component> mon`的方式启动。守护进程模式中，`bin/<component>.pid`将是mon进程的pid，`bin/<component>.pid.child`为组件真实的pid。mon进程并不是系统服务，如果mon进程意外退出，将无法继续守护。
```

## 更新 nameserver 配置文件
* 备份配置文件
    ```
    cp conf/nameserver.flags conf/nameserver.flags.bak
    ```
* 修改配置文件
* 重启 nameserver
    ```bash
    bash bin/start.sh restart nameserver
    ```
* 对剩余nameserver重复以上步骤

## 更新 tablet 配置文件

```{important}
如果有多个 tablet，请务必对每一个 tablet 进行顺序操作，不要同时对多个 tablet 进行更新配置操作。即对一个 tablet 完成配置更新，结果确认以后，再进行下一个 tablet 的更新配置操作。否则会导致集群状态异常。如果误操作导致集群异常，可以尝试使用[运维工具](openmldb_ops.md)的 `recoverdata` 进行恢复。
```

更新过程对服务的影响:

* 如果创建的表是单副本，用户可以选择：
   - 通过`pre-upgrade`和`post-upgrade`，重启前自动添加副本，重启后自动删除新添加的副本。这样行为和多副本保持一致
   - 如果允许单副本表在重启过程中不可用，可以在`pre-upgrade`的时候添加`--allow_single_replica`选项，在内存紧张的环境下，可以避免添加副本可能造成的OOM
* 更新过程中，会把待更新的tablet上的leader分片迁移到其它tablets上，更新结束会迁移回来。更新过程中，写请求会有少量的数据丢失，如果不能容忍少量写丢失，需要在更新过程中停掉写操作。

更新步骤如下：
* 备份配置文件
    ```
    cp conf/tablet.flags conf/tablet.flags.bak
    ```
* 修改配置文件
* 重启前准备`pre-upgrade`：为了避免对线上服务的影响，需要在重启tablet前，进行`pre-upgrade`操作，把该tablet上的leader分片迁移到其它tablets上（详细命令说明可以参考：[OpenMLDB运维工具](./openmldb_ops.md)）
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=pre-upgrade --endpoints=127.0.0.1:10921
    ```
  如果允许单副本表在升级更新中不可用，可以添加`--allow_single_replica`来避免添加新的副本。
* 重启tablet
    ```bash
    bash bin/start.sh restart tablet
    ```
* 如果auto\_failover处于关闭状态，需要手动执行`recoverdata`来恢复数据
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=recoverdata
    ```
* 重启后处理`post-upgrade`：把`pre-upgrade`迁移出的leader分片迁移回该tablet
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=post-upgrade --endpoints=127.0.0.1:10921
    ```

### 重启结果确认
* `showopstatus`命令查看是否有操作为kFailed, 如果有查看日志排查原因
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=showopstatus --filter=kFailed
    ```
* `showtablestatus`查看所有表状态是否正常
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=showtablestatus
    ```
    一个tablet节点更新完成后，对其他tablet重复上述步骤。

所有节点更新完成后恢复写操作，可以通过执行`showtablestatus`命令查看列 `Rows`，确认是否有新数据成功写入。

## 更新 APIServer 配置文件
* 备份配置文件
    ```
    cp conf/apiserver.flags conf/apiserver.flags.bak
    ```
* 修改配置文件
* 重启 APIServer
    ```bash
    bash bin/start.sh restart apiserver
    ```
## 更新 TaskManager 配置文件
* 备份配置文件
    ```
    cp conf/taskmanager.properties conf/taskmanager.properties.bak
    ```
* 修改配置文件
* 重启 TaskManager
    ```bash
    bash bin/start.sh restart taskmanager
    ```
