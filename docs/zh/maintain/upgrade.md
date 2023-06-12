# 版本升级

升级过程对服务的影响:
* 如果创建的表是单副本，用户可以选择：
   - 通过`pre-upgrade`和`post-upgrade`，升级前自动添加副本，升级结束会自动删除新添加的副本。这样行为和多副本保持一致
   - 如果允许单副本表在升级过程中不可用，可以在`pre-upgrade`的时候添加`--allow_single_replica`选项，在内存紧张的环境下，可以避免添加副本可能造成的OOM
* 升级过程中，会把待升级的tablet上的leader分片迁移到其它tablets上，升级结束会迁移回来。迁移过程中，写请求会有少量的数据丢失，如果不能容忍少量写丢失，需要在升级过程中停掉写操作。
* 
```{note}
下文均使用常规后台进程模式启动组件，如果想要使守护进程模式启动组件，请使用`bash bin/start.sh start <component> mon`的方式启动。守护进程模式中，`bin/<component>.pid`将是mon进程的pid，`bin/<component>.pid.child`为组件真实的pid。mon进程并不是系统服务，如果mon进程意外退出，将无法继续守护。
```

## 1. 升级nameserver

* 停止nameserver 
    ```bash
    bash bin/start.sh stop nameserver
    ```
* 备份旧版本bin目录
* 替换新版本bin
* 启动新版本nameserver
    ```bash
    bash bin/start.sh start nameserver
    ```
* 对剩余nameserver重复以上步骤

## 2. 升级tablet

* 升级前准备`pre-upgrade`：为了避免对线上服务的影响，需要在升级tablet前，进行`pre-upgrade`操作，把该tablet上的leader分片迁移到其它tablets上（详细命令说明可以参考：[OpenMLDB运维工具](./openmldb_ops.md)）
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=pre-upgrade --endpoints=127.0.0.1:10921
    ```
  如果允许单副本表在升级过程中不可用，可以添加`--allow_single_replica`来避免添加新的副本。
* 停止tablet
    ```bash
    bash bin/start.sh stop tablet
    ```
* 备份旧版本bin目录
* 替换新版本bin
* 启动新版本tablet
    ```bash
    bash bin/start.sh start tablet
    ```
* 如果auto\_failover处于关闭状态，需要手动执行`recoverdata`来恢复数据
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=recoverdata
    ```
* 升级后处理`post-upgrade`：把`pre-upgrade`迁移出的leader分片迁移回该tablet
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=post-upgrade --endpoints=127.0.0.1:10921
    ```
  
### 升级结果确认
* `showopstatus`命令查看是否有操作为kFailed, 如果有查看日志排查原因
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=showopstatus --filter=kFailed
    ```
* `showtablestatus`查看所有表状态是否正常
    ```bash
    python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.24.4.40:30481 --zk_root_path=/openmldb --cmd=showtablestatus
    ```
一个tablet节点升级完成后，对其他tablet重复上述步骤。

所有节点升级完成后恢复写操作, 执行`showtablestatus`命令查看`Rows`是否增加。

## 3. 升级 apiserver

* 停止 apiserver
    ```bash
    bash bin/start.sh stop apiserver
    ```
* 备份旧版本bin目录
* 替换新版本bin
* 启动apiserver
    ```bash
    bash bin/start.sh start apiserver
    ```

## 4. 升级 taskmanager
* 下载新版的OpenMLDB Spark发行版，替换老的Spark目录（即`$SPARK_HOME`指向的目录）
* 停止 taskmanager
    ```bash
    bash bin/start.sh stop taskmanager
    ```
* 备份旧版本bin和taskmanager目录
* 替换新版本bin和taskmanager
* 启动taskmanager
    ```bash
    bash bin/start.sh start taskmanager
    ```

### Yarn模式下的升级

Yarn模式下，第一步替换Spark时，还需要注意`spark.yarn.jars`和`batchjob.jar.path`的配置，如果指向HDFS路径，那么HDFS路径上的包也需要更新。这种情况下，更新TaskMananger的本地`$SPARK_HOME`目录不会影响到Yarn模式下的Spark。

剩下的TaskManager升级步骤和上文的步骤一致。

## 5. 升级SDK

### 升级Java SDK
* 更新pom文件中java sdk的版本号，包括`openmldb-jdbc`和`openmldb-native`

### 升级Python SDK
* 安装新版本的python sdk
  ```bash
  pip install openmldb=={NEW_VERSION}
  ```
