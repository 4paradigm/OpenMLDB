# 集群启停
OpenMLDB集群目前有两种部署方式，[一键部署和手动部署](../deploy/install_deploy.md)。本文针对这两种不同的部署方式，分别描述启停步骤。注意，请不要将两种部署方式的的启停方法混合使用，可能会导致不可预知的问题。

```{important}
如果是第一次部署和启动集群，请参考完整的[安装部署文档](../deploy/install_deploy.md)，需要额外的配置和注意事项。本文针对已经完成部署的集群，因为各类原因（比如例行维护、升级、配置更新等），而涉及到启动、停止、重启操作的场景。
```


## 一键部署的集群

### 启动集群

到部署集群的目录执行，确保该目录下 conf/hosts 文件内容和实际各组件的部署信息一致

```
sbin/start-all.sh
```

### 停止集群

到部署集群的目录执行，确保该目录下 conf/hosts 文件内容和实际各组件的部署信息一致
```
sbin/stop-all.sh
```

## 手动部署的集群

### 启动集群

**1. 启动TabletServer**

到每一个部署 TabletServer 节点的部署目录里执行如下命令

```
bash bin/start.sh start tablet
```

启动后应有`success`提示，如下所示。

```
Starting tablet ...
Start tablet success
```

**2. 启动 NameServer**

到每一个部署 NameServer 节点的部署目录里执行如下命令

```
bash bin/start.sh start nameserver
```

启动后应有`success`提示，如下所示。

```
Starting nameserver ...
Start nameserver success
```

**3. 启动 TaskManager**

如果没有部署 TaskManager 可以跳过此步  
到每一个部署 TaskManager 节点的部署目录里执行如下命令

```
bash bin/start.sh start taskmanager
```

**4. 启动 APIServer**

如果没有部署 APIServer 可以跳过此步  
到每一个部署 APIServer 节点的部署目录里执行如下命令

```
bash bin/start.sh start apiserver
```

启动后应有`success`提示，如下所示。

```
Starting apiserver ...
Start apiserver success
```

**5. 检查服务是否启动**

启动 sql_client，其中`zk_cluster`和`zk_root_path`的值替换为配置文件中对应的值

```bash
./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/openmldb --role=sql_client
```

然后执行如下命令

```
show components;
```

结果应类似下表，包含所有集群的组件（APIServer除外）。

```
------------------- ------------ --------------------- -------- ---------
  Endpoint            Role         Connect_time          Status   Ns_role
 ------------------- ------------ --------------------- -------- ---------
  172.24.4.39:10821   tablet       2023-09-01 11:36:58   online   NULL
  172.24.4.40:10821   tablet       2023-09-01 11:36:57   online   NULL
  172.24.4.56:10821   tablet       2023-09-01 11:36:58   online   NULL
  172.24.4.40:7520    nameserver   2023-09-01 11:36:59   online   master
 ------------------- ------------ --------------------- -------- ---------

4 rows in set
```

**6. 开启 auto_failover**

启动 ns_client，其中`zk_cluster`和`zk_root_path`的值替换为配置文件中对应的值

```
./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/openmldb --role=ns_client
```

执行如下命令

```
confset auto_failover true 
```

**7. 恢复数据**

使用OpenMLDB运维工具中的一键数据恢复来恢复数据。关于运维工具的使用和说明参考[OpenMLDB运维工具](./openmldb_ops.md)

到任意一个OpenMLDB的部署目录中执行如下命令，其中`zk_cluster`和`zk_root_path`的值替换为配置文件中对应的值。此命令只需要执行一次，执行过程中不要中断

```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/openmldb --cmd=recoverdata
```

### 停止集群

**1. 关闭 auto_failover**

启动 ns_client，其中`zk_cluster`和`zk_root_path`的值替换为配置文件中对应的值
```
./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/openmldb --role=ns_client
```
执行如下命令
```
confset auto_failover false 
```

**2. 停止 TabletServer**

到每一个部署 TabletServer 节点的部署目录里执行如下命令
```
bash bin/start.sh stop tablet
```
**3. 停止 NameServer**

到每一个部署 NameServer 节点的部署目录里执行如下命令
```
bash bin/start.sh stop nameserver
```
**4. 停止 TaskManager**

如果没有部署 TaskManager 可以跳过此步  
到每一个部署 TaskManager 节点的部署目录里执行如下命令
```
bash bin/start.sh stop taskmanager
```

**5. 停止 APIServer**

如果没有部署 APIServer 可以跳过此步  
到每一个部署 TaskManager 节点的部署目录里执行如下命令
```
bash bin/start.sh stop apiserver
```

## 集群重启 

集群重启分为两种情况，一种是对于版本升级或者更新配置文件需要，进行集群重启；还有一种是不需要进行升级和配置，正常状态下的重启（比如例行维护）。

- 版本升级或者更新配置文件：具体步骤查看相关操作文档进行操作，[版本升级](upgrade.md)，[更新配置文件](update_conf.md)。特别注意的是，对于 tablet 的重启，务必进行顺序化操作，不要对多个 tablet 同时进行升级或者配置。需要等一个 tablet 完成升级/配置以后，确认重启状态，再进行下一个 tablet 的操作。具体操作查看版本升级和更新配置文件文档的“重启结果确认”章节。
- 正常重启：如果不涉及到版本升级或者更新配置文件，那么依次执行上述章节的停止集群和启动集群操作即可。
