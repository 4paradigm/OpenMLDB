# 集群启停
OpenMLDB集群目前有两种部署方式，[一键部署和手动部署](../deploy/install_deploy.md)。针对不同的部署方式采用不同的重启策略

## 手动部署的集群
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

## 用一键部署脚本部署的集群

### 停止集群

到部署集群的目录执行，确保该目录下 conf/hosts 文件内容和实际各组件的部署信息一致
```
sbin/stop-all.sh
```

### 启动集群
到部署集群的目录执行
```
sbin/start-all.sh
```
