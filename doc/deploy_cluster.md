# rtidb 集群部署文档

## 部署zookeeper

* 修改zk目录中conf/zoo.cfg的clientPort和dataDir
* 在conf/zoo.cfg添加server配置. 格式为server.id=ip:port1:port2, 假如部署三个节点就添加三行:  
  server.1=172.27.128.31:2881:3881  
  server.2=172.27.128.32:2882:3882  
  server.3=172.27.128.33:2883:3883  
  **注: port1和port2不能和对应ip机器已占用端口冲突**  
* 在配置的dataDir路径里创建myid文件  
  如果本实例对应的是server.1, dataDir配置的为./data, 运行echo 1 > ./data/myid  
  同理如果是对应的是server.2, 则运行echo 2 > ./data/myid  
* 分别在各节点启动zk ./bin/zkServer.sh start
* 查看zk节点运行角色 ./bin/zkServer.sh status  
  显示Mode: leader或者Mode: follower说明zk集群启动成功  
* 检查zk状态  
  1 用客户端连到zk集群 ./bin/zkCli.sh -server 172.27.128.31:7181,172.27.128.32:7181,172.27.128.33:7181  
  2 运行命令 ls /  
  3 输出[zookeeper]  
  说明zk集群运行正常  

### sample 配置

```
# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
# do not use /tmp for storage, /tmp here is just
# example sakes.
dataDir=datadir
# the port at which the clients will connect
clientPort=7181
# the maximum number of client connections.
# increase this if you need to handle more clients
maxClientCnxns=60
#
# Be sure to read the maintenance section of the
# administrator guide before turning on autopurge.
#
# http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
#
# The number of snapshots to retain in dataDir
#autopurge.snapRetainCount=3
# Purge task interval in hours
# Set to "0" to disable auto purge feature
#autopurge.purgeInterval=1
server.1=172.27.128.31:2881:3881
server.2=172.27.128.32:2882:3882
server.3=172.27.128.33:2883:3883
```


## 部署nameserver

* 在配置或者环境变量中指定endpoint, zk_cluster和zk_root_path
* 如果要开启自动failover和自动恢复, 设置auto_failover和auto_recover_table为true
* 启动nameserver: sh ./bin/start_ns.sh


## 部署tablet

* 在配置或者环境变量中指定endpoint, zk_cluster和zk_root_path. (注: zk_cluster和zk_root_path要与nameserver配置的一样)
* 启动tablet: sh ./bin/start.sh

## 部署metricbeat
* 在配置或者环境变量中指定endpoint
* 在conf下的metricbeat.yml中指定所运行模块的路径（目前仅有brpc，所以不需要改动）
* 在metricbeat.yml中的hosts项中配置正确的es地址
* 启动metricbeat: sh ./bin/start_metricbeat.sh


## 部署filebeat
* 在conf下的filebeat.yml中指定运行模块的路径（目前仅有rtidb，不需要改动）
* 在filebeat.yml中的hosts项中配置正确的es地址
* 在conf/module/rtidb/tablet/config中的rtidb-accesslog.yml中配置正确的需要收集的日志（目前默认为当前目录下logs/tablet*), 修改pod_name的值为当前机器的hostname或者ip(默认是docker02)
* 在conf/module/rtidb/nameserver/config中的rtidb-accesslog.yml中配置正确的需要收集的日志（目前默认为当前目录下logs/nameserver*), 修改pod_name的值为当前机器的hostname或者ip(默认是docker02)
* 启动filebeat: sh ./bin/start_filebeat.sh
