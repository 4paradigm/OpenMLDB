# rtidb 集群部署文档

## 部署zookeeper

* 修改zk目录中conf/zoo.cfg的ip和clientPort
* 启动zk ./bin/zkServer.sh start

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
server.1=172.27.128.31:2888:3888
server.2=172.27.128.32:2888:3888
server.3=172.27.128.33:2888:3888
```


## 部署nameserver

* 在配置或者环境变量中指定endpoint, zk_cluster和zk_root_path
* 如果要开启自动failover和自动恢复, 设置auto_failover和auto_recover_table为true
* 启动nameserver: sh bin/start_ns.sh &


## 部署tablet

* 在配置或者环境变量中指定endpoint, zk_cluster和zk_root_path. (注: zk_cluster和zk_root_path要与nameserver配置的一样)
* 启动tablet: sh bin/start.sh &
* 启动日志监控程序: sh boot_monitor.sh &


