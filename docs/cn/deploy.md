# 部署OpenMLDB

## 部署Zookeeper
建议部署3.4.14版本  
如果已有可用Zookeeper集群可略过此步骤

### 1. 下载Zookeeper安装包
```
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
tar -zxvf zookeeper-3.4.14.tar.gz
cd zookeeper-3.4.14
cp conf/zoo_sample.cfg conf/zoo.cfg
```

### 2. 修改配置文件
打开文件`conf/zoo.cfg`修改`dataDir`和`clientPort`
```
dataDir=./data
clientPort=6181
```

### 3. 启动Zookeeper
```
sh bin/zkServer.sh start
```
部署Zookeeper集群[参考这里](https://zookeeper.apache.org/doc/r3.4.14/zookeeperStarted.html)

## 部署`nameserver`
### 1. 下载OpenMLDB部署包
````
wget https://github.com/4paradigm/OpenMLDB/releases/download/0.2.2/openmldb-0.2.2-linux.tar.gz
tar -zxvf openmldb-0.2.2-linux.tar.gz
mv openmldb-0.2.2-linux openmldb-ns-0.2.2
cd openmldb-ns-0.2.2
````

### 2. 修改配置文件`conf/nameserver.flags`
* 修改`endpoint`
* 修改`zk_cluster`为已经启动的zk集群地址. ip为zk所在机器的ip, port为zk配置文件中clientPort配置的端口号. 如果zk是集群模式用逗号分割, 格式为`ip1:port1,ip2:port2,ip3:port3`
* 如果和其他OpenMLDB共用zk需要修改`zk_root_path`
```
--endpoint=172.27.128.31:6527
--role=nameserver
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
--enable_distsql=true
```

**注: `endpoint`不能用`0.0.0.0`和`127.0.0.1`**

### 3. 启动服务
```
sh bin/start.sh start nameserver
```

## 部署tablet
### 1. 下载OpenMLDB部署包
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/0.2.2/openmldb-0.2.2-linux.tar.gz
tar -zxvf openmldb-0.2.2-linux.tar.gz
mv openmldb-0.2.2-linux openmldb-tablet-2.2.0
cd openmldb-tablet-2.2.0
```

### 2. 修改配置文件`conf/tablet.flags`
* 修改`endpoint`
* 修改`zk_cluster`为已经启动的zk集群地址
* 如果和其他OpenMLDB共用zk需要修改`zk_root_path`
```
--endpoint=172.27.128.33:9527
--role=tablet

# if tablet run as cluster mode zk_cluster and zk_root_path should be set
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
--enable_distsql=true
```

**注意：**
* `endpoint`不能用`0.0.0.0`和`127.0.0.1`
* 如果此处使用的域名, 所有使用openmldb的client所在的机器都得配上对应的host. 不然会访问不到
* `zk_cluster`和`zk_root_path`配置和`nameserver`的保持一致

### 3. 启动服务
```
sh bin/start.sh start tablet
```
**注: 服务启动后会在bin目录下产生`tablet.pid`文件, 里边保存启动时的进程号。如果该文件内的pid正在运行则会启动失败**

重复以上步骤部署多个`nameserver`和`tablet`

## 部署`APIServer`

`APIServer`负责接收http请求，转发给OpenMLDB并返回结果。它是无状态的，而且并不是OpenMLDB必须部署的组件。
运行前需确保OpenMLDB cluster已经启动，否则`APIServer`将初始化失败并退出进程。

### 1. 下载OpenMLDB部署包

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/0.2.2/openmldb-0.2.2-linux.tar.gzz
tar -zxvf openmldb-0.2.2-linux.tar.gz
mv openmldb-0.2.2-linux openmldb-apiserver-0.2.2
cd openmldb-apiserver-0.2.2
```

### 2. 修改配置文件`conf/apiserver.flags`

* 修改`endpoint`
* 修改`zk_cluster`为需要转发到的OpenMLDB的zk集群地址

```
./bin/openmldb --endpoint=172.27.128.33:8080
--role=apiserver
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
--openmldb_log_dir=./logs
```

**注意：**

* `endpoint`不能用`0.0.0.0`和`127.0.0.1`。也可以选择不设置`--endpoint`，而只配置端口号 `--port`。
* 还可自行配置APIServer的线程数，`--thread_pool_size`，默认为16。

### 3. 启动服务

```
sh bin/start.sh start apiserver
```
