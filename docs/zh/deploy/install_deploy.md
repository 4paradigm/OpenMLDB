# 安装部署

## 软硬件环境需求

* 操作系统：CentOS 7, Ubuntu 20.04, macOS >= 10.15。其中Linux glibc版本 >= 2.17。其他操作系统版本没有做完整的测试，不能保证完全正确运行。
* 内存：视数据量而定，推荐在 8 GB 及以上。
* CPU：
  * 目前仅支持 x86 架构，暂不支持例如 ARM 等架构。
  * 核数推荐不少于 4 核，如果 Linux 环境下 CPU 不支持 AVX2 指令集，需要从源码重新编译部署包。


## 部署包准备
本说明文档中默认使用预编译好的 OpenMLDB 部署包（[Linux](https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.3/openmldb-0.6.3-linux.tar.gz), [macOS](https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.3/openmldb-0.6.3-darwin.tar.gz)），所支持的操作系统要求为：CentOS 7, Ubuntu 20.04, macOS >= 10.15。如果用户期望自己编译（如做 OpenMLDB 源代码开发，操作系统或者 CPU 架构不在预编译部署包的支持列表内等原因），用户可以选择在 docker 容器内编译使用或者从源码编译，具体请参照我们的[编译文档](compile.md)。

## 配置环境(Linux)

### 关闭操作系统swap

查看当前系统swap是否关闭

```bash
$ free
              total        used        free      shared  buff/cache   available
Mem:      264011292    67445840     2230676     3269180   194334776   191204160
Swap:             0           0           0
```

如果swap一项全部为0表示已经关闭，否则运行下面命令关闭swap

```
$ swapoff -a
```

### 关闭THP(Transparent Huge Pages)

查看THP是否关闭

```
$ cat /sys/kernel/mm/transparent_hugepage/enabled
always [madvise] never
$ cat /sys/kernel/mm/transparent_hugepage/defrag
[always] madvise never
```

如果上面两个配置中"never"没有被方括号圈住就需要设置一下

```bash
$ echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled
$ echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag
```

查看是否设置成功，如果"never"被方括号圈住表明已经设置成功，如下所示：

```bash
$ cat /sys/kernel/mm/transparent_hugepage/enabled
always madvise [never]
$ cat /sys/kernel/mm/transparent_hugepage/defrag
always madvise [never]
```

### 时间和时区设置

OpenMLDB数据过期删除机制依赖于系统时钟, 如果系统时钟不正确会导致过期数据没有删掉或者删掉了没有过期的数据

```bash
$ date
Wed Aug 22 16:33:50 CST 2018
```
请确保时间是正确的

## 部署单机版
OpenMLDB单机版需要部署一个nameserver和一个tablet. nameserver用于表管理和元数据存储，tablet用于数据存储。APIServer是可选的，如果要用http的方式和OpenMLDB交互需要部署此模块

**注意:** 最好把不同的组件部署在不同的目录里，便于单独升级

### 部署tablet
#### 1 下载OpenMLDB部署包
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.3/openmldb-0.6.3-linux.tar.gz
tar -zxvf openmldb-0.6.3-linux.tar.gz
mv openmldb-0.6.3-linux openmldb-tablet-0.6.3
cd openmldb-tablet-0.6.3
```
#### 2 修改配置文件conf/standalone_tablet.flags
* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号
```
--endpoint=172.27.128.33:9527
```
**注意：**
* endpoint不能用0.0.0.0和127.0.0.1 
* 如果此处使用的域名, 所有使用openmldb的client所在的机器都得配上对应的host. 不然会访问不到
#### 3 启动服务
```
bash bin/start.sh start standalone_tablet
```
**注: 服务启动后会在bin目录下产生standalone_tablet.pid文件, 里边保存启动时的进程号。如果该文件内的pid正在运行则会启动失败**

### 部署nameserver
#### 1 下载OpenMLDB部署包
````
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.3/openmldb-0.6.3-linux.tar.gz
tar -zxvf openmldb-0.6.3-linux.tar.gz
mv openmldb-0.6.3-linux openmldb-ns-0.6.3
cd openmldb-ns-0.6.3
````
#### 2 修改配置文件conf/standalone_nameserver.flags
* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号
* tablet配置项需要配置上前面启动的tablet的地址
```
--endpoint=172.27.128.33:6527
--tablet=172.27.128.33:9527
```
**注: endpoint不能用0.0.0.0和127.0.0.1**
#### 3 启动服务
```
bash bin/start.sh start  standalone_nameserver
```
#### 4 检查服务是否启动
```bash
$ ./bin/openmldb --host=172.27.128.33 --port=6527
> show databases;
 -----------
  Databases
 -----------
0 row in set
```

### 部署apiserver

APIServer负责接收http请求，转发给OpenMLDB并返回结果。它是无状态的，而且并不是OpenMLDB必须部署的组件。
运行前需确保OpenMLDB cluster已经启动，否则APIServer将初始化失败并退出进程。

#### 1 下载OpenMLDB部署包

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.3/openmldb-0.6.3-linux.tar.gz
tar -zxvf openmldb-0.6.3-linux.tar.gz
mv openmldb-0.6.3-linux openmldb-apiserver-0.6.3
cd openmldb-apiserver-0.6.3
```

#### 2 修改配置文件conf/standalone_apiserver.flags

* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号
* 修改nameserver为nameserver的地址

```
--endpoint=172.27.128.33:8080
--nameserver=172.27.128.33:6527
```

**注意：**

* endpoint不能用0.0.0.0和127.0.0.1。也可以选择不设置`--endpoint`，而只配置端口号 `--port`。

#### 3 启动服务

```
bash bin/start.sh start standalone_apiserver
```

## 部署集群版
OpenMLDB集群版需要部署zookeeper、nameserver、tablet等模块。其中zookeeper用于服务发现和保存元数据信息。nameserver用于管理tablet，实现高可用和failover。tablet用于存储数据和主从同步数据。APIServer是可选的，如果要用http的方式和OpenMLDB交互需要部署此模块

**注意:** 最好把不同的组件部署在不同的目录里，便于单独升级。如果在同一台机器部署多个tablet也需要部署在不同的目录里

### 部署zookeeper
建议部署3.4.14版本。如果已有可用zookeeper集群可略过此步骤

#### 1. 下载zookeeper安装包
```
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
tar -zxvf zookeeper-3.4.14.tar.gz
cd zookeeper-3.4.14
cp conf/zoo_sample.cfg conf/zoo.cfg
```

#### 2. 修改配置文件
打开文件`conf/zoo.cfg`修改`dataDir`和`clientPort`
```
dataDir=./data
clientPort=7181
```

#### 3. 启动Zookeeper
```
bash bin/zkServer.sh start
```
部署zookeeper集群[参考这里](https://zookeeper.apache.org/doc/r3.4.14/zookeeperStarted.html#sc_RunningReplicatedZooKeeper)


### 部署tablet
#### 1 下载OpenMLDB部署包
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.3/openmldb-0.6.3-linux.tar.gz
tar -zxvf openmldb-0.6.3-linux.tar.gz
mv openmldb-0.6.3-linux openmldb-tablet-0.6.3
cd openmldb-tablet-0.6.3
```
#### 2 修改配置文件conf/tablet.flags
* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号
* 修改zk_cluster为已经启动的zk集群地址
* 如果和其他OpenMLDB共用zk需要修改zk_root_path
```
--endpoint=172.27.128.33:9527
--role=tablet

# if tablet run as cluster mode zk_cluster and zk_root_path should be set
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
```
**注意：**
* endpoint不能用0.0.0.0和127.0.0.1 
* 如果此处使用的域名, 所有使用openmldb的client所在的机器都得配上对应的host. 不然会访问不到
* zk_cluster和zk_root_path配置和nameserver的保持一致
#### 3 启动服务
```
bash bin/start.sh start tablet
```
重复以上步骤部署多个tablet

**注意:**
* 服务启动后会在bin目录下产生tablet.pid文件, 里边保存启动时的进程号。如果该文件内的pid正在运行则会启动失败
* 集群版至少需要部署2个tablet
* 如果需要部署多个tablet，把所有tablet部署完再部署nameserver

### 部署nameserver
#### 1 下载OpenMLDB部署包
````
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.3/openmldb-0.6.3-linux.tar.gz
tar -zxvf openmldb-0.6.3-linux.tar.gz
mv openmldb-0.6.3-linux openmldb-ns-0.6.3
cd openmldb-ns-0.6.3
````
#### 2 修改配置文件conf/nameserver.flags
* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号
* 修改zk_cluster为已经启动的zk集群地址. ip为zk所在机器的ip, port为zk配置文件中clientPort配置的端口号. 如果zk是集群模式用逗号分割, 格式为ip1:port1,ip2:port2,ip3:port3
* 如果和其他OpenMLDB共用zk需要修改zk_root_path
```
--endpoint=172.27.128.31:6527
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
```
**注: endpoint不能用0.0.0.0和127.0.0.1**
#### 3 启动服务
```
bash bin/start.sh start nameserver
```
重复上述步骤部署多个nameserver

#### 4 检查服务是否启动
```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:7181,172.27.128.32:7181,172.27.128.33:7181 --zk_root_path=/openmldb_cluster --role=ns_client
> showns
  endpoint            role
-----------------------------
  172.27.128.31:6527  leader
```


### 部署 APIServer

APIServer负责接收http请求，转发给OpenMLDB并返回结果。它是无状态的，而且并不是OpenMLDB必须部署的组件。
运行前需确保OpenMLDB cluster已经启动，否则APIServer将初始化失败并退出进程。

#### 1 下载OpenMLDB部署包

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.3/openmldb-0.6.3-linux.tar.gz
tar -zxvf openmldb-0.6.3-linux.tar.gz
mv openmldb-0.6.3-linux openmldb-apiserver-0.6.3
cd openmldb-apiserver-0.6.3
```

#### 2 修改配置文件conf/apiserver.flags

* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号
* 修改zk_cluster为需要转发到的OpenMLDB的zk集群地址

```
--endpoint=172.27.128.33:8080
--role=apiserver
--zk_cluster=172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181
--zk_root_path=/openmldb_cluster
--openmldb_log_dir=./logs
```

**注意：**

* endpoint不能用0.0.0.0和127.0.0.1。也可以选择不设置`--endpoint`，而只配置端口号 `--port`。
* 还可自行配置APIServer的线程数，`--thread_pool_size`，默认为16。

#### 3 启动服务

```
bash bin/start.sh start apiserver
```

**注**: 如果在linux平台通过发布包启动nameserver/tablet/apiserver时core掉，很可能时指令集不兼容问题，需要通过源码编译openmldb。源码编译参考[这里](./compile.md), 需要采用方式三完整源代码编译。

### 部署TaskManager

#### 1 下载 OpenMLDB 部署包和面向特征工程优化的 Spark 发行版
````
wget https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.6.3/spark-3.2.1-bin-openmldbspark.tgz 
tar -zxvf spark-3.2.1-bin-openmldbspark.tgz 
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.3/openmldb-0.6.3-linux.tar.gz
tar -zxvf openmldb-0.6.3-linux.tar.gz
mv openmldb-0.6.3-linux openmldb-taskmanager-0.6.3
cd openmldb-taskmanager-0.6.3
````
#### 2 修改配置文件conf/taskmanager.properties

* 修改server.host。host是部署机器的ip/域名。
* 修改server.port。port是部署机器的端口号。
* 修改zk_cluster为已经启动的zk集群地址。ip为zk所在机器的ip, port为zk配置文件中clientPort配置的端口号. 如果zk是集群模式用逗号分割, 格式为ip1:port1,ip2:port2,ip3:port3。
* 如果和其他OpenMLDB共用zk需要修改zookeeper.root_path。
* 修改batchjob.jar.path为BatchJob Jar文件路径，如果设置为空会到上一级lib目录下寻找。如果使用Yarn模式需要修改为对应HDFS路径。
* 修改offline.data.prefix为离线表存储路径，如果使用Yarn模式需要修改为对应HDFS路径。
* 修改spark.master为离线任务运行模式，目前支持local和yarn模式。
* 修改spark.home为Spark环境路径，如果不配置或配置为空则使用SPARK_HOME环境变量的配置。需要设置为第一步解压出来spark优化版包的目录，路径为绝对路径

```
server.host=0.0.0.0
server.port=9902
zookeeper.cluster=172.27.128.31:7181,172.27.128.32:7181,172.27.128.33:7181
zookeeper.root_path=/openmldb_cluster
batchjob.jar.path=
offline.data.prefix=file:///tmp/openmldb_offline_storage/
spark.master=local
spark.home=
```

#### 3 启动服务
```
bash bin/start.sh start taskmanager
```
#### 4 检查服务是否启动
```bash
$ ./bin/openmldb --zk_cluster=172.27.128.31:7181,172.27.128.32:7181,172.27.128.33:7181 --zk_root_path=/openmldb_cluster --role=sql_client
> show jobs;
---- ---------- ------- ------------ ---------- ----------- --------- ---------------- -------
 id   job_type   state   start_time   end_time   parameter   cluster   application_id   error
---- ---------- ------- ------------ ---------- ----------- --------- ---------------- -------
```
