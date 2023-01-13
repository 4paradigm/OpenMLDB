# 安装部署

## 软硬件环境需求

* 操作系统：CentOS 7, Ubuntu 20.04, macOS >= 10.15。其中Linux glibc版本 >= 2.17。其他操作系统版本没有做完整的测试，不能保证完全正确运行。
* 内存：视数据量而定，推荐在 8 GB 及以上。
* CPU：
  * 目前仅支持 x86 架构，暂不支持例如 ARM 等架构。
  * 核数推荐不少于 4 核，如果 Linux 环境下 CPU 不支持 AVX2 指令集，需要从源码重新编译部署包。

* 运行环境：zookeeper和taskmanager部署需要java runtime environment。其他组件无要求。

## 部署包准备
本说明文档中默认使用预编译好的 OpenMLDB 部署包（[Linux](https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.1/openmldb-0.7.1-linux.tar.gz), [macOS](https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.1/openmldb-0.7.1-darwin.tar.gz)），所支持的操作系统要求为：CentOS 7, Ubuntu 20.04, macOS >= 10.15。如果用户期望自己编译（如做 OpenMLDB 源代码开发，操作系统或者 CPU 架构不在预编译部署包的支持列表内等原因），用户可以选择在 docker 容器内编译使用或者从源码编译，具体请参照我们的[编译文档](compile.md)。

## 配置环境(Linux)

### 配置coredump文件大小限制和最多文件打开数目
```bash
ulimit -c unlimited
ulimit -n 655360
```

通过`ulimit`命令配置的参数，只对当前session有效，如果希望持久化配置，需要在`/etc/security/limits.conf`添加如下配置：
```bash
*       soft    core    unlimited
*       hard    core    unlimited
*       soft    nofile  655360
*       hard    nofile  655360
```

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
swapoff -a
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
echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled
echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag
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

## 预备测试

由于linux平台的多样性，发布包可能在你的机器上不兼容，请先通过简单的运行测试。
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.1/openmldb-0.7.1-linux.tar.gz
tar -zxvf openmldb-0.7.1-linux.tar.gz
./openmldb-0.7.1-linux/bin/openmldb --version
```
结果应显示该程序的版本号，类似
```
openmldb version 0.6.3-xxxx
Debug build (NDEBUG not #defined)
```

如果产生coredump，很可能时指令集不兼容问题，需要通过源码编译openmldb。源码编译参考[这里](./compile.md), 需要采用方式三完整源代码编译。

## 守护进程启动方式

下文均使用常规后台进程模式启动组件，如果想要使守护进程模式启动组件，请使用`bash bin/start.sh start <component> mon`或者`sbin/start-all.sh mon`的方式启动。守护进程模式中，`bin/<component>.pid`将是mon进程的pid，`bin/<component>.pid.child`为组件真实的pid。mon进程并不是系统服务，如果mon进程意外退出，将无法继续守护。

## 部署单机版
OpenMLDB单机版需要部署一个nameserver和一个tablet。nameserver用于表管理和元数据存储，tablet用于数据存储。APIServer是可选的，如果要用http的方式和OpenMLDB交互需要部署此模块

### 下载OpenMLDB部署包
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.1/openmldb-0.7.1-linux.tar.gz
tar -zxvf openmldb-0.7.1-linux.tar.gz
cd openmldb-0.7.1-linux
```

### 配置
如果只是本机访问，可以跳过此步。

#### 1 配置tablet: conf/standalone_tablet.flags
* 修改endpoint，endpoint是用冒号分隔的部署机器ip/域名和端口号。
```
--endpoint=172.27.128.33:9527
```
**注意：**
* endpoint不能用0.0.0.0和127.0.0.1 
* 如果此处使用的域名, 所有使用openmldb的client所在的机器都得配上对应的host. 不然会访问不到

#### 2 配置nameserver：conf/standalone_nameserver.flags。
* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号
* tablet配置项需要配置上前面启动的tablet的地址
```
--endpoint=172.27.128.33:6527
--tablet=172.27.128.33:9527
```
**注: endpoint不能用0.0.0.0和127.0.0.1**

#### 3 配置apiserver：conf/standalone_apiserver.flags

APIServer负责接收http请求，转发给OpenMLDB并返回结果。它是无状态的，而且并不是OpenMLDB必须部署的组件。
运行前需确保OpenMLDB其他服务已经启动，否则APIServer将初始化失败并退出进程。

* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号
* 修改nameserver为nameserver的地址

```
--endpoint=172.27.128.33:8080
--nameserver=172.27.128.33:6527
```

**注意：**

* endpoint不能用0.0.0.0和127.0.0.1。也可以选择不设置`--endpoint`，而只配置端口号 `--port`。

### 启动standalone所有服务
请确保`conf/openmldb-env.sh`里面`OPENMLDB_MODE`为`standalone`（默认）

```
sbin/start-all.sh
```
 
服务启动后会在bin目录下产生standalone_tablet.pid, standalone_nameserver.pid, standalone_apiserver.pid文件, 里边保存启动时的进程号。如果该文件内的pid正在运行则会启动失败。


## 部署集群版（一键部署）
OpenMLDB集群版需要部署zookeeper、nameserver、tablet、taskmanager等模块。其中zookeeper用于服务发现和保存元数据信息。nameserver用于管理tablet，实现高可用和failover。tablet用于存储数据和主从同步数据。APIServer是可选的，如果要用http的方式和OpenMLDB交互需要部署此模块。taskmanager用于管理离线job。
我们提供了一键部署脚本，可以简化手动在每台机器上下载和配置的复杂性。

**注意:** 同一台机器部署多个组件时，一定要部署在不同的目录里，便于单独管理。尤其是部署tablet server，一定不能重复使用目录，避免数据文件和日志文件冲突。

环境要求：
- 部署机器（执行部署脚本的机器）可以免密登录其他部署节点
- 部署机器安装`rsync`工具

### 下载OpenMLDB发行版

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.1/openmldb-0.7.1-linux.tar.gz
tar -zxvf openmldb-0.7.1-linux.tar.gz
cd openmldb-0.7.1-linux
```

### 配置
#### 环境配置
环境变量定义在`conf/openmldb-env.sh`，如下表所示：

| 环境变量                              | 默认值                                | 定义                                                                      |
|-----------------------------------|------------------------------------|-------------------------------------------------------------------------|
| OPENMLDB_MODE                     | standalone                         | standalone或者cluster                                                     |
| OPENMLDB_HOME                     | 当前发行版的根目录                          | openmldb发行版根目录                                                          |
| SPARK_HOME                        | $OPENMLDB_HOME/spark               | openmldb spark发行版根目录，如果该目录不存在，自动从网上下载                                   |
| OPENMLDB_TABLET_PORT              | 10921                              | tablet默认端口                                                              |
| OPENMLDB_NAMESERVER_PORT          | 7527                               | nameserver默认端口                                                          |
| OPENMLDB_TASKMANAGER_PORT         | 9902                               | taskmanager默认端口                                                         |
| OPENMLDB_APISERVER_PORT           | 9080                               | apiserver默认端口                                                           |
| OPENMLDB_USE_EXISTING_ZK_CLUSTER  | false                              | 是否使用已经部署的zookeeper集群。如果是`false`，会在部署脚本里自动启动zookeeper集群                  |
| OPENMLDB_ZK_HOME                  | $OPENMLDB_HOME/zookeeper           | zookeeper发行版根目录                                                         |
| OPENMLDB_ZK_CLUSTER               | 自动从`conf/hosts`中的`[zookeeper]`配置获取 | zookeeper集群地址                                                           |
| OPENMLDB_ZK_ROOT_PATH             | /openmldb                          | OpenMLDB在zookeeper的根目录                                                  |
| OPENMLDB_ZK_CLUSTER_CLIENT_PORT   | 2181                               | zookeeper client port, 即zoo.cfg里面的clientPort                            |
| OPENMLDB_ZK_CLUSTER_PEER_PORT     | 2888                               | zookeeper peer port，即zoo.cfg里面这种配置server.1=zoo1:2888:3888中的第一个端口配置      |
| OPENMLDB_ZK_CLUSTER_ELECTION_PORT | 3888                               | zookeeper election port, 即zoo.cfg里面这种配置server.1=zoo1:2888:3888中的第二个端口配置 |

#### 部署节点配置
节点配置文件为`conf/hosts`，示例如下：
```bash
[tablet]
node1:10921 /tmp/openmldb/tablet
node2:10922 /tmp/openmldb/tablet

[nameserver]
node3:7527

[apiserver]
node3:9080

[zookeeper]
node3:2181:2888:3888 /tmp/openmldb/zk-1
```

配置文件分为四个区域，以`[]`来识别：

- `[tablet]`：配置部署tablet的节点列表
- `[nameserver]`：配置部署nameserver的节点列表
- `[apiserver]`：配置部署apiserver的节点列表
- `[zookeeper]`：配置部署zookeeper的节点列表

每个区域的节点列表，每一行代表一个节点，每行格式为`host:port WORKDIR`。
对于`[zookeeper]`,
会有额外端口参数，包括follower用来连接leader的`zk_peer_port`和用于leader选择的`zk_election_port`，
其格式为`host:port:zk_peer_port:zk_election_port WORKDIR`。

每一行节点列表，除了`host`是必须的，其他均为可选，如果没有提供，会使用默认配置，默认配置参考`conf/openmldb-env.sh`。

### 部署
```bash
sbin/deploy-all.sh
```
该脚本会把相关的文件分发到`conf/hosts`里面配置的机器上，同时根据`conf/hosts`和`conf/openmldb-env.sh`
的配置，对相关组件的配置做出相应的更新。

如果希望为每个节点添加一些额外的相同的定制化配置，可以在执行deploy脚本之前，修改`conf/xx.template`的配置，
这样在分发配置文件的时候，每个节点都可以用到更改后的配置。
重复执行`sbin/deploy-all.sh`会覆盖上一次的配置。

### 启动服务
```bash
sbin/start-all.sh
```
该脚本会把`conf/hosts`里面配置的所有服务启动起来。

可以通过`sbin/openmldb-cli.sh`连接集群，验证集群是否正常启动。

### 停止服务
如果需要停止所有服务，可以执行以下脚本：
```bash
sbin/stop-all.sh
```


## 部署集群版（手动部署）
OpenMLDB集群版需要部署zookeeper、nameserver、tablet、taskmanager等模块。其中zookeeper用于服务发现和保存元数据信息。nameserver用于管理tablet，实现高可用和failover。tablet用于存储数据和主从同步数据。APIServer是可选的，如果要用http的方式和OpenMLDB交互需要部署此模块。taskmanager用于管理离线job。

**注意:** 同一台机器部署多个组件时，一定要部署在不同的目录里，便于单独管理。尤其是部署tablet server，一定不能重复使用目录，避免数据文件和日志文件冲突。

### 部署zookeeper
zookeeper 要求版本在 3.4 到 3.6 之间, 建议部署3.4.14版本。如果已有可用zookeeper集群可略过此步骤。如果想要部署zookeeper集群，参考[这里](https://zookeeper.apache.org/doc/r3.4.14/zookeeperStarted.html#sc_RunningReplicatedZooKeeper)。本步骤只演示部署standalone zookeeper。

#### 1. 下载zookeeper安装包
```
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
tar -zxvf zookeeper-3.4.14.tar.gz
cd zookeeper-3.4.14
cp conf/zoo_sample.cfg conf/zoo.cfg
```

#### 2. 修改配置文件
打开文件`conf/zoo.cfg`修改`dataDir`和`clientPort`。
```
dataDir=./data
clientPort=7181
```

#### 3. 启动Zookeeper
```
bash bin/zkServer.sh start
```

启动成功将如下图所示，有`STARTED`提示。
![zk started](images/zk_started.png)

通过`ps f|grep zoo.cfg`也可以看到zookeeper进程正在运行。

![zk ps](images/zk_ps.png)

```{attention}
如果发现zookeeper进程启动失败，请查看当前目录的zookeeper.out日志。
```

#### 4. 记录zookeeper服务地址与连接测试

后续tablet、nameserver与taskmanager连接zookeeper，均需要配置这个zookeeper服务地址。跨主机访问zookeeper服务需要使用公网IP（这里我们假设为`172.27.128.33`，实际请获得你的zookeeper部署机器IP），又由第二步填写的`clientPort`，可知zookeeper服务地址为`172.27.128.33:7181`。

你可以使用`zookeeper-3.4.14/bin/zkCli.sh`来进行连接zookeeper的测试，仍在`zookeeper-3.4.14`目录中运行
```
bash bin/zkCli.sh -server 172.27.128.33:7181
```
可以进入zk客户端程序，如下图所示，有`CONNECTED`提示。

![zk cli](images/zk_cli.png)

输入`quit`回车或`Ctrl+C`退出zk客户端。

### 部署tablet（至少两台）
#### 1 下载OpenMLDB部署包
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.1/openmldb-0.7.1-linux.tar.gz
tar -zxvf openmldb-0.7.1-linux.tar.gz
mv openmldb-0.7.1-linux openmldb-tablet-0.7.1
cd openmldb-tablet-0.7.1
```
#### 2 修改配置文件`conf/tablet.flags`
```bash
# 可以在示例配置文件的基础上，进行修改
cp conf/tablet.flags.template conf/tablet.flags
```
```{attention}
注意，配置文件是`conf/tablet.flags`，不是其他配置文件。启动多台tablet时（多tablet目录应该独立，不可共享），依然是修改该配置文件。
```
* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号（endpoint不能用0.0.0.0和127.0.0.1，必须是公网IP）。
* 修改zk_cluster为已经启动的zk服务地址(见[zookeeper启动步骤](#4-记录zookeeper服务地址与连接测试))。如果zk服务是集群，可用逗号分隔，例如，`172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181`。
* 修改zk_root_path，本例中使用`/openmldb_cluster`。注意，**同一个集群下的组件`zk_root_path`是相同的**。所以本次部署中，各个组件配置的`zk_root_path`都为`/openmldb_cluster`。
```
--endpoint=172.27.128.33:9527
--role=tablet

# if tablet run as cluster mode zk_cluster and zk_root_path should be set
--zk_cluster=172.27.128.33:7181
--zk_root_path=/openmldb_cluster
```
**注意：**
* 如果endpoint配置项使用的是域名, 所有使用openmldb client的机器都得配上对应的host. 不然会访问不到

#### 3 启动服务
```
bash bin/start.sh start tablet
```
启动后应有`success`提示，如下所示。
```
Starting tablet ...
Start tablet success
```

通过`ps f | grep tablet`查看进程状态。
![tablet ps](images/tablet_ps.png)

通过`curl http://<tablet_ip>:<port>/status`也可以测试tablet是否运行正常。

```{attention}
如果发现tablet启动失败，或进程运行一阵子后就退出了，可以检查该tablet启动目录下的`logs/tablet.WARNING`，更详细可以检查`logs/tablet.INFO`。

如果是ip地址已被使用，请自行更改tablet的endpoint端口，再次启动。

启动前请保证启动目录中没有`db  logs  recycle`三个目录，`rm -rf db logs recycle`，以防止遗留的文件与日志对当前的判断造成干扰。

如果无法解决，请联系技术人员，提供日志。

```
#### 重复以上步骤部署多个tablet
```{warning}
集群版的tablet数量必须2台及以上，如果只有1台tablet，启动nameserver将会failed。nameserver的日志(logs/nameserver.WARNING)中会包含"is less then system table replica num"的日志。
```

在另一台机器启动下一个tablet只需在该机器上重复以上步骤。如果是在同一个机器上启动下一个tablet，请保证是在另一个目录中，不要重复使用已经启动过tablet的目录。

比如，可以再次解压压缩包（不要cp已经启动过tablet的目录，启动后的生成文件会造成影响），并命名目录为`openmldb-tablet-0.7.1-2`。

```
tar -zxvf openmldb-0.7.1-linux.tar.gz
mv openmldb-0.7.1-linux openmldb-tablet-0.7.1-2
cd openmldb-tablet-0.7.1-2
```

再修改配置并启动。注意，tablet如果都在同一台机器上，请使用不同端口号，否则日志(logs/tablet.WARNING)中将会有"Fail to listen"信息。

**注意:**
* 服务启动后会在bin目录下产生tablet.pid文件, 里边保存启动时的进程号。如果该文件内的pid正在运行则会启动失败

### 部署nameserver
```{attention}
请保证所有tablet已经启动成功，再部署nameserver，不能更改部署顺序。
```
#### 1 下载OpenMLDB部署包
````
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.1/openmldb-0.7.1-linux.tar.gz
tar -zxvf openmldb-0.7.1-linux.tar.gz
mv openmldb-0.7.1-linux openmldb-ns-0.7.1
cd openmldb-ns-0.7.1
````
#### 2 修改配置文件conf/nameserver.flags
```bash
# 可以在示例配置文件的基础上，进行修改
cp conf/nameserver.flags.template conf/nameserver.flags
```
```{attention}
注意，配置文件是`conf/nameserver.flags`，不是其他配置文件。启动多台nameservert时（多nameserver目录应该独立，不可共享），依然是修改该配置文件。
```
* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号（endpoint不能用0.0.0.0和127.0.0.1，必须是公网IP）。
* 修改zk_cluster为已经启动的zk服务地址(见[zookeeper启动步骤](#4-记录zookeeper服务地址与连接测试))。如果zk服务是集群，可用逗号分隔，例如，`172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181`。
* 修改zk_root_path，本例中使用`/openmldb_cluster`。注意，**同一个集群下的组件`zk_root_path`是相同的**。所以本次部署中，各个组件配置的`zk_root_path`都为`/openmldb_cluster`。
```
--endpoint=172.27.128.31:6527
--zk_cluster=172.27.128.33:7181
--zk_root_path=/openmldb_cluster
```

#### 3 启动服务
```
bash bin/start.sh start nameserver
```

启动后应有`success`提示，如下所示。
```
Starting nameserver ...
Start nameserver success
```

同样可以通过`curl http://<ns_ip>:<port>/status`检测nameserver是否正常运行。

#### 重复上述步骤部署多个nameserver

nameserver 可以只存在一台，如果你需要高可用性，可以部署多 nameserver。

在另一台机器启动下一个 nameserver 只需在该机器上重复以上步骤。如果是在同一个机器上启动下一个 nameserver，请保证是在另一个目录中，不要重复使用已经启动过 namserver 的目录。

比如，可以再次解压压缩包（不要cp已经启动过 namserver 的目录，启动后的生成文件会造成影响），并命名目录为`openmldb-ns-0.7.1-2`。

```
tar -zxvf openmldb-0.7.1-linux.tar.gz
mv openmldb-0.7.1-linux openmldb-ns-0.7.1-2
cd openmldb-ns-0.7.1-2
```
然后再修改配置并启动。

**注意:**
* 服务启动后会在bin目录下产生`namserver.pid`文件, 里边保存启动时的进程号。如果该文件内的pid正在运行则会启动失败
* 请把所有 tablet 部署完再部署 nameserver

#### 4 检查服务是否启动

```{attention}
必须部署了至少一个nameserver，才可以使用以下方式查询到**当前**集群已启动的服务组件。
```

```bash
echo "show components;" | ./bin/openmldb --zk_cluster=172.27.128.33:7181 --zk_root_path=/openmldb_cluster --role=sql_client
```

结果**类似**下图，可以看到你已经部署好了的所有tablet和nameserver。
```
 ------------------- ------------ --------------- -------- ---------
  Endpoint            Role         Connect_time    Status   Ns_role
 ------------------- ------------ --------------- -------- ---------
  172.27.128.33:9527  tablet       1665568158749   online   NULL
  172.27.128.33:9528  tablet       1665568158741   online   NULL
  172.27.128.31:6527  nameserver   1665568159782   online   master
 ------------------- ------------ --------------- -------- ---------
```

### 部署 APIServer

APIServer负责接收http请求，转发给OpenMLDB集群并返回结果。它是无状态的。APIServer并不是OpenMLDB必须部署的组件，如果不需要使用http接口，可以跳过本步骤，进入下一步[部署taskmanager](#部署taskmanager)。

运行前需确保OpenMLDB集群的tablet和nameserver进程已经启动（taskmanager不影响APIServer的启动），否则APIServer将初始化失败并退出进程。

#### 1 下载OpenMLDB部署包

```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.1/openmldb-0.7.1-linux.tar.gz
tar -zxvf openmldb-0.7.1-linux.tar.gz
mv openmldb-0.7.1-linux openmldb-apiserver-0.7.1
cd openmldb-apiserver-0.7.1
```

#### 2 修改配置文件conf/apiserver.flags
```bash
# 可以在示例配置文件的基础上，进行修改
cp conf/apiserver.flags.template conf/apiserver.flags
```

* 修改endpoint。endpoint是用冒号分隔的部署机器ip/域名和端口号（endpoint不能用0.0.0.0和127.0.0.1，必须是公网IP）。
* 修改zk_cluster为已经启动的zk服务地址(见[zookeeper启动步骤](#4-记录zookeeper服务地址与连接测试))。如果zk服务是集群，可用逗号分隔，例如，`172.27.128.33:7181,172.27.128.32:7181,172.27.128.31:7181`。
* 修改zk_root_path，本例中使用`/openmldb_cluster`。注意，**同一个集群下的组件`zk_root_path`是相同的**。所以本次部署中，各个组件配置的`zk_root_path`都为`/openmldb_cluster`。

```
--endpoint=172.27.128.33:8080
--zk_cluster=172.27.128.33:7181
--zk_root_path=/openmldb_cluster
```

**注意：**

* 如果http请求并发度较大，可自行调大APIServer的线程数，`--thread_pool_size`，默认为16，重启生效。

#### 3 启动服务

```
bash bin/start.sh start apiserver
```

启动后应有`success`提示，如下所示。
```
Starting apiserver ...
Start apiserver success
```

```{attention}
APIServer是非必需组件，所以不会出现在`show components;`中。
```

可以通过`curl http://<apiserver_ip>:<port>/status`检测 APIServer 是否正常运行，更推荐通过执行sql的方式来测试是否正常：
```
curl http://<apiserver_ip>:<port>/dbs/foo -X POST -d'{"mode":"online","sql":"show components;"}'
```
结果中应该有已启动的所有tablet和nameserver的信息。

### 部署TaskManager

TaskManager 可以只存在一台，如果你需要高可用性，可以部署多 TaskManager ，需要注意避免IP端口冲突。如果 TaskManager 主节点出现故障，从节点将自动恢复故障取代主节点，客户端无需任何修改可继续访问 TaskManager 服务。

#### 1 下载 OpenMLDB 部署包和面向特征工程优化的 Spark 发行版

Spark发行版：
```
wget https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.7.1/spark-3.2.1-bin-openmldbspark.tgz 
tar -zxvf spark-3.2.1-bin-openmldbspark.tgz 
export SPARK_HOME=`pwd`/spark-3.2.1-bin-openmldbspark/
```

OpenMLDB部署包：
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.7.1/openmldb-0.7.1-linux.tar.gz
tar -zxvf openmldb-0.7.1-linux.tar.gz
mv openmldb-0.7.1-linux openmldb-taskmanager-0.7.1
cd openmldb-taskmanager-0.7.1
```

#### 2 修改配置文件conf/taskmanager.properties
```bash
# 可以在示例配置文件的基础上，进行修改
cp conf/taskmanager.properties.template conf/taskmanager.properties
```

* 修改server.host。host是部署机器的ip/域名。
* 修改server.port。port是部署机器的端口号。
* 修改zk_cluster为已经启动的zk集群地址。ip为zk所在机器的ip, port为zk配置文件中clientPort配置的端口号. 如果zk是集群模式用逗号分割, 格式为ip1:port1,ip2:port2,ip3:port3。
* 如果和其他OpenMLDB共用zk需要修改zookeeper.root_path。
* 修改batchjob.jar.path为BatchJob Jar文件路径，如果设置为空会到上一级lib目录下寻找。如果使用Yarn模式需要修改为对应HDFS路径。
* 修改offline.data.prefix为离线表存储路径，如果使用Yarn模式需要修改为对应HDFS路径。
* 修改spark.master为离线任务运行模式，目前支持local和yarn模式。
* 修改spark.home为Spark环境路径，如果不配置或配置为空则使用`SPARK_HOME`环境变量的配置。也可在配置文件中设置，路径为绝对路径。

```
server.host=172.27.128.33
server.port=9902
zookeeper.cluster=172.27.128.33:7181
zookeeper.root_path=/openmldb_cluster
batchjob.jar.path=
offline.data.prefix=file:///tmp/openmldb_offline_storage/
spark.master=local
spark.home=
```
```{attention}
分布式部署的集群，请不要使用客户端本地文件作为源数据导入，推荐使用hdfs路径。

`spark.master=yarn`时，**必须**使用hdfs路径。
`spark.master=local`时，如果一定要是有本地文件，可以将文件拷贝至taskmanager运行的主机上，使用taskmanager主机上的绝对路径地址。

离线数据量较大时，也推荐`offline.data.prefix`使用hdfs，而不是本地file。
```

#### 3 启动服务
```
bash bin/start.sh start taskmanager
```

`ps f|grep taskmanager`应运行正常，`curl http://<taskmanager_ip>:<port>/status`可以查询到taskmanager进程状态。

```{note}
taskmanager的日志分为taskmanager进程日志和每个离线命令的job日志。默认路径为<启动目录>/taskmanager/bin/logs，其中：
- taskmanager.log/.out为taskmanager进程日志，如果taskmanager进程退出，请查看这个日志。
- job_x_error.log为单个job的运行日志，job_x.log为单个job的print日志（如果是异步select，结果将打印在此处）。如果离线任务失败，例如job 10失败，**请到taskmanager所在机器上**找到对应的日志job_10.log和job_10_error.log。
```

#### 4 检查服务是否启动
```bash
$ ./bin/openmldb --zk_cluster=172.27.128.33:7181  --zk_root_path=/openmldb_cluster --role=sql_client
> show components;
```
结果应类似下表，包含所有集群的组件（APIServer除外）。
```
 ------------------- ------------ --------------- -------- ---------
  Endpoint            Role         Connect_time    Status   Ns_role
 ------------------- ------------ --------------- -------- ---------
  172.27.128.33:9527  tablet       1665568158749   online   NULL
  172.27.128.33:9528  tablet       1665568158741   online   NULL
  172.27.128.31:6527  nameserver   1665568159782   online   master
  172.27.128.33:9902  taskmanager  1665649276766   online   NULL
 ------------------- ------------ --------------- -------- ---------
 ```

在sql client中，可以通过执行以下sql命令，读写简单的表测试一下集群功能是否正常。（为了简单起见，这里只测试在线部分）
```
create database simple_test;
use simple_test;
create table t1(c1 int, c2 string);
set @@execute_mode='online';
Insert into t1 values (1, 'a'),(2,'b');
select * from t1;
```
