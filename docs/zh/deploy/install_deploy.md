# 安装部署

## 软硬件环境需求

* 操作系统：CentOS 7, Ubuntu 20.04, macOS >= 10.15。其中Linux glibc版本 >= 2.17。其他操作系统版本没有做完整的测试，不能保证完全正确运行。
* 内存：视数据量而定，推荐在 8 GB 及以上。
* CPU：
  * 目前仅支持 x86 架构，暂不支持例如 ARM 等架构。
  * 核数推荐不少于 4 核，如果 Linux 环境下 CPU 不支持 AVX2 指令集，需要从源码重新编译部署包。

* 运行环境：zookeeper和taskmanager部署需要java runtime environment。其他组件无要求。

## 部署包准备
本说明文档中默认使用预编译好的 OpenMLDB 部署包（[Linux](https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-linux.tar.gz), [macOS](https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-darwin.tar.gz)），所支持的操作系统要求为：CentOS 7, Ubuntu 20.04, macOS >= 10.15。如果用户期望自己编译（如做 OpenMLDB 源代码开发，操作系统或者 CPU 架构不在预编译部署包的支持列表内等原因），用户可以选择在 docker 容器内编译使用或者从源码编译，具体请参照我们的[编译文档](compile.md)。

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

## 预备测试

由于linux平台的多样性，发布包可能在你的机器上不兼容，请先通过简单的运行测试。
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-linux.tar.gz
tar -zxvf openmldb-0.6.8-linux.tar.gz
./openmldb-0.6.8-linux/bin/openmldb --version
```
结果应显示该程序的版本号，类似
```
openmldb version 0.6.3-xxxx
Debug build (NDEBUG not #defined)
```

如果产生coredump，很可能时指令集不兼容问题，需要通过源码编译openmldb。源码编译参考[这里](./compile.md), 需要采用方式三完整源代码编译。

## 部署单机版
OpenMLDB单机版需要部署一个nameserver和一个tablet. nameserver用于表管理和元数据存储，tablet用于数据存储。APIServer是可选的，如果要用http的方式和OpenMLDB交互需要部署此模块

**注意:** 最好把不同的组件部署在不同的目录里，便于单独升级

### 部署tablet
#### 1 下载OpenMLDB部署包
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-linux.tar.gz
tar -zxvf openmldb-0.6.8-linux.tar.gz
mv openmldb-0.6.8-linux openmldb-tablet-0.6.8
cd openmldb-tablet-0.6.8
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
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-linux.tar.gz
tar -zxvf openmldb-0.6.8-linux.tar.gz
mv openmldb-0.6.8-linux openmldb-ns-0.6.8
cd openmldb-ns-0.6.8
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
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-linux.tar.gz
tar -zxvf openmldb-0.6.8-linux.tar.gz
mv openmldb-0.6.8-linux openmldb-apiserver-0.6.8
cd openmldb-apiserver-0.6.8
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
OpenMLDB集群版需要部署zookeeper、nameserver、tablet等模块。其中zookeeper用于服务发现和保存元数据信息。nameserver用于管理tablet，实现高可用和failover。tablet用于存储数据和主从同步数据。APIServer是可选的，如果要用http的方式和OpenMLDB交互需要部署此模块。taskmanager用于管理离线job。

**注意:** 同一台机器部署多个组件时，一定要部署在不同的目录里，便于单独管理。尤其是部署tablet server，一定不能重复使用目录。

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
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-linux.tar.gz
tar -zxvf openmldb-0.6.8-linux.tar.gz
mv openmldb-0.6.8-linux openmldb-tablet-0.6.8
cd openmldb-tablet-0.6.8
```
#### 2 修改配置文件`conf/tablet.flags`
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

比如，可以再次解压压缩包（不要cp已经启动过tablet的目录，启动后的生成文件会造成影响），并命名目录为`openmldb-tablet-0.6.8-2`。

```
tar -zxvf openmldb-0.6.8-linux.tar.gz
mv openmldb-0.6.8-linux openmldb-tablet-0.6.8-2
cd openmldb-tablet-0.6.8-2
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
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-linux.tar.gz
tar -zxvf openmldb-0.6.8-linux.tar.gz
mv openmldb-0.6.8-linux openmldb-ns-0.6.8
cd openmldb-ns-0.6.8
````
#### 2 修改配置文件conf/nameserver.flags
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

比如，可以再次解压压缩包（不要cp已经启动过 namserver 的目录，启动后的生成文件会造成影响），并命名目录为`openmldb-ns-0.6.8-2`。

```
tar -zxvf openmldb-0.6.8-linux.tar.gz
mv openmldb-0.6.8-linux openmldb-ns-0.6.8-2
cd openmldb-ns-0.6.8-2
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
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-linux.tar.gz
tar -zxvf openmldb-0.6.8-linux.tar.gz
mv openmldb-0.6.8-linux openmldb-apiserver-0.6.8
cd openmldb-apiserver-0.6.8
```

#### 2 修改配置文件conf/apiserver.flags

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

#### 1 下载 OpenMLDB 部署包和面向特征工程优化的 Spark 发行版

Spark发行版：
```
wget https://github.com/4paradigm/spark/releases/download/v3.2.1-openmldb0.6.8/spark-3.2.1-bin-openmldbspark.tgz 
tar -zxvf spark-3.2.1-bin-openmldbspark.tgz 
export SPARK_HOME=`pwd`/spark-3.2.1-bin-openmldbspark/
```

OpenMLDB部署包：
```
wget https://github.com/4paradigm/OpenMLDB/releases/download/v0.6.8/openmldb-0.6.8-linux.tar.gz
tar -zxvf openmldb-0.6.8-linux.tar.gz
mv openmldb-0.6.8-linux openmldb-taskmanager-0.6.8
cd openmldb-taskmanager-0.6.8
```

#### 2 修改配置文件conf/taskmanager.properties

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
