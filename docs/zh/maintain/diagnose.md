# 诊断工具

## 概述

为了方便排查用户环境中的常见问题，OpenMLDB提供了诊断工具。

## 使用

安装方式与使用：
```bash
pip install openmldb-tool
openmldb_tool # 注意下划线
```
有以下几个子命令可选择执行：
```bash
usage: openmldb_tool [-h] [--helpfull] {status,inspect,test,static-check} ...
```
只有`static-check`静态检查命令需要指定`--dist_conf`参数，该参数指定OpenMLDB节点分布的配置文件。其他命令只需要`--cluster`参数，格式为`<zk_cluster>/<zk_root_path>`，默认为镜像中的OpenMLDB集群地址`127.0.0.1:2181/openmldb`。如果是自行设置的OpenMLDB集群，请配置此参数。

## 子命令详情

### status 状态

`status`用于查看OpenMLDB集群的状态，包括服务组件的地址，角色，连接时间，状态等，等价于`SHOW COMPONENTS`。如果发现集群表现不正常，请先查询各服务组件的实时状态。
```
openmldb_tool status -h
usage: openmldb_tool status [-h] [--helpfull] [--diff]

optional arguments:
  -h, --help  show this help message and exit
  --helpfull  show full help message and exit
  --diff      check if all endpoints in conf are in cluster. If set, need to set `--conf_file`
  --conn      check network connection of all servers
```

- 简单查询集群状态：
  ```
  openmldb_tool status [--cluster=...]
  ```
  输出类似下表：
  ```
  +-----------------+-------------+---------------+--------+---------+
  |     Endpoint    |     Role    |  Connect_time | Status | Ns_role |
  +-----------------+-------------+---------------+--------+---------+
  | localhost:10921 |    tablet   | 1677398926974 | online |   NULL  |
  | localhost:10922 |    tablet   | 1677398926978 | online |   NULL  |
  |  localhost:7527 |  nameserver | 1677398927985 | online |  master |
  |  localhost:9902 | taskmanager | 1677398934773 | online |   NULL  |
  +-----------------+-------------+---------------+--------+---------+
  ```

#### 检查配置文件与集群状态是否一致

如果指定`--diff`参数，会检查配置文件中的所有节点是否都在已经启动的集群中，如果有节点不在集群中，会输出异常信息。如果集群中有节点不在配置文件中，不会输出异常信息。需要配置`-f,--conf_file`，例如，你可以在镜像里这样检查：
```bash
openmldb_tool status --diff -f=/work/openmldb/conf/hosts
```

#### 检查各个节点的连接状况
如果指定`--conn`参数，会检查集群中所有节点能否成功连接，如果连接成功，会返回连接所用时间和版本信息。如果有节点连接失败，会输出异常信息。你可以这样检查：
```bash
openmldb_tool status --conn
```
输出类似下表:
```
+---------------------------------------------------------------------------+
|                                Connections                                |
+-----------------+---------------+-----------+-----------------------------+
|     Endpoint    |    version    | cost_time |            extra            |
+-----------------+---------------+-----------+-----------------------------+
| localhost:10921 | 0.7.3-736ecb2 |  4.491ms  |                             |
| localhost:10922 | 0.7.3-736ecb2 |  3.425ms  |                             |
|  localhost:7527 | 0.7.3-736ecb2 |  3.489ms  |                             |
|  localhost:9902 | 0.7.3-736ecb2 |  5.273ms  | batchVersion: 0.7.3-736ecb2 |
+-----------------+---------------+-----------+-----------------------------+
```

### inspect 检查

`inspect`用于检查集群的在线和离线两个部分是否正常工作，可以选择单独检查`online`或`offline`，不指定则都检查。可以定期执行检查，以便及时发现异常。
```
openmldb_tool inspect -h
usage: openmldb_tool inspect [-h] [--helpfull] {online,offline} ...

positional arguments:
  {online,offline}
    online          only inspect online table
    offline         only inspect offline jobs, check the job log
```

#### 在线检查

在线检查会检查集群中的表状态（包括系统表），并输出有异常的表，包括表的状态，分区信息，副本信息等，等价于`SHOW TABLE STATUS`并筛选出有异常的表。如果发现集群表现不正常，请先检查下是否有异常表。例如，`SHOW JOBS`无法正常输出历史任务时，可以`inspect online`检查一下是否是job系统表出现问题。

##### 指定数据库

在线检查中，可以使用`inspect online --db DB`来指定要检查的数据库，会输出数据库在各个节点上的分片情况，内存占用信息等。若要查询多个数据库，请使用 ',' 分隔数据库名称，或使用`--db all`查询所有数据库的信息。

#### 离线检查

离线检查会检查集群中的离线任务，并输出最终状态为失败的任务（不检查“运行中”的任务）及其日志，等价于`SHOW JOBS`并筛选出失败任务。

### test 测试

`test`将执行一些测试SQL，包括：创建库，创建表，在线插入数据，在线查询数据，删除表，删除库。如果TaskManager组件存在，还会执行离线查询任务，由于没有做离线导入数据，查询结果理应为空。

在新建好集群时或发现集群表现不正常时，可以执行`test`测试集群是否能工作。

### static-check 静态检查

`static-check`静态检查，根据集群部署配置文件（通过参数`-f,--conf_file`指定），登录各个服务组件的部署地址，可以收集版本信息、配置文件、日志文件，检查版本是否一致，对收集到的配置文件和日志文件做分析。可以在集群未部署前进行检查，避免因程序版本或配置文件错误导致的集群部署失败。或在集群异常时，将分布式的日志文件收集在一起，方便调查问题。

```bash
openmldb_tool static-check -h
usage: openmldb_tool static-check [-h] [--helpfull] [--version] [--conf] [--log]

optional arguments:
  -h, --help     show this help message and exit
  --helpfull     show full help message and exit
  --version, -V  check version
  --conf, -C     check conf
  --log, -L      check log
```

#### 部署配置文件

`-f,--conf_file`部署配置文件格式可以是hosts风格或yaml风格，描述集群中有哪些组件，分布在哪个节点以及部署目录。
- hosts风格（参考release包中的`conf/hosts`）
```
[tablet]
localhost:10921 /tmp/openmldb/tablet-1
localhost:10922 /tmp/openmldb/tablet-2

[nameserver]
localhost:7527

[apiserver]
localhost:9080

[taskmanager]
localhost:9902

[zookeeper]
localhost:2181:2888:3888 /tmp/openmldb/zk-1
```
- 集群版yaml
```yaml
mode: cluster
zookeeper:
  zk_cluster: 127.0.0.1:2181
  zk_root_path: /openmldb
nameserver:
  -
    endpoint: 127.0.0.1:6527
    path: /work/ns1
tablet:
  -
    endpoint: 127.0.0.1:9527
    path: /work/tablet1
  -
    endpoint: 127.0.0.1:9528
    path: /work/tablet2
taskmanager:
  -
    endpoint: 127.0.0.1:9902
    path: /work/taskmanager1
```

如果是分布式部署，诊断工具需要到部署节点上拉取文件，所以需要添加机器互信免密。设置方法参考[这里](https://www.itzgeek.com/how-tos/linux/centos-how-tos/ssh-passwordless-login-centos-7-rhel-7.html)。

如果hosts/yaml中某些组件没有配置path，将会使用`--default_dir`作为部署目录，默认值为`/work/openmldb`。如果你的部署目录不是这个，可以通过`--default_dir`指定。

如果是onebox部署，可以指定`--local`，我们会把所有节点当作本地节点，不会尝试ssh登录。如果部分节点是本地节点，只能使用yaml格式的部署配置文件，对本地节点配置`is_local: true`。例如：
```yaml
nameserver:
  -
    endpoint: 127.0.0.1:6527
    path: /work/ns1
    is_local: true
```

#### 检查内容

检查可通过组合FLAG来来指定检查哪些内容，例如，`-V`只检查版本，`-CL`只检查配置文件和日志，`-VCL`检查全部。

- `-V,--version`检查版本，检查各个组件的版本是否一致，如果不一致，会输出不一致的组件和版本信息。
- `-C,--conf`收集配置文件，检查各个组件的配置文件中ZooKeeper地址是否一致等。
- `-L,--log`收集日志，输出WARNING及以上的日志。

如果检查配置文件或日志，将会把收集到的文件保存在`--collect_dir`中，默认为`/tmp/diag_collect`。你也也可以访问此目录查看收集到的配置或日志，进行更多的分析。


#### 检查示例

在镜像容器中可以这样静态检查：
```bash
openmldb_tool static-check --conf_file=/work/openmldb/conf/hosts -VCL --local
```

## 附加

可使用`openmldb_tool --helpfull`查看所有配置项。例如，`--sdk_log`可以打印sdk的日志（zk，glog），可用于调试。
