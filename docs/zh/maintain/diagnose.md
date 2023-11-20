# 诊断工具

## 概述

为了方便排查用户环境中的常见问题，OpenMLDB提供了诊断工具。

## 使用

安装方式与使用：
```bash
pip install openmldb-tool # openmldb-tool[pb]
openmldb_tool # 注意下划线
```
有以下几个子命令可选择执行：
```bash
usage: openmldb_tool [-h] [--helpfull] {status,inspect,rpc,test,static-check} ...
```

注意`-c/--cluster`参数，格式为`<zk_cluster>/<zk_root_path>`，默认将访问`127.0.0.1:2181/openmldb`。如果是自行设置的OpenMLDB集群，请配置此参数。其他参数根据子命令不同而不同，可以使用`-h`查看，或查看各个子命令的详细文档。

### 一键inspect

`openmldb_tool inspect [--cluster=0.0.0.0:2181/openmldb]`可以一键查询，得到完整的集群状态报告。如果需要局部视角或额外的诊断功能，才需要其他子命令。

报告分为几个板块，其中如果所有表都是健康的，不会展示Ops和Partitions板块。用户首先看报告末尾的总结 summary & hint，如果存在server offline（红色），需先重启server，保证server尤其是TabletServer都在线。server重启后，集群可能会尝试自动修复，自动修复也可能会失败，所以，用户有必要等待一定时间后再次inspect。此时如果仍然有不健康的表，可以检查它们的状态，Fatal表需要尽快修复，它们可能会读写失败，Warn表，用户可以考虑推迟修复。修复方式见报告末尾提供的文档。

`inspect`可配置参数除了`--cluster/-c`，还可配置不显示彩色`--nocolor/-noc`方便复制，以及`--table_width/-tw n`配置表格宽度，`--offset_diff_thresh/-od n`配置offset diff的报警阈值。

```
diagnosing cluster xxx


Server Detail
{server map}
{server online/offline report}


Table Partitions Detail
tablet server order: {tablet ip -> idx}
{partition tables of unhealthy tables}
Example:
{a detailed description of partition table}


Ops Detail
> failed ops do not mean cluster is unhealthy, just for reference
last one op(check time): {}
last 10 ops != finished:
{op list}



==================
Summary & Hint
==================
Server:

{online | offline servers ['[tablet]xxx'], restart them first}

Table:
{all healthy | unhealthy tables desc}
[]Fatal/Warn table, {read/write may fail or still work}, {repair immediatly or not}
{partition detail: if leader healthy, if has unhealthy replicas, if offset too large, related ops}

    Make sure all servers online, and no ops for the table is running.
    Repair table manually, run recoverdata, check https://openmldb.ai/docs/zh/main/maintain/openmldb_ops.html.
    Check 'Table Partitions Detail' above for detail.
```

### 其他常用命令

除了一键inspect，在这样几个场景中，我们推荐使用诊断工具的子命令来帮助用户判断集群状态、简化运维。

- 部署好集群后，可以使用`test`测试集群是否能正常工作，不需要用户手动测试。如果发现问题，再使用`inspect`诊断。
- 组件都在线，但出现超时或错误提示某组件无法连接时，可以使用`status --conn`检查与各组件的连接，会打印出简单访问的耗时。也可以用它来测试客户端主机与集群的连接情况，及时发现网络隔离。
- 离线job如果出现问题，`SHOW JOBLOG id`可以查看日志，但经验较少的用户可能会被日志中的无关信息干扰，可以使用`inspect job`来提取job日志中的关键信息。
- 离线job太多时，CLI中的展示会不容易读，可以使用`inspect offline`筛选所有failed的job，或者`inspect job --state <state>`来筛选出特定状态的job。
- 在一些棘手的问题中，可能需要用户通过RPC来获得一些信息，帮助定位问题。`openmldb_tool rpc`可以帮助用户简单快速地调用RPC，降低运维门槛。
- 没有Prometheus监控时，可以通过`inspect online --dist`获得数据分布信息。
- 如果你的操作节点到各个组件的机器是ssh免密的，那么，可以使用`static-check`检查配置文件是否正确，版本是否统一，避免部署失败。还可以一键收集整个集群的日志，方便打包并提供给开发人员分析。

## 子命令详情

### status 状态

`status`用于查看OpenMLDB集群的状态，包括服务组件的地址，角色，连接时间，状态等，等价于`SHOW COMPONENTS`。如果发现集群表现不正常，请先查询各服务组件的实时状态。
```
openmldb_tool status -h
usage: openmldb_tool status [-h] [--helpfull] [--diff]

optional arguments:
  -h, --help  show this help message and exit
  --helpfull  show full help message and exit
  --diff      check if all endpoints in conf are in cluster. If set, need to set `-f,--conf_file`
  --conn                check network connection of all servers
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

- 检查并测试集群链接与版本：
  ```
  openmldb_tool status --conn
  ```

#### 检查配置文件与集群状态是否一致

如果指定`--diff`参数，会检查配置文件中的所有节点是否都在已经启动的集群中，如果有节点不在集群中，会输出异常信息。如果集群中有节点不在配置文件中，不会输出异常信息。需要配置`-f,--conf_file`，例如，你可以在镜像里这样检查：
```bash
openmldb_tool status --diff -f=/work/openmldb/conf/hosts
```

### inspect 检查

如果是为了检查集群状态，更推荐一键`inspect`获取集群完整检查报告，`inspect`子命令是更具有针对性的检查。

```
openmldb_tool inspect -h
usage: openmldb_tool inspect [-h] [--helpfull] {online,offline,job} ...

positional arguments:
  {online,offline,job}
    online              only inspect online table.
    offline             only inspect offline jobs.
    job                 show jobs by state, show joblog or parse joblog by id.
```

#### online在线检查

`inspect online`检查在线表的健康状态，并输出有异常的表，包括表的状态，分区信息，副本信息等，等价于`SHOW TABLE STATUS`并筛选出有异常的表。

##### 检查在线数据分布

可以使用`inspect online --dist`检查在线数据分布，默认检查所有数据库，可以使用`--db`指定要检查的数据库。若要查询多个数据库，请使用 ',' 分隔数据库名称。会输出数据库在各个节点上的数据分布情况。

#### offline离线检查

`inspect offline`离线检查会输出最终状态为失败的任务（不检查“运行中”的任务），等价于`SHOW JOBS`并筛选出失败任务。更多功能待补充。

#### JOB 检查

JOB 检查是更灵活的离线任务检查命令，可以按条件筛选job，或针对单个job日志进行分析。

##### 按state筛选

可以使用`inspect job`或`inspect job --state all`查询所有任务，等价于`SHOW JOBS`并按job_id排序。使用`inspect job --state <state>`可以筛选出特定状态的日志，可以使用 ',' 分隔，同时查询不同状态的日志。例如：`inspect offline` 相当于`inspect job --state failed,killed,lost`即筛选出所有失败的任务。

以下是一些常见的state:

| state    | 描述           |
| -------- | -------------- |
| finished | 成功完成的任务 |
| running  | 正在运行的任务 |
| failed   | 失败的任务     |
| killed   | 被终止的任务   |

更多state信息详见[Spark State]( https://spark.apache.org/docs/3.2.1/api/java/org/apache/spark/launcher/SparkAppHandle.State.html)，[Yarn State](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/yarn/api/records/YarnApplicationState.html)

##### 解析单个JOB日志

使用`inspect job --id <job_id>`查询指定任务的log日志，其结果会使用配置文件筛选出主要错误信息。

解析依靠配置文件，默认情况会自动下载。如需更新配置文件，可以`--conf-update`，它将会在解析前强制下载一次配置文件。如果默认下载源不合适，可以同时配置`--conf-url`配置镜像源，例如使用`--conf-url https://openmldb.ai/download/diag/common_err.yml`配置国内镜像。

如果只需要完整的日志信息而不是解析日志的结果，可以使用`--detail`获取详细信息，不会打印解析结果。

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

### RPC 接口

`openmldb_tool`还提供了一个RPC接口，它可以让我们发送RPC更容易，不需要定位Server的IP，拼接RPC方法URL路径，也可以提示所有RPC方法和RPC方法的输入结构。使用方式是`openmldb_tool rpc`，例如，`openmldb_tool rpc ns ShowTable --field '{"show_all":true}'`可以调用`nameserver`的`ShowTable`接口，获取表的状态信息。

其中组件不使用ip，可以直接使用角色名。NameServer与TaskManager只有一个活跃，所以我们用ns和tm来代表这两个组件。而TabletServer有多个，我们用`tablet1`，`tablet2`等来指定某个TabletServer，从1开始，顺序可通过`openmldb_tool rpc`或`openmldb_tool status`来查看。

如果对RPC服务的方法或者输入参数不熟悉，可以通过`openmldb_tool rpc <component> [method] --hint`查看帮助信息。但它是一个额外组件，需要通过`pip install openmldb-tool[pb]`安装。hint还需要额外的pb文件，帮助解析输入参数，默认是从`/tmp/diag_cache`中读取，如果不存在则自动下载。如果你已有相应的文件，或者已经手动下载，可以通过`--pbdir`指定该目录。自行编译pb文件，见[openmldb tool开发文档](https://github.com/4paradigm/OpenMLDB/blob/main/python/openmldb_tool/README.md#rpc)。

例如：
```bash
$ openmldb_tool rpc ns ShowTable --hint
...
server proto version is 0.7.0-e1d35fcf6
hint use pb2 files from /tmp/diag_cache
You should input json like this, ignore round brackets in the key and double quotation marks in the value: --field '{
    "(optional)name": "string",
    "(optional)db": "string",
    "(optional)show_all": "bool"
}'
```

## 附加

可使用`openmldb_tool --helpfull`查看所有配置项。例如，`--sdk_log`可以打印sdk的日志（zk，glog），可用于调试。
