# 诊断工具

## 概述

为了方便排查用户环境中的常见问题，OpenMLDB提供了诊断工具。

## 使用

安装方式与使用：
```bash
pip install openmldb-tool # openmldb-tool[rpc]
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

### inspect 检查

`inspect`用于检查集群的在线和离线两个部分是否正常工作，可以选择单独检查`online`或`offline`，不指定则都检查。可以定期执行检查，以便及时发现异常。
```
openmldb_tool inspect -h
usage: openmldb_tool inspect [-h] [--helpfull] {online,offline,job} ...

positional arguments:
  {online,offline,job}
    online              only inspect online table.
    offline             only inspect offline jobs.
    job                 show jobs by state, show joblog or parse joblog by id.
```
在线检查会检查集群中的表状态（包括系统表），并输出有异常的表，包括表的状态，分区信息，副本信息等，等价于`SHOW TABLE STATUS`并筛选出有异常的表。如果发现集群表现不正常，请先检查下是否有异常表。例如，`SHOW JOBS`无法正常输出历史任务时，可以`inspect online`检查一下是否是job系统表出现问题。

##### 检查在线数据分布

在线检查中，可以使用`inspect online --dist`检查在线数据分布，默认检查所有数据库，可以使用`--db`指定要检查的数据库。若要查询多个数据库，请使用 ',' 分隔数据库名称。会输出数据库在各个节点上的数据分布情况。

#### 离线检查

离线检查会输出最终状态为失败的任务（不检查“运行中”的任务），等价于`SHOW JOBS`并筛选出失败任务。

#### JOB 检查

JOB 检查会检查集群中的离线任务，可以使用`inspect job`或`inspect job --state all`查询所有任务，等价于`SHOW JOBS`并按job_id排序。使用`inspect job --state <state>`可以筛选出特定状态的日志，可以使用 ',' 分隔，同时查询不同状态的日志。例如：`inspect offline` 相当于`inspect job --state failed,killed,lost`即筛选出所有失败的任务。

以下是一些常见的state:

| state    | 描述           |
| -------- | -------------- |
| finished | 成功完成的任务 |
| running  | 正在运行的任务 |
| failed   | 失败的任务     |
| killed   | 被终止的任务   |

更多state信息详见[Spark State]( https://spark.apache.org/docs/3.2.1/api/java/org/apache/spark/launcher/SparkAppHandle.State.html)，[Yarn State](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/yarn/api/records/YarnApplicationState.html)


使用`inspect job --id <job_id>`查询指定任务的log日志，其结果会使用配置文件筛选出主要错误信息。如需更新配置文件，可以添加`--conf-update`，并且可以使用`--conf-url`配置镜像源，例如使用`--conf-url https://openmldb.ai/download/diag/common_err.yml`配置国内镜像。如果需要完整的日志信息，可以添加`--detail`获取详细信息。

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

### rpc

`openmldb_tool`还提供了一个RPC接口，但它是一个额外组件，需要通过`pip install openmldb-tool[rpc]`安装。使用方式是`openmldb_tool rpc`，例如，`openmldb_tool rpc ns ShowTable --field '{"show_all":true}'`可以调用`nameserver`的`ShowTable`接口，获取表的状态信息。

NameServer与TaskManager只有一个活跃，所以我们用ns和tm来代表这两个组件。
而TabletServer有多个，我们用`tablet1`，`tablet2`等来指定某个TabletServer，顺序可通过`openmldb_tool rpc`或`openmldb_tool status`来查看。

如果对RPC服务的方法或者输入参数不熟悉，可以通过`openmldb_tool rpc <component> [method] --hint`查看帮助信息。例如：
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
hint还需要额外的pb文件，帮助解析输入参数，默认是从`/tmp/diag_cache`中读取，如果不存在则自动下载。如果你已有相应的文件，或者已经手动下载，可以通过`--pbdir`指定该目录。

## 附加

可使用`openmldb_tool --helpfull`查看所有配置项。例如，`--sdk_log`可以打印sdk的日志（zk，glog），可用于调试。
  