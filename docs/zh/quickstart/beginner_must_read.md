# 上手必读

由于OpenMLDB是分布式系统，多种模式，客户端丰富，初次使用可能会有很多疑问，或者遇到一些运行、使用问题，本文从新手使用的角度，讲解如何进行诊断调试，需求帮助时如何提供有效信息给技术人员等等。

## 创建OpenMLDB与连接

首先，我们建议不熟悉分布式多进程管理的新手使用docker创建OpenMLDB，方便快速上手。熟悉OpenMLDB各组件之后，再尝试分布式部署。

docker创建OpenMLDB见[快速上手](./openmldb_quickstart.md)，请注意文档中有两个版本，单机版和集群版。请清楚自己要创建哪个版本，不要混合使用。

启动成功的标准是可以使用CLI连接上OpenMLDB服务端（即使用`/work/openmldb/bin/openmldb`连接OpenMLDB，单机或集群均可以通过CLI连接），并且执行`show components;`可以看到OpenMLDB服务端组件的运行情况。

如果CLI无法连接OpenMLDB，请先确认进程是否运行正常，可以通过`ps f|grep bin/openmldb`确认nameserver和tabletserver进程，集群版还需要通过`ps f | grep zoo.cfg`来确认zk服务，`ps f | grep TaskManagerServer`来确认taskmanager进程。

如果所有服务进程都运行中，但CLI连接服务端失败，请确认CLI运行的参数。如果仍有问题，请联系我们并提供CLI的错误信息。

```{seealso}
如果我们还需要OpenMLDB服务端的配置和日志，可以使用诊断工具获取，见[下文](#提供配置与日志)。
```

## 执行SQL

OpenMLDB所有命令均为SQL，如果SQL执行失败或交互有问题（不知道命令是否执行成功），请先确认SQL书写是否有误，命令并未执行，还是命令进入了执行阶段。

例如，下面提示Syntax error的是SQL书写有误，请参考[sql reference](../reference/sql/)纠正错误。
```
127.0.0.1:7527/db> create table t1(c1 int;
Error: Syntax error: Expected ")" or "," but got ";" [at 1:23]
create table t1(c1 int;
                      ^
```

如果是命令进入执行阶段，但执行失败或交互失败，需要明确以下几点：

- OpenMLDB是单机还是集群？
- 执行模式是什么？CLI运行命令时可以使用`show variable`获取，但注意单机版的执行模式没有意义。

我们需要特别注意集群版的一些使用逻辑。

### 集群

#### 离线

如果是集群离线命令，默认异步模式下，发送命令会得到job id的返回。可使用`show job <id>`来查询job执行情况。

离线job如果是SELECT（并不INTO保存结果），也不会将结果打印在客户端。需要从日志中获得结果，日志默认在`/work/openmldb/taskmanager/bin/logs/jog_x.log`。

如果发现job failed或者finished，但不符合你的预期，请查询日志。日志默认在`/work/openmldb/taskmanager/bin/logs/jog_x_error.log`(注意有error后缀)，

日志地址由taskmanager.properties的`job.log.path`配置，如果你改变了此配置项，需要到配置的目的地寻找日志。

```{note}
如果taskmanager是yarn模式，而不是local模式，`job_x_error.log`中的信息会较少，不会有job错误的详细信息。需要通过`job_x_error.log`中记录的yarn app id，去yarn系统中查询job的真正错误原因。
```

#### 在线

集群版在线模式下，我们通常只推荐使用`DEPLOY`创建deployment和执行deployment做实时特征计算。在CLI或其他客户端中，直接在在线中进行SELECT查询，称为“在线预览”。在线预览有诸多限制，详情请参考[功能边界-集群版在线预览模式](./function_boundary.md#集群版在线预览模式)，请不要执行不支持的SQL。

### 提供复现脚本

如果你通过自主诊断，无法解决问题，请向我们提供复现脚本。一个完整的复现脚本，如下所示：

```
create database db;
use db;
-- create youer table
create table xx ();

-- offline or online
set @@execute_mode='';

-- load data or online insert
-- load data infile '' into table xx;
-- insert into xx values (),();

-- query / deploy ...

```

如果你的问题需要数据才能复现，请提供数据。如果是离线数据，离线无法支持insert，请提供csv/parquet数据文件。如果是在线数据，可以提供数据文件，也可以直接在脚本中进行insert。

这样的数据脚本可以通过重定向符号，批量执行sql脚本中的命令。
```
/work/openmldb/bin/openmldb --host 127.0.0.1 --port 6527 < reproduce.sql
/work/openmldb/bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < reproduce.sql
```

请确保在你本地可以使用复现脚本复现问题，再记录issue或发送给我们。

```{caution}
请注意离线job默认为异步。如果你需要离线导入再查询，请设置为同步模式，详情见[配置离线命令同步执行](../reference/sql/ddl/SET_STATEMENT.md#配置离线命令同步执行)。否则导入还未完成就进行查询，是无意义的。
```

### 提供配置与日志

如果你的问题无法通过复现脚本复现，那么，就需要你提供客户端和服务端的配置与日志，以便我们调查。

docker或本地的集群（服务端所有进程都在本地），可以使用诊断工具快速获取配置、日志等信息。

使用init.sh/start-all.sh脚本启动的OpenMLDB服务端，可以使用以下命令进行诊断，分别对应集群版和单机版。
```
openmldb_tool --env=onebox --dist_conf=cluster_dist.yml
openmldb_tool --env=onebox --dist_conf=stadnalone_dist.yml
```
`cluster_dist.yml`和`stadnalone_dist.yml`，可在docker容器`/work/diag`目录中找到，或将[github目录](https://github.com/4paradigm/OpenMLDB/tree/main/demo)中的yml文件复制下来使用。

如果是分布式的集群，需要配置ssh免密才能顺利使用诊断工具，参考文档[诊断工具](../maintain/diagnose.md)。

如果你的环境无法做到，请手动获取配置与日志。
