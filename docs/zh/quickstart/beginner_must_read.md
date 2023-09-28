# 上手必读

由于OpenMLDB是分布式系统，多种模式，客户端丰富，初次使用可能会有很多疑问，或者遇到一些运行、使用问题，本文从新手使用的角度，讲解如何进行诊断调试，需要帮助时如何提供有效信息给技术人员等等。

## OpenMLDB集群管理

首先，我们建议不熟悉分布式多进程管理的新手使用docker创建OpenMLDB，方便快速上手。熟悉OpenMLDB各组件之后，再尝试分布式部署。

docker创建OpenMLDB见[快速上手](./openmldb_quickstart.md)，请注意文档中有两个版本，单机版和集群版。请清楚自己要创建哪个版本，不要混合使用。

启动成功的标准是可以使用CLI连接上OpenMLDB服务端（即使用`/work/openmldb/bin/openmldb`连接OpenMLDB，单机或集群均可以通过CLI连接），并且执行`show components;`可以看到OpenMLDB服务端组件的运行情况。推荐使用[诊断工具](../maintain/diagnose.md)，执行status和inspect，可以得到更可靠的诊断结果。

如果CLI无法连接OpenMLDB，请先确认进程是否运行正常，可以通过`ps f|grep bin/openmldb`确认nameserver和tabletserver进程，集群版还需要通过`ps f | grep zoo.cfg`来确认zk服务，`ps f | grep TaskManagerServer`来确认taskmanager进程。

如果所有服务进程都运行中，但CLI连接服务端失败，请确认CLI运行的参数。如果仍有问题，请联系我们并提供CLI的错误信息。

```{seealso}
如果我们还需要OpenMLDB服务端的配置和日志，可以使用诊断工具获取，见[下文](#提供配置与日志获得技术支持)。
```

### 运维

集群各组件进程启动后，在使用过程中可能遇到各种变化，比如服务进程意外退出，需要重启服务进程，或者需要扩容服务进程。

如果你需要保留已有的在线表，**不要主动地kill全部Tablet再重启**，保证Tablet只有单台在上下线。`stop-all.sh`和`start-all.sh`脚本是给快速重建集群用的，可能会导致在线表数据恢复失败，**不保证能修复**。

当你发现进程变化或者操作其变化后，需要使用诊断工具进行诊断，确认集群状态是否正常。最常用的两个命令是：
```bash
openmldb_tool status # --diff hosts 可检查TaskManager等是否掉线，当然，你也可以手动判断
openmldb_tool inspect online
```

如果诊断出server offline，或是TaskManager等掉线，需要先尝试启动回来。如果启动失败，请查看对应日志，提供错误信息。

如果server都在线，但inspect online发现online表不正常。需要从以下几点排查：
- 是否是手动操作了stop all & start all，日志中是否包含`recovering data`信息？
  - 如果过程中已经尝试过`recovering data`，但inspect结果仍然不正常，手动恢复的可能性较小 TODO
  - 如果没有尝试过`recovering data`，参考下一步。
- 尝试`recovering data`，命令参考[OpenMLDB运维工具](../maintain/openmldb_ops.md)。如果仍然不正常，请提供日志。

## 源数据

### LOAD DATA

从文件导入数据到OpenMLDB，通常使用LOAD DATA命令，详情参考[LOAD DATA INFILE](../openmldb_sql/dml/LOAD_DATA_STATEMENT.md)。LOAD DATA可使用的数据源和数据格式，与OpenMLDB版本（单机/集群）、执行模式、导入模式（即LOAD DATA配置项load_mode）都有一定关系。集群版默认 load_mode 为cluster，也可设置为local；单机版默认 load_mode 为local，**不支持cluster**。所以我们分为三种情况讨论：

| LOAD DATA类型 | 支持执行模式（导入目的地） | 支持异步/同步 | 支持数据源 | 支持数据格式 |
| :------------ | :----------------------- | :----------- | :-------- | :---------- |
| 集群版 load_mode=cluster | 在线、离线 | 异步、同步 | file(有条件限制，请参考具体文档)/hdfs/hive| csv/parquet(hive源不限制格式) |
| 集群版 load_mode=local | 在线 | 同步 | 客户端本地file | csv |
| 单机版（only local) | 在线 |  同步 | 客户端本地file | csv |

当LOAD DATA的源数据为csv格式时，还需额外注意列类型为timestamp列的格式问题。timestamp格式可分为"int64"(以下称为int64型)和"yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"(以下称为年月日型)。所以我们分为三种讨论：

| LOAD DATA类型 | 支持int64 | 支持年月日 |
| :------------ | :------- | :-------- |
| 集群版 load_mode=cluster | **``✓``** | **``✓``** |
| 集群版 load_mode=local | **``✓``** | **``X``** |
| 单机版（only local) | **``✓``** | **``X``** |

```{hint}
csv文件格式有诸多不便，更推荐使用parquet格式，需要OpenMLDB集群版并启动taskmanager组件。
```

## SQL

OpenMLDB并不完全兼容标准SQL。所以，部分SQL执行会得不到预期结果。如果发现SQL执行不符合预期，请先查看下SQL是否满足[功能边界](./function_boundary.md)。

### SQL编写指南

首先，OpenMLDB SQL 通常是使用`WINDOW`（包括`WINDOW UNION`），`LAST JOIN`子句，它们能保证在任何模式下使用。可以跟随教程"基于 SQL 的特征开发"[(上)](../tutorial/tutorial_sql_1.md)[(下)](../tutorial/tutorial_sql_2.md)进行学习。

如果使用`WHERE`，`WITH`，`HAVING`等子句，需要注意限制条件。在每个子句的详细文档中都有具体的说明，比如[`HAVING`子句](../openmldb_sql/dql/HAVING_CLAUSE.md)在在线请求模式中不支持。翻阅OpenMLDB SQL的DQL目录，或使用搜索功能，可以快速找到子句的详细文档。

在不熟悉OpenMLDB SQL的情况下，我们建议从下到上编写SQL，确保每个子句都能通过，再逐步组合成完整的SQL。

推荐使用[OpenMLDBSimulator](https://github.com/vagetablechicken/OpenMLDBSimulator)进行SQL探索，SQL完成后再去真实集群进行上线。Simulator可以不依赖真实OpenMLDB集群，在一个交互式虚拟环境中，快速创建表、校验SQL、导出当前环境等等，详情参考该项目的README。使用Simulator不需要操作集群，也就不需要测试后清理集群，还可通过少量的数据进行SQL运行测试，比较适合SQL探索时期。

#### SQL 语法提示

当发现SQL编译报错时，需要查看错误信息。例如`Syntax error: Expected XXX but got keyword YYY`错误，它说明SQL不符合语法，通常是某些关键字写错了位置，或并没有这种写法。详情需要查询错误的子句文档，可注意子句的`Syntax`章节，它详细说明了每个部分的组成，请检查SQL是否符合要求。

比如，[`WINDOW`子句](../openmldb_sql/dql/WINDOW_CLAUSE.md#syntax)中`WindowFrameClause (WindowAttribute)*`部分，我们再拆解它就是`WindowFrameUnits WindowFrameBounds [WindowFrameMaxSize] (WindowAttribute)*`。那么，`WindowFrameUnits WindowFrameBounds MAXSIZE 10 EXCLUDE CURRENT_TIME`就是符合语法的，`WindowFrameUnits WindowFrameBounds EXCLUDE CURRENT_TIME MAXSIZE 10`就是不符合语法的，不能把`WindowFrameMaxSize`放到`WindowFrameClause`外面。

#### SQL 计算提示

SQL编译通过，可以结合数据进行计算。如果计算结果不符合预期，请逐步检查：
- SQL无论是一列还是多列计算结果不符合预期，都请选择**其中一列**进行调试。
- 如果你的表数据较多，建议使用小数据量（几行，几十行的量级）来测试，也可以使用OpenMLDBSimulator的[运行toydb](https://github.com/vagetablechicken/OpenMLDBSimulator#run-in-toydb)功能，构造case进行测试。
- 该列是不是表示了自己想表达的意思，是否使用了不符合预期的函数，或者函数参数错误。
- 该列如果是窗口聚合的结果，是不是WINDOW定义错误，导致窗口范围不对。参考[推断窗口](../openmldb_sql/dql/WINDOW_CLAUSE.md#如何推断窗口是什么样的)进行检查，使用小数据进行验证测试。

如果你仍然无法解决问题，可以提供OpenMLDBSimulator的yaml case。如果在集群中进行的测试，请[提供复现脚本](#提供复现脚本)。

#### 在线请求模式

SQL上线，等价于`DEPLOY <name> <SQL>`成功。但`DEPLOY`操作是一个很“重”的操作，SQL如果可以上线，将会创建或修改索引并复制数据到新索引。所以，在SQL探索期使用`DEPLOY`测试SQL是否能上线，是比较浪费资源的，尤其是某些SQL可能需要多次修改才能上线，多次的`DEPLOY`可能产生很多无用的索引。在探索期间，可能还会修改表Schema，又需要删除和再创建。这些操作都是只能手动处理，比较繁琐。

如果你对OpenMLDB SQL较熟悉，一些场景下可以用“在线预览模式”进行测试，但“在线预览模式”不等于“在线请求模式”，不能保证一定可以上线。如果你对索引较为熟悉，可以通过`EXPLAIN <SQL>`来确认SQL是否可以上线，但`EXPLAIN`的检查较为严格，可能因为当前表没有匹配的索引，而判定SQL无法在“在线请求模式”中执行（因为无索引而无法保证实时性能，所以被拒绝）。

目前只有Java SDK可以使用[validateSQLInRequest](./sdk/java_sdk.md#sql-校验)方法来检验，使用上稍麻烦。我们推荐使用OpenMLDBSimulator来测试。在Simulator中，通过简单语法创建表，再使用`valreq <SQL>`可以判断是否能上线。

## SQL执行

OpenMLDB所有命令均为SQL，如果SQL执行失败或交互有问题（不知道命令是否执行成功），请先确认SQL书写是否有误，命令并未执行，还是命令进入了执行阶段。

例如，下面提示Syntax error的是SQL书写有误，请参考[SQL编写指南](#sql编写指南)纠正错误。
```
127.0.0.1:7527/db> create table t1(c1 int;
Error: Syntax error: Expected ")" or "," but got ";" [at 1:23]
create table t1(c1 int;
                      ^
```

如果是命令进入执行阶段，但执行失败或交互有问题，需要明确以下几点：

- OpenMLDB是单机还是集群？
- 执行模式是什么？CLI运行命令时可以使用`show variable`获取，但注意**单机版的执行模式没有意义**。

我们需要特别注意集群版的一些使用逻辑。

### 集群版SQL执行

#### 离线

如果是集群离线命令，默认异步模式下，发送命令会得到job id的返回。可使用`show job <id>`来查询job执行情况。

离线job如果是异步SELECT（并不INTO保存结果），也不会将结果打印在客户端（同步SELECT将会打印结果）。可以通过`show joblog <id>`来获得结果，结果中包含stdout和stderr两部分，stdout为查询结果，stderr为job运行日志。如果发现job failed或者其他状态，不符合你的预期，请仔细查看job运行日志。

```{note}
日志地址由taskmanager.properties的`job.log.path`配置，如果你改变了此配置项，需要到配置的目的地寻找日志。stdout日志默认在`/work/openmldb/taskmanager/bin/logs/job_x.log`，job运行日志默认在`/work/openmldb/taskmanager/bin/logs/job_x_error.log`(注意有error后缀)，

如果taskmanager是yarn模式，而不是local模式，`job_x_error.log`中的信息会较少，不会有job错误的详细信息。需要通过`job_x_error.log`中记录的yarn app id，去yarn系统中查询job的真正错误原因，需要查询到某application中主container的stderr日志。
```

#### 在线

集群版在线模式下，我们通常只推荐两种使用，`DEPLOY`创建deployment，执行deployment做实时特征计算（SDK请求deployment，或HTTP访问APIServer请求deployment）。在CLI或其他客户端中，可以直接在“在线”中进行SELECT查询，称为“在线预览”。在线预览有诸多限制，详情请参考[功能边界-集群版在线预览模式](./function_boundary.md#集群版在线预览模式)，请不要执行不支持的SQL。

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
请注意离线job默认为异步。如果你需要离线导入再查询，请设置为同步模式，详情见[离线命令配置详情](../openmldb_sql/ddl/SET_STATEMENT.md#离线命令配置详情)。否则导入还未完成就进行查询，是无意义的。
```

## 提供配置与日志，获得技术支持

如果你的SQL执行问题无法通过复现脚本复现，或者并非SQL执行问题而是集群管理问题，那么请提供客户端和服务端的配置与日志，以便我们调查。

docker或本地的集群（服务端所有进程都在本地），可以使用诊断工具快速获取配置、日志等信息。

使用`init.sh`/`start-all.sh`和`init.sh standalone`/`start-standalone.sh`脚本启动的OpenMLDB服务端，可以使用以下命令进行诊断，分别对应集群版和单机版。

```
openmldb_tool --env=onebox --dist_conf=cluster_dist.yml
openmldb_tool --env=onebox --dist_conf=standalone_dist.yml
```
`cluster_dist.yml`和`stadnalone_dist.yml`，可在docker容器`/work/`目录中找到，或将[github目录](https://github.com/4paradigm/OpenMLDB/tree/main/demo)中的yml文件复制下来使用。

如果是分布式的集群，需要配置ssh免密才能顺利使用诊断工具，参考文档[诊断工具](../maintain/diagnose.md)。

如果你的环境无法做到，请手动获取配置与日志。

## 性能

deployment耗时统计需要开启：
```
SET GLOBAL deploy_stats = 'on';
```
开启后的Deployment执行都将被统计，之前的不会被统计，表中的数据不包含集群外部的网络耗时，仅统计deployment在server端从开始执行到结束的时间。推荐直接查询统计表：
```
set @@execute_mode='online';
select * from INFORMATION_SCHEMA.DEPLOY_RESPONSE_TIME;
```
表的读法，可参考[mariadb response-time-distribution](https://mariadb.com/kb/en/query-response-time-plugin/#response-time-distribution)。


在监控项目[OpenMLDB Exporter](https://github.com/4paradigm/openmldb-exporter)中的deploy相关指标，都是从这个统计表中提取并再计算的结果。

https://github.com/4paradigm/openmldb-exporter/blob/493974b5c7b003d1f3d06c9db18b470cb4462c65/openmldb_exporter/collector/collectors.py#L75
