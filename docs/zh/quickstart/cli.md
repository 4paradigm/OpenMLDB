# CLI

CLI，指OpenMLDB的shell客户端，使用发布包`bin`中的binary程序`openmldb`启动，启动命令通常为`bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client`。
```{note}
如果是使用sbin部署的集群，可以使用sbin中的openmldb-cli.sh脚本启动CLI，脚本会自动填充参数。例如，在demo集群中，启动CLI的命令为`OPENMLDB_MODE=cluster /work/openmldb/sbin/openmldb-cli.sh`。
```

CLI默认执行模式为离线，其他SDK的默认可能不同。

本文将介绍常用的一些CLI可配置项与便利的非交互式使用方法，完整的可配置项见`bin/openmldb --help`。

## 基本使用

CLI每执行一个命令，都会打印"SUCCEED"/"Error"，表示该命令执行是否成功。

## 可配置项

CLI除了启动必须的`--zk_cluster`，`--zk_root_path`，`--role`三个参数，还可以补充一些可配置项，配置方式同样是`--<option_name>=<option_value>`的格式，例如我们想补充`spark_conf`配置项：
```
bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --spark_conf=/work/openmldb/bin/spark.conf
```

单个可配置项的说明可以通过help查询，例如，我们想知道`spark_conf`配置项的含义与默认值，可以`bin/openmldb --help | grep spark_conf`查询到。

下面具体介绍各个常用可配置项。

- spark_conf: 当离线任务的默认配置不满足需求时，例如Spark的driver/executor memory太小，可通过配置此选项改变离线任务的Spark参数。输入为配置文件的路径，格式详情见[客户端Spark配置文件](../reference/client_config/client_spark_config.md)。仅作用于当前启动的CLI，不会对其他的CLI或SDK客户端产生影响。
- glog_level & glog_dir: CLI默认只打印WARNING以上的日志，如果需要改变日志等级，可使用`glog_level`来调整，INFO, WARNING, ERROR, and FATAL日志分别对应 0, 1, 2, and 3。glog_dir默认为空，输出到stdout，也可以指定路径，保存为日志文件。
- sync_job_timeout: CLI执行离线同步任务的默认同步等待时间0.5h，如果离线同步任务需要等待更长的时间，可改变这一配置，但注意还需要改变集群中TaskManager的配置，详情见[离线命令配置详情](../openmldb_sql/ddl/SET_STATEMENT.md#离线命令配置详情)。
- zk_log_level & zk_log_file: CLI连接ZooKeeper产生的日志默认是不打印的，如果需要展示日志，可以调整`zk_log_level`。打印的日志默认是打印到stderr，且由于ZooKeeper连接是后台线程，可能出现CLI交互界面突然出现ZooKeeper相关的日志，不影响CLI的使用但影响界面展示，可以使用`zk_log_file`将ZooKeeper相关的日志输出到文件中。
- zk_session_timeout: 期望的ZooKeeper session超时时间，并不一定是真实的session超时时间。如果调整过大，也需要调整ZooKeeper Server的tickTime或maxSessionTimeout。

## 非交互式使用方法

启动CLI后的界面，我们称之为交互式界面。你需要输入SQL语句并回车，来执行操作。下面介绍一些非交互式的使用方法，方便进行批处理或调试。

### 非交互式删除

在执行删除操作，例如`drop table`, `drop deployment`时，CLI默认需要进行一次确认，以避免误操作。可使用`--interactive=false`来禁用交互式的删除确认。

### 单SQL

在一些场景中，我们通常只使用CLI执行一条SQL语句，例如，检查集群组件是否工作正常的`show components;`，我们可以使用非交互式的方式，避免交互式CLI的繁琐步骤。非交互式启动使用`--cmd`传递SQL，例如：
```
bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --cmd='show components;'
```

如果`cmd`需要在指定database下运行，可以使用`--database`(仅在cmd不为空时生效，其他时候配置无效)指定database名，例如：
```
bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client --database=demo_db --cmd='desc demo_table1'
```
```{note}
使用`--cmd`会自动设置`--interactive=false`，无需手动再设置。因此，`--cmd`可直接执行删除操作。
```

### SQL脚本

如果你希望CLI执行一个SQL脚本，可使用重定向的方式。例如，我们有这样一个SQL脚本用来创建database和表：
```
CREATE DATABASE demo_db;
USE demo_db;
CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date);
```
我们将它保存为`create.sql`文件，然后通过重定向执行这个脚本：
```
bin/openmldb --zk_cluster=127.0.0.1:2181 --zk_root_path=/openmldb --role=sql_client < create.sql
```
```{note}
请注意我们使用重定向时，无法通过CLI启动项来指定database，所以在SQL脚本中需要`use demo_db;`。

如果SQL脚本中有删除操作，需要`--interactive=false`来跳过删除确认。
```

