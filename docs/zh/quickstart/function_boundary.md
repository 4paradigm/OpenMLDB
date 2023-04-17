# 功能边界

本文将介绍 OpenMLDB SQL 的功能边界。

```{note}
如果对 SQL 语句有疑问，请参考 OpenMLDB SQL，或直接利用搜索功能搜索。
```

## 系统配置——TaskManager

通过配置 TaskManager 可以决定离线存储地址 `offline.data.prefix`、离线  job 计算所需 Spark 模式 `spark.master` 等。

`offline.data.prefix`：可配置为文件路径或 HDFS 路径。生产环境建议配置 HDFS 路径，测试环境（特指 onebox 型，例如在 Docker 容器内启动）可以配置本地文件路径。文件路径作为离线存储，将无法支持多 TaskManager 分布式部署（TaskManager 之间不会传输数据）。如果想在多台主机上部署 TaskManager，请使用 HDFS 等多机可同时访问到的存储介质。如果想测试多 TaskManager 工作协同，可以在一台主机上部署多个 TaskManager，此时可以使用文件路径作为离线存储。

`spark.master=local[*]`：Spark 默认配置为 `local[*]` 模式（自动绑定 CPU 核数，如果发现离线任务比较慢，建议使用 yarn 模式，改变配置后重启 TaskManager 生效。更多配置可参考 [master-urls](https://spark.apache.org/docs/3.1.2/submitting-applications.htmlmaster-urls)。

### spark.default.conf

更多可选配置，可以写在 `spark.default.conf` 参数中，格式为 `k1=v1;k2=v2`。例如：

```Plain
spark.default.conf=spark.port.maxRetries=32;foo=bar
```

`spark.port.maxRetries`：默认为 16，参考 [Spark 配置](https://spark.apache.org/docs/3.1.2/configuration.html)。每个离线 job 都会绑定 Spark UI，对应一个 port。每次 port 都是从默认初始端口开始尝试绑定，retry 即绑定下一个端口 port+1，如果同时运行的 job 大于 `spark.port.maxRetries`，retry 次数也就大于 `spark.port.maxRetries`，job 就会启动失败。如果你需要更大的 job 并发，请配置更大的 `spark.port.maxRetries`，重启 TaskManager 生效。

## DDL 边界——DEPLOY 语句

通过 `DEPLOY <deploy_name> <sql>` 可以部署上线 SQL 方案，这个操作也会自动解析 SQL 帮助创建索引（可以通过 `DESC <table_name>` 查看索引详情），详情可参考 [DEPLOY STATEMENT](../openmldb_sql/deployment_manage/DEPLOY_STATEMENT.md)。

部署操作是否成功，跟表的在线数据有一定关系。

### 长窗口 SQL

长窗口 SQL，即 `DEPLOY` 语句带有 `OPTIONS(long_windows=...)` 配置项，语法详情见[长窗口](../openmldb_sql/deployment_manage/DEPLOY_STATEMENT#长窗口优化)。长窗口 SQL 的部署条件比较严格，必须保证 SQL 中使用的表没有在线数据。否则，即使部署和之前一致的 SQL，也会操作失败。

### 普通 SQL

- 如果部署之前已存在相关的索引，那么这一次部署操作不会创建索引。无论表中有无在线数据，`DEPLOY` 操作将成功。
- 如果部署时需要创建新的索引，而此时表中已有在线数据，那么 `DEPLOY` 操作将失败。

解决方案有两种：

- 严格保持先 `DEPLOY` 再导入在线数据，不要在表中有在线数据后做 `DEPLOY`。
- `CRATE INDEX` 语句可以在创建新索引时，自动导入已存在的在线数据（已有索引里的数据）。如果一定需要在表已有在线数据的情况下 `DEPLOY`，可以先手动 `CRATE INDEX` 创建需要的索引（新索引就有数据了），再 `DEPLOY`（这时的 `DEPLOY` 不会创建新索引，计算时直接使用手动创建的那些索引）。

```{note}
如何知道应该创建哪些索引？ 

目前只有 Java SDK 支持，可以通过 `SqlClusterExecutor.genDDL` 获取需要创建的所有索引。（但 `genDDL` 是获得建表语句，所以需要手动转换为 `CREATE INDEX`。） 未来将支持**直接获取创建索引语句**，或支持 `DEPLOY` **自动导入数据到新索引**。
```

## DML 边界

### LOAD DATA

`LOAD DATA` 无论导入到在线或离线，都是离线 job。源数据的格式规则，离线在线没有区别。

推荐使用 HDFS 文件作为源数据，无论 TaskManager 是 local/yarn 模式，还是 TaskManager 在别的主机上运行，都可以导入。如果源数据为本地文件，是否可以顺利导入需要考虑 TaskManager 模式和运行主机。

- TaskManager 是 local 模式，只有将源数据放在 TaskManager 进程的主机上才能顺利导入。
- TaskManager 是 yarn (client and cluster) 模式时，由于不知道运行容器是哪台主机，不可使用文件路径作为源数据地址。

### DELETE

在线存储的表有多索引，`DELETE` 可能无法删除所有索引中的对应数据，所以，可能出现删除了数据，却能查出已删除数据的情况。

举例说明：

```SQL
create database db;
use db;
create table t1(c1 int, c2 int,index(key=c1),index(key=c2));
desc t1;
set @@execute_mode='online';
insert into t1 values (1,1),(2,2);
delete from t1 where c2=2;
select * from t1;
select * from t1 where c2=2;
```

结果如下：

```Plain
 --- ------- ------ ------ ---------
     Field   Type   Null   Default
 --- ------- ------ ------ ---------
  1   c1      Int    YES
  2   c2      Int    YES
 --- ------- ------ ------ ---------
 --- -------------------- ------ ---- ------ ---------------
     name                 keys   ts   ttl    ttl_type
 --- -------------------- ------ ---- ------ ---------------
  1   INDEX_0_1668504212   c1     -    0min   kAbsoluteTime
  2   INDEX_1_1668504212   c2     -    0min   kAbsoluteTime
 --- -------------------- ------ ---- ------ ---------------
 --------------
  storage_mode
 --------------
  Memory
 --------------
 ---- ----
  c1   c2
 ---- ----
  1    1
  2    2
 ---- ----

2 rows in set
 ---- ----
  c1   c2
 ---- ----

0 rows in set
```

说明：

表 `t1` 有多个索引（`DEPLOY` 也可能自动创建出多索引），`delete from t1 where c2=2` 实际只删除了第二个 index 的数据，第一个 index 数据没有被影响。所以 `select * from t1` 使用第一个索引，结果会有两条数据，并没有删除，`select * from t1 where c2=2` 使用第二个索引，结果为空，数据已被删除。

## DQL 边界

根据执行模式的不同，支持的查询模式（即 `SELECT` 语句）也有所不同：

| **执行模式** | **查询语句**                                                 |
| ------------ | ------------------------------------------------------------ |
| 离线模式     | 批查询                                                       |
| 在线模式     | 批查询（又称在线预览模式，仅支持部分 SQL）和请求查询（又称在线请求模式） |

### 在线预览模式

OpenMLDB CLI 中在线模式下执行 SQL，均为在线预览模式。在线预览模式支持有限，详情可参考 [SELECT STATEMENT](../openmldb_sql/dql/SELECT_STATEMENT)。

在线预览模式主要目的为预览查询结果。如果希望能运行复杂 SQL，请使用离线模式。如果希望查询完整的在线数据，建议使用数据导出工具查看（比如 `SELECT INTO` 命令 ）。如果在线表数据量过大，还可能触发数据截断，`SELECT * FROM table` 命令很可能会导致部分结果不被返回。

在线数据通常是分布式存储的，`SELECT * FROM table` 从各个 Tablet Server 中获取结果，但并不会做全局排序，且 Server 顺序有一定的随机性。所以每次执行 `SELECT * FROM table` 的结果不能保证数据顺序一致。

### 离线模式与在线请求模式

在[特征工程开发上线全流程](../tutorial/modes.md11-特征工程开发上线全流程)中，主要使用离线模式和在线请求模式。

- 离线模式的批查询：离线特征生成
- 在线请求模式的请求查询：实时特征计算

两种模式虽然不同，但使用的是相同的 SQL 语句，且计算结果一致。但由于离线和在线使用两套执行引擎，功能尚未完全对齐，因此离线可执行的 SQL 不一定可以部署上线（在线请求模式可执行的 SQL 是离线可执行 SQL 的子集）。在实际开发中，需要在完成离线 SQL 开发后 `DEPLOY`，来测试 SQL 是否可上线。

